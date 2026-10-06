var __defProp = Object.defineProperty;
var __name = (target, value) => __defProp(target, "name", { value, configurable: true });

// src/chat-session.ts
import { DurableObject } from "cloudflare:workers";
var MODEL = "@cf/meta/llama-3.3-70b-instruct-fp8-fast";
var MAX_MESSAGE_CHARS = 4e3;
var ASSISTANT_NAME = "Atlas";
var HISTORY_LIMIT = 200;
var CONTEXT_MAX_MESSAGES = 30;
var CONTEXT_CHAR_BUDGET = 16e3;
var MAX_REPLY_TOKENS = 1024;
var MEMORY_TRIGGER_THRESHOLD = 6;
var MEMORY_JOB_TIMEOUT_MS = 5 * 60 * 1e3;
var SNAPSHOT_MAX_MESSAGES = 60;
var MAX_FACTS = 25;
var MAX_FACT_CHARS = 200;
var MAX_SUMMARY_CHARS = 2e3;
var ChatSession = class extends DurableObject {
  static {
    __name(this, "ChatSession");
  }
  sql;
  constructor(ctx, env) {
    super(ctx, env);
    this.sql = ctx.storage.sql;
    this.sql.exec(
      `CREATE TABLE IF NOT EXISTS messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        role TEXT NOT NULL,
        content TEXT NOT NULL,
        created_at INTEGER NOT NULL
      )`
    );
    this.sql.exec(
      `CREATE TABLE IF NOT EXISTS facts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        fact TEXT NOT NULL
      )`
    );
    this.sql.exec(`CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)`);
  }
  // ---------------------------------------------------------------------------
  // HTTP (used for the streaming chat endpoint)
  // ---------------------------------------------------------------------------
  async fetch(request) {
    const url = new URL(request.url);
    if (request.method === "POST" && url.pathname === "/chat") {
      const body = await request.json();
      if (typeof body.message !== "string" || !body.message.trim()) {
        return Response.json({ error: "Message is required" }, { status: 400 });
      }
      return this.chat(body.message.trim().slice(0, MAX_MESSAGE_CHARS));
    }
    return Response.json({ error: "Not found" }, { status: 404 });
  }
  // ---------------------------------------------------------------------------
  // RPC methods (called by the Worker and the memory Workflow)
  // ---------------------------------------------------------------------------
  async getState() {
    return { messages: this.listMessages(), memory: await this.getMemory() };
  }
  async getMemory() {
    let job = null;
    const active = this.activeJob();
    if (active) {
      const status = await this.workflowStatus(active.instanceId);
      if (status === "errored" || status === "terminated") {
        this.clearJob(active.instanceId);
      } else if (this.activeJob()?.instanceId === active.instanceId) {
        job = { ...active, status };
      }
    }
    const updatedAt = this.getMeta("memory_updated_at");
    return {
      summary: this.getMeta("summary") ?? "",
      facts: this.listFacts(),
      pendingMessages: this.pendingMessageCount(),
      updatedAt: updatedAt ? Number(updatedAt) : null,
      job
    };
  }
  async refreshMemory() {
    const result = await this.startMemoryJob(true);
    return { ...result, memory: await this.getMemory() };
  }
  async reset() {
    this.sql.exec("DELETE FROM messages");
    this.sql.exec("DELETE FROM facts");
    this.sql.exec("DELETE FROM meta");
  }
  getMemorySnapshot() {
    const throughBefore = this.summarizedThroughId();
    const rows = this.sql.exec(
      "SELECT id, role, content, created_at FROM messages WHERE id > ? ORDER BY id ASC LIMIT ?",
      throughBefore,
      SNAPSHOT_MAX_MESSAGES
    ).toArray();
    return {
      summary: this.getMeta("summary") ?? "",
      facts: this.listFacts(),
      messages: rows.map((row) => ({ role: row.role, content: row.content })),
      throughId: rows.length ? rows[rows.length - 1].id : throughBefore
    };
  }
  applyMemory(update) {
    if (this.getMeta("job_instance_id") !== update.instanceId) {
      return { applied: false };
    }
    const summary = update.summary.trim().slice(0, MAX_SUMMARY_CHARS);
    const facts = sanitizeFacts(update.facts);
    this.ctx.storage.transactionSync(() => {
      this.setMeta("summary", summary);
      this.sql.exec("DELETE FROM facts");
      for (const fact of facts) {
        this.sql.exec("INSERT INTO facts (fact) VALUES (?)", fact);
      }
      this.setMeta("summarized_through_id", String(Math.max(this.summarizedThroughId(), update.throughId)));
      this.setMeta("memory_updated_at", String(Date.now()));
      this.clearJob(update.instanceId);
    });
    return { applied: true };
  }
  releaseMemoryJob(instanceId) {
    this.clearJob(instanceId);
  }
  // ---------------------------------------------------------------------------
  // Chat
  // ---------------------------------------------------------------------------
  async chat(message) {
    const userMessage = this.insertMessage("user", message);
    const prompt = this.buildPrompt();
    let aiStream;
    try {
      aiStream = await this.env.AI.run(MODEL, {
        messages: prompt,
        stream: true,
        max_tokens: MAX_REPLY_TOKENS
      });
    } catch (err) {
      console.error("Workers AI request failed", err);
      this.sql.exec("DELETE FROM messages WHERE id = ?", userMessage.id);
      return Response.json({ error: "The AI model is unavailable right now. Please try again." }, { status: 502 });
    }
    const { readable, writable } = new TransformStream();
    const writer = writable.getWriter();
    const encoder = new TextEncoder();
    let clientConnected = true;
    const send = /* @__PURE__ */ __name(async (event, data) => {
      if (!clientConnected) return;
      try {
        await writer.write(encoder.encode(`event: ${event}
data: ${JSON.stringify(data)}

`));
      } catch {
        clientConnected = false;
      }
    }, "send");
    const pump = /* @__PURE__ */ __name(async () => {
      let reply = "";
      try {
        await send("start", { userMessage });
        for await (const token of readAiTokens(aiStream)) {
          reply += token;
          await send("token", { token });
        }
        const assistantMessage = this.insertMessage(
          "assistant",
          reply.trim() || "Sorry, I couldn't come up with a response. Could you rephrase that?"
        );
        try {
          await this.startMemoryJob(false);
        } catch (err) {
          console.error("Failed to start memory workflow", err);
        }
        await send("done", { assistantMessage, memory: await this.getMemory() });
      } catch (err) {
        console.error("Streaming reply failed", err);
        if (reply.trim()) {
          this.insertMessage("assistant", reply.trim());
        }
        await send("error", { message: "The response was interrupted. Please try again." });
      } finally {
        if (clientConnected) {
          await writer.close().catch(() => void 0);
        }
      }
    }, "pump");
    this.ctx.waitUntil(pump());
    return new Response(readable, {
      headers: {
        "content-type": "text/event-stream; charset=utf-8",
        "cache-control": "no-cache, no-store",
        "x-content-type-options": "nosniff"
      }
    });
  }
  buildPrompt() {
    const rows = this.sql.exec(
      "SELECT id, role, content, created_at FROM messages ORDER BY id DESC LIMIT ?",
      CONTEXT_MAX_MESSAGES
    ).toArray();
    const history = [];
    let budget = CONTEXT_CHAR_BUDGET;
    for (const row of rows) {
      if (history.length > 0 && budget - row.content.length < 0) break;
      budget -= row.content.length;
      history.unshift({ role: row.role, content: row.content });
    }
    while (history.length > 0 && history[0].role !== "user") {
      history.shift();
    }
    return [{ role: "system", content: this.systemPrompt() }, ...history];
  }
  systemPrompt() {
    const parts = [
      `You are ${ASSISTANT_NAME}, a friendly, knowledgeable AI assistant running on Cloudflare Workers AI.`,
      "Answer clearly and concisely. Use short paragraphs or lists when helpful.",
      "Use the long-term memory below to personalise your answers, but never invent facts about the user.",
      "If you are unsure about something, say so.",
      `Today's date is ${(/* @__PURE__ */ new Date()).toISOString().slice(0, 10)}.`
    ];
    const summary = this.getMeta("summary");
    if (summary) {
      parts.push(`
Summary of the earlier conversation:
${summary}`);
    }
    const facts = this.listFacts();
    if (facts.length > 0) {
      parts.push(`
Known facts about the user:
${facts.map((fact) => `- ${fact}`).join("\n")}`);
    }
    return parts.join("\n");
  }
  // ---------------------------------------------------------------------------
  // Memory workflow coordination
  // ---------------------------------------------------------------------------
  async startMemoryJob(force) {
    if (this.activeJob()) {
      return { started: false, reason: "A memory update is already in progress." };
    }
    const pending = this.pendingMessageCount();
    if (pending === 0) {
      return { started: false, reason: "Memory is already up to date." };
    }
    if (!force && pending < MEMORY_TRIGGER_THRESHOLD) {
      return { started: false, reason: "Not enough new messages yet." };
    }
    const instanceId = crypto.randomUUID();
    this.setMeta("job_instance_id", instanceId);
    this.setMeta("job_started_at", String(Date.now()));
    try {
      await this.env.MEMORY_WORKFLOW.create({
        id: instanceId,
        params: { sessionObjectId: this.ctx.id.toString() }
      });
    } catch (err) {
      this.clearJob(instanceId);
      throw err;
    }
    return { started: true, reason: "Memory update started." };
  }
  activeJob() {
    const instanceId = this.getMeta("job_instance_id");
    const startedAt = Number(this.getMeta("job_started_at") ?? 0);
    if (!instanceId) return null;
    if (Date.now() - startedAt > MEMORY_JOB_TIMEOUT_MS) {
      this.clearJob(instanceId);
      return null;
    }
    return { instanceId, startedAt };
  }
  clearJob(instanceId) {
    if (this.getMeta("job_instance_id") === instanceId) {
      this.sql.exec("DELETE FROM meta WHERE key IN ('job_instance_id', 'job_started_at')");
    }
  }
  async workflowStatus(instanceId) {
    try {
      const instance = await this.env.MEMORY_WORKFLOW.get(instanceId);
      return (await instance.status()).status;
    } catch {
      return "queued";
    }
  }
  // ---------------------------------------------------------------------------
  // Storage helpers
  // ---------------------------------------------------------------------------
  insertMessage(role, content) {
    const createdAt = Date.now();
    const { id } = this.sql.exec(
      "INSERT INTO messages (role, content, created_at) VALUES (?, ?, ?) RETURNING id",
      role,
      content,
      createdAt
    ).one();
    return { id, role, content, createdAt };
  }
  listMessages() {
    return this.sql.exec(
      "SELECT id, role, content, created_at FROM (SELECT * FROM messages ORDER BY id DESC LIMIT ?) ORDER BY id ASC",
      HISTORY_LIMIT
    ).toArray().map((row) => ({ id: row.id, role: row.role, content: row.content, createdAt: row.created_at }));
  }
  listFacts() {
    return this.sql.exec("SELECT fact FROM facts ORDER BY id ASC").toArray().map((row) => row.fact);
  }
  pendingMessageCount() {
    return this.sql.exec("SELECT COUNT(*) AS n FROM messages WHERE id > ?", this.summarizedThroughId()).one().n;
  }
  summarizedThroughId() {
    return Number(this.getMeta("summarized_through_id") ?? 0);
  }
  getMeta(key) {
    const rows = this.sql.exec("SELECT value FROM meta WHERE key = ?", key).toArray();
    return rows.length ? rows[0].value : null;
  }
  setMeta(key, value) {
    this.sql.exec(
      "INSERT INTO meta (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
      key,
      value
    );
  }
};
function sanitizeFacts(facts) {
  if (!Array.isArray(facts)) return [];
  const seen = /* @__PURE__ */ new Set();
  const result = [];
  for (const raw of facts) {
    if (typeof raw !== "string") continue;
    const fact = raw.replace(/\s+/g, " ").trim().slice(0, MAX_FACT_CHARS);
    const key = fact.toLowerCase();
    if (!fact || seen.has(key)) continue;
    seen.add(key);
    result.push(fact);
    if (result.length >= MAX_FACTS) break;
  }
  return result;
}
__name(sanitizeFacts, "sanitizeFacts");
async function* readAiTokens(stream) {
  const reader = stream.pipeThrough(new TextDecoderStream()).getReader();
  let buffer = "";
  try {
    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      buffer += value;
      let newline;
      while ((newline = buffer.indexOf("\n")) !== -1) {
        const line = buffer.slice(0, newline).trim();
        buffer = buffer.slice(newline + 1);
        if (!line.startsWith("data:")) continue;
        const payload = line.slice(5).trim();
        if (payload === "[DONE]") return;
        try {
          const parsed = JSON.parse(payload);
          if (typeof parsed.response === "string" && parsed.response) {
            yield parsed.response;
          }
        } catch {
        }
      }
    }
  } finally {
    reader.releaseLock();
  }
}
__name(readAiTokens, "readAiTokens");

// src/memory-workflow.ts
import {
  WorkflowEntrypoint
} from "cloudflare:workers";
var AI_STEP_CONFIG = {
  retries: { limit: 3, delay: "5 seconds", backoff: "exponential" },
  timeout: "2 minutes"
};
var MAX_TRANSCRIPT_MESSAGE_CHARS = 2e3;
var SUMMARY_SYSTEM_PROMPT = `You maintain a running summary of a conversation between a user and an AI assistant named Atlas.
Merge the existing summary with the new transcript into a single updated summary of at most 150 words.
Focus on the topics discussed, decisions made, open questions and the user's goals.
Write in the third person ("The user asked..."). Output only the summary text, with no preamble.`;
var FACTS_SYSTEM_PROMPT = `You maintain a list of durable facts about the USER of a chat assistant: their name, preferences, goals, background, projects and constraints.
Given the existing facts and a new transcript, return the complete updated list:
- keep facts that are still valid,
- add new facts the user stated about themselves,
- update or remove facts contradicted by the new transcript.
Do not include facts about the assistant, general knowledge, or temporary details of a single question.
Each fact must be a short, standalone sentence. Return at most ${MAX_FACTS} facts.
Respond with JSON of the form {"facts": ["..."]}.`;
var FACTS_SCHEMA = {
  type: "object",
  properties: {
    facts: { type: "array", items: { type: "string" } }
  },
  required: ["facts"]
};
var MemoryWorkflow = class extends WorkflowEntrypoint {
  static {
    __name(this, "MemoryWorkflow");
  }
  async run(event, step) {
    const { instanceId } = event;
    const namespace = this.env.CHAT_SESSION;
    const session = namespace.get(namespace.idFromString(event.payload.sessionObjectId));
    try {
      const snapshot = await step.do("load conversation", async () => {
        const data = await session.getMemorySnapshot();
        return {
          summary: data.summary,
          facts: [...data.facts],
          messages: data.messages.map((m) => ({ role: m.role, content: m.content })),
          throughId: data.throughId
        };
      });
      if (snapshot.messages.length === 0) {
        await step.do("release lock", async () => {
          await session.releaseMemoryJob(instanceId);
          return true;
        });
        return { status: "skipped" };
      }
      const transcript = formatTranscript(snapshot.messages);
      const summary = await step.do(
        "summarize conversation",
        AI_STEP_CONFIG,
        async () => this.summarize(snapshot.summary, transcript)
      );
      const facts = await step.do(
        "extract user facts",
        AI_STEP_CONFIG,
        async () => this.extractFacts(snapshot.facts, transcript)
      );
      const result = await step.do("save memory", async () => {
        const { applied } = await session.applyMemory({
          instanceId,
          summary,
          facts,
          throughId: snapshot.throughId
        });
        return { applied };
      });
      return { status: result.applied ? "applied" : "discarded", factCount: facts.length };
    } catch (err) {
      await step.do("release lock after failure", async () => {
        await session.releaseMemoryJob(instanceId);
        return true;
      });
      throw err;
    }
  }
  async summarize(previousSummary, transcript) {
    const result = await this.env.AI.run(MODEL, {
      messages: [
        { role: "system", content: SUMMARY_SYSTEM_PROMPT },
        {
          role: "user",
          content: `Existing summary:
${previousSummary || "(none)"}

New transcript:
${transcript}`
        }
      ],
      max_tokens: 400,
      temperature: 0.2
    });
    const summary = responseText(result).trim();
    if (!summary) {
      throw new Error("Model returned an empty summary");
    }
    return summary;
  }
  async extractFacts(existingFacts, transcript) {
    const existing = existingFacts.length ? existingFacts.map((f) => `- ${f}`).join("\n") : "(none)";
    const result = await this.env.AI.run(MODEL, {
      messages: [
        { role: "system", content: FACTS_SYSTEM_PROMPT },
        { role: "user", content: `Existing facts:
${existing}

New transcript:
${transcript}` }
      ],
      response_format: { type: "json_schema", json_schema: FACTS_SCHEMA },
      max_tokens: 600,
      temperature: 0.1
    });
    const response = result.response;
    const parsed = typeof response === "string" ? JSON.parse(extractJson(response)) : response;
    const facts = parsed?.facts;
    if (!Array.isArray(facts)) {
      throw new Error("Model response did not contain a facts array");
    }
    return facts.filter((f) => typeof f === "string");
  }
};
function formatTranscript(messages) {
  return messages.map((m) => {
    const speaker = m.role === "user" ? "User" : "Assistant";
    const content = m.content.length > MAX_TRANSCRIPT_MESSAGE_CHARS ? `${m.content.slice(0, MAX_TRANSCRIPT_MESSAGE_CHARS)}\u2026` : m.content;
    return `${speaker}: ${content}`;
  }).join("\n\n");
}
__name(formatTranscript, "formatTranscript");
function responseText(result) {
  const response = result?.response;
  return typeof response === "string" ? response : "";
}
__name(responseText, "responseText");
function extractJson(text) {
  const start = text.indexOf("{");
  const end = text.lastIndexOf("}");
  return start !== -1 && end > start ? text.slice(start, end + 1) : text;
}
__name(extractJson, "extractJson");

// src/index.ts
var SESSION_ID_PATTERN = /^[A-Za-z0-9-]{8,64}$/;
var SESSION_ROUTE = /^\/api\/sessions\/([^/]+)(\/[a-z/]*)?$/;
function json(data, status = 200) {
  return Response.json(data, { status, headers: { "cache-control": "no-store" } });
}
__name(json, "json");
function errorResponse(message, status) {
  return json({ error: message }, status);
}
__name(errorResponse, "errorResponse");
async function handleChat(request, session) {
  if (!request.headers.get("content-type")?.toLowerCase().includes("application/json")) {
    return errorResponse("Expected an application/json body", 415);
  }
  let body;
  try {
    body = await request.json();
  } catch {
    return errorResponse("Invalid JSON body", 400);
  }
  const raw = body?.message;
  const message = typeof raw === "string" ? raw.trim() : "";
  if (!message) {
    return errorResponse("Message is required", 400);
  }
  if (message.length > MAX_MESSAGE_CHARS) {
    return errorResponse(`Message must be at most ${MAX_MESSAGE_CHARS} characters`, 413);
  }
  return session.fetch("https://chat-session/chat", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ message })
  });
}
__name(handleChat, "handleChat");
var index_default = {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (!url.pathname.startsWith("/api/")) {
      return env.ASSETS.fetch(request);
    }
    if (url.pathname === "/api/health") {
      return json({ ok: true, model: MODEL });
    }
    const match = SESSION_ROUTE.exec(url.pathname);
    if (!match) {
      return errorResponse("Not found", 404);
    }
    const sessionId = match[1];
    const action = match[2] ?? "";
    if (!SESSION_ID_PATTERN.test(sessionId)) {
      return errorResponse("Invalid session id", 400);
    }
    const session = env.CHAT_SESSION.get(env.CHAT_SESSION.idFromName(sessionId));
    try {
      switch (`${request.method} ${action}`) {
        case "GET ":
          return json(await session.getState());
        case "DELETE ":
          await session.reset();
          return json({ ok: true });
        case "POST /chat":
          return await handleChat(request, session);
        case "GET /memory":
          return json(await session.getMemory());
        case "POST /memory/refresh":
          return json(await session.refreshMemory());
        default:
          return errorResponse("Not found", 404);
      }
    } catch (err) {
      console.error("Request failed", err);
      return errorResponse("Internal server error", 500);
    }
  }
};
export {
  ChatSession,
  MemoryWorkflow,
  index_default as default
};
//# sourceMappingURL=index.js.map
