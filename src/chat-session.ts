import { DurableObject } from "cloudflare:workers";

export const MODEL = "@cf/meta/llama-3.3-70b-instruct-fp8-fast";
export const MAX_MESSAGE_CHARS = 4000;

const ASSISTANT_NAME = "Atlas";
const HISTORY_LIMIT = 200;
const CONTEXT_MAX_MESSAGES = 30;
const CONTEXT_CHAR_BUDGET = 16000;
const MAX_REPLY_TOKENS = 1024;
const MEMORY_TRIGGER_THRESHOLD = 6;
const MEMORY_JOB_TIMEOUT_MS = 5 * 60 * 1000;
const SNAPSHOT_MAX_MESSAGES = 60;
export const MAX_FACTS = 25;
const MAX_FACT_CHARS = 200;
const MAX_SUMMARY_CHARS = 2000;

export type Role = "user" | "assistant";

export interface ChatMessage {
  id: number;
  role: Role;
  content: string;
  createdAt: number;
}

export interface MemoryJob {
  instanceId: string;
  startedAt: number;
  status: string;
}

export interface MemoryView {
  summary: string;
  facts: string[];
  pendingMessages: number;
  updatedAt: number | null;
  job: MemoryJob | null;
}

export interface SessionState {
  messages: ChatMessage[];
  memory: MemoryView;
}

export interface MemorySnapshot {
  summary: string;
  facts: string[];
  messages: { role: Role; content: string }[];
  throughId: number;
}

export interface MemoryUpdate {
  instanceId: string;
  summary: string;
  facts: string[];
  throughId: number;
}

export interface MemoryRefreshResult {
  started: boolean;
  reason: string;
  memory: MemoryView;
}

export interface MemoryWorkflowParams {
  sessionObjectId: string;
}

type PromptMessage = { role: "system" | Role; content: string };
type MessageRow = { id: number; role: string; content: string; created_at: number };

/**
 * One Durable Object per chat session. Owns the transcript and long-term memory in
 * SQLite storage, streams Llama 3.3 replies, and coordinates memory Workflow runs.
 */
export class ChatSession extends DurableObject<Env> {
  private readonly sql: SqlStorage;

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    this.sql = ctx.storage.sql;
    this.sql.exec(
      `CREATE TABLE IF NOT EXISTS messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        role TEXT NOT NULL,
        content TEXT NOT NULL,
        created_at INTEGER NOT NULL
      )`,
    );
    this.sql.exec(
      `CREATE TABLE IF NOT EXISTS facts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        fact TEXT NOT NULL
      )`,
    );
    this.sql.exec(`CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)`);
  }

  // ---------------------------------------------------------------------------
  // HTTP (used for the streaming chat endpoint)
  // ---------------------------------------------------------------------------

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    if (request.method === "POST" && url.pathname === "/chat") {
      const body = await request.json<{ message?: unknown }>();
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

  async getState(): Promise<SessionState> {
    return { messages: this.listMessages(), memory: await this.getMemory() };
  }

  async getMemory(): Promise<MemoryView> {
    let job: MemoryJob | null = null;
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
      job,
    };
  }

  async refreshMemory(): Promise<MemoryRefreshResult> {
    const result = await this.startMemoryJob(true);
    return { ...result, memory: await this.getMemory() };
  }

  async reset(): Promise<void> {
    this.sql.exec("DELETE FROM messages");
    this.sql.exec("DELETE FROM facts");
    this.sql.exec("DELETE FROM meta");
  }

  getMemorySnapshot(): MemorySnapshot {
    const throughBefore = this.summarizedThroughId();
    const rows = this.sql
      .exec<MessageRow>(
        "SELECT id, role, content, created_at FROM messages WHERE id > ? ORDER BY id ASC LIMIT ?",
        throughBefore,
        SNAPSHOT_MAX_MESSAGES,
      )
      .toArray();
    return {
      summary: this.getMeta("summary") ?? "",
      facts: this.listFacts(),
      messages: rows.map((row) => ({ role: row.role as Role, content: row.content })),
      throughId: rows.length ? rows[rows.length - 1].id : throughBefore,
    };
  }

  applyMemory(update: MemoryUpdate): { applied: boolean } {
    // Ignore results from stale runs (e.g. the session was reset mid-run).
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

  releaseMemoryJob(instanceId: string): void {
    this.clearJob(instanceId);
  }

  // ---------------------------------------------------------------------------
  // Chat
  // ---------------------------------------------------------------------------

  private async chat(message: string): Promise<Response> {
    const userMessage = this.insertMessage("user", message);
    const prompt = this.buildPrompt();

    let aiStream: ReadableStream<Uint8Array>;
    try {
      aiStream = (await this.env.AI.run(MODEL, {
        messages: prompt,
        stream: true,
        max_tokens: MAX_REPLY_TOKENS,
      })) as ReadableStream<Uint8Array>;
    } catch (err) {
      console.error("Workers AI request failed", err);
      this.sql.exec("DELETE FROM messages WHERE id = ?", userMessage.id);
      return Response.json({ error: "The AI model is unavailable right now. Please try again." }, { status: 502 });
    }

    const { readable, writable } = new TransformStream<Uint8Array, Uint8Array>();
    const writer = writable.getWriter();
    const encoder = new TextEncoder();
    let clientConnected = true;

    const send = async (event: string, data: unknown): Promise<void> => {
      if (!clientConnected) return;
      try {
        await writer.write(encoder.encode(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`));
      } catch {
        // Keep consuming the model stream so the reply is still persisted.
        clientConnected = false;
      }
    };

    const pump = async (): Promise<void> => {
      let reply = "";
      try {
        await send("start", { userMessage });
        for await (const token of readAiTokens(aiStream)) {
          reply += token;
          await send("token", { token });
        }
        const assistantMessage = this.insertMessage(
          "assistant",
          reply.trim() || "Sorry, I couldn't come up with a response. Could you rephrase that?",
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
          await writer.close().catch(() => undefined);
        }
      }
    };

    this.ctx.waitUntil(pump());

    return new Response(readable, {
      headers: {
        "content-type": "text/event-stream; charset=utf-8",
        "cache-control": "no-cache, no-store",
        "x-content-type-options": "nosniff",
      },
    });
  }

  private buildPrompt(): PromptMessage[] {
    const rows = this.sql
      .exec<MessageRow>(
        "SELECT id, role, content, created_at FROM messages ORDER BY id DESC LIMIT ?",
        CONTEXT_MAX_MESSAGES,
      )
      .toArray();

    const history: PromptMessage[] = [];
    let budget = CONTEXT_CHAR_BUDGET;
    for (const row of rows) {
      if (history.length > 0 && budget - row.content.length < 0) break;
      budget -= row.content.length;
      history.unshift({ role: row.role as Role, content: row.content });
    }
    // Llama chat templates expect the first non-system turn to come from the user.
    while (history.length > 0 && history[0].role !== "user") {
      history.shift();
    }

    return [{ role: "system", content: this.systemPrompt() }, ...history];
  }

  private systemPrompt(): string {
    const parts = [
      `You are ${ASSISTANT_NAME}, a friendly, knowledgeable AI assistant running on Cloudflare Workers AI.`,
      "Answer clearly and concisely. Use short paragraphs or lists when helpful.",
      "Use the long-term memory below to personalise your answers, but never invent facts about the user.",
      "If you are unsure about something, say so.",
      `Today's date is ${new Date().toISOString().slice(0, 10)}.`,
    ];
    const summary = this.getMeta("summary");
    if (summary) {
      parts.push(`\nSummary of the earlier conversation:\n${summary}`);
    }
    const facts = this.listFacts();
    if (facts.length > 0) {
      parts.push(`\nKnown facts about the user:\n${facts.map((fact) => `- ${fact}`).join("\n")}`);
    }
    return parts.join("\n");
  }

  // ---------------------------------------------------------------------------
  // Memory workflow coordination
  // ---------------------------------------------------------------------------

  private async startMemoryJob(force: boolean): Promise<{ started: boolean; reason: string }> {
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

    // Claim the lock before awaiting so concurrent requests cannot start duplicate runs.
    const instanceId = crypto.randomUUID();
    this.setMeta("job_instance_id", instanceId);
    this.setMeta("job_started_at", String(Date.now()));
    try {
      await this.env.MEMORY_WORKFLOW.create({
        id: instanceId,
        params: { sessionObjectId: this.ctx.id.toString() },
      });
    } catch (err) {
      this.clearJob(instanceId);
      throw err;
    }
    return { started: true, reason: "Memory update started." };
  }

  private activeJob(): Omit<MemoryJob, "status"> | null {
    const instanceId = this.getMeta("job_instance_id");
    const startedAt = Number(this.getMeta("job_started_at") ?? 0);
    if (!instanceId) return null;
    if (Date.now() - startedAt > MEMORY_JOB_TIMEOUT_MS) {
      this.clearJob(instanceId);
      return null;
    }
    return { instanceId, startedAt };
  }

  private clearJob(instanceId: string): void {
    if (this.getMeta("job_instance_id") === instanceId) {
      this.sql.exec("DELETE FROM meta WHERE key IN ('job_instance_id', 'job_started_at')");
    }
  }

  private async workflowStatus(instanceId: string): Promise<string> {
    try {
      const instance = await this.env.MEMORY_WORKFLOW.get(instanceId);
      return (await instance.status()).status;
    } catch {
      // The instance may not be visible yet right after creation.
      return "queued";
    }
  }

  // ---------------------------------------------------------------------------
  // Storage helpers
  // ---------------------------------------------------------------------------

  private insertMessage(role: Role, content: string): ChatMessage {
    const createdAt = Date.now();
    const { id } = this.sql
      .exec<{ id: number }>(
        "INSERT INTO messages (role, content, created_at) VALUES (?, ?, ?) RETURNING id",
        role,
        content,
        createdAt,
      )
      .one();
    return { id, role, content, createdAt };
  }

  private listMessages(): ChatMessage[] {
    return this.sql
      .exec<MessageRow>(
        "SELECT id, role, content, created_at FROM (SELECT * FROM messages ORDER BY id DESC LIMIT ?) ORDER BY id ASC",
        HISTORY_LIMIT,
      )
      .toArray()
      .map((row) => ({ id: row.id, role: row.role as Role, content: row.content, createdAt: row.created_at }));
  }

  private listFacts(): string[] {
    return this.sql
      .exec<{ fact: string }>("SELECT fact FROM facts ORDER BY id ASC")
      .toArray()
      .map((row) => row.fact);
  }

  private pendingMessageCount(): number {
    return this.sql
      .exec<{ n: number }>("SELECT COUNT(*) AS n FROM messages WHERE id > ?", this.summarizedThroughId())
      .one().n;
  }

  private summarizedThroughId(): number {
    return Number(this.getMeta("summarized_through_id") ?? 0);
  }

  private getMeta(key: string): string | null {
    const rows = this.sql.exec<{ value: string }>("SELECT value FROM meta WHERE key = ?", key).toArray();
    return rows.length ? rows[0].value : null;
  }

  private setMeta(key: string, value: string): void {
    this.sql.exec(
      "INSERT INTO meta (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
      key,
      value,
    );
  }
}

function sanitizeFacts(facts: unknown): string[] {
  if (!Array.isArray(facts)) return [];
  const seen = new Set<string>();
  const result: string[] = [];
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

/** Parses the Workers AI server-sent-event stream and yields text tokens. */
async function* readAiTokens(stream: ReadableStream<Uint8Array>): AsyncGenerator<string> {
  const reader = stream.pipeThrough(new TextDecoderStream()).getReader();
  let buffer = "";
  try {
    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      buffer += value;
      let newline: number;
      while ((newline = buffer.indexOf("\n")) !== -1) {
        const line = buffer.slice(0, newline).trim();
        buffer = buffer.slice(newline + 1);
        if (!line.startsWith("data:")) continue;
        const payload = line.slice(5).trim();
        if (payload === "[DONE]") return;
        try {
          const parsed = JSON.parse(payload) as { response?: unknown };
          if (typeof parsed.response === "string" && parsed.response) {
            yield parsed.response;
          }
        } catch {
          // Ignore keep-alive or malformed lines.
        }
      }
    }
  } finally {
    reader.releaseLock();
  }
}
