import {
  WorkflowEntrypoint,
  type WorkflowEvent,
  type WorkflowStep,
  type WorkflowStepConfig,
} from "cloudflare:workers";
import { MAX_FACTS, MODEL, type MemorySnapshot, type MemoryWorkflowParams, type Role } from "./chat-session";

const AI_STEP_CONFIG = {
  retries: { limit: 3, delay: "5 seconds", backoff: "exponential" },
  timeout: "2 minutes",
} satisfies WorkflowStepConfig;

const MAX_TRANSCRIPT_MESSAGE_CHARS = 2000;

const SUMMARY_SYSTEM_PROMPT = `You maintain a running summary of a conversation between a user and an AI assistant named Atlas.
Merge the existing summary with the new transcript into a single updated summary of at most 150 words.
Focus on the topics discussed, decisions made, open questions and the user's goals.
Write in the third person ("The user asked..."). Output only the summary text, with no preamble.`;

const FACTS_SYSTEM_PROMPT = `You maintain a list of durable facts about the USER of a chat assistant: their name, preferences, goals, background, projects and constraints.
Given the existing facts and a new transcript, return the complete updated list:
- keep facts that are still valid,
- add new facts the user stated about themselves,
- update or remove facts contradicted by the new transcript.
Do not include facts about the assistant, general knowledge, or temporary details of a single question.
Each fact must be a short, standalone sentence. Return at most ${MAX_FACTS} facts.
Respond with JSON of the form {"facts": ["..."]}.`;

const FACTS_SCHEMA = {
  type: "object",
  properties: {
    facts: { type: "array", items: { type: "string" } },
  },
  required: ["facts"],
};

/**
 * Durable, retryable pipeline that condenses a chat session into long-term memory:
 * load transcript -> summarize -> extract user facts -> persist back to the Durable Object.
 */
export class MemoryWorkflow extends WorkflowEntrypoint<Env, MemoryWorkflowParams> {
  async run(event: WorkflowEvent<MemoryWorkflowParams>, step: WorkflowStep) {
    const { instanceId } = event;
    const namespace = this.env.CHAT_SESSION;
    const session = namespace.get(namespace.idFromString(event.payload.sessionObjectId));

    try {
      const snapshot = await step.do("load conversation", async (): Promise<MemorySnapshot> => {
        const data = await session.getMemorySnapshot();
        return {
          summary: data.summary,
          facts: [...data.facts],
          messages: data.messages.map((m) => ({ role: m.role, content: m.content })),
          throughId: data.throughId,
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

      const summary = await step.do("summarize conversation", AI_STEP_CONFIG, async () =>
        this.summarize(snapshot.summary, transcript),
      );

      const facts = await step.do("extract user facts", AI_STEP_CONFIG, async () =>
        this.extractFacts(snapshot.facts, transcript),
      );

      const result = await step.do("save memory", async () => {
        const { applied } = await session.applyMemory({
          instanceId,
          summary,
          facts,
          throughId: snapshot.throughId,
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

  private async summarize(previousSummary: string, transcript: string): Promise<string> {
    const result = await this.env.AI.run(MODEL, {
      messages: [
        { role: "system", content: SUMMARY_SYSTEM_PROMPT },
        {
          role: "user",
          content: `Existing summary:\n${previousSummary || "(none)"}\n\nNew transcript:\n${transcript}`,
        },
      ],
      max_tokens: 400,
      temperature: 0.2,
    });
    const summary = responseText(result).trim();
    if (!summary) {
      throw new Error("Model returned an empty summary");
    }
    return summary;
  }

  private async extractFacts(existingFacts: string[], transcript: string): Promise<string[]> {
    const existing = existingFacts.length ? existingFacts.map((f) => `- ${f}`).join("\n") : "(none)";
    const result = await this.env.AI.run(MODEL, {
      messages: [
        { role: "system", content: FACTS_SYSTEM_PROMPT },
        { role: "user", content: `Existing facts:\n${existing}\n\nNew transcript:\n${transcript}` },
      ],
      response_format: { type: "json_schema", json_schema: FACTS_SCHEMA },
      max_tokens: 600,
      temperature: 0.1,
    });

    const response = (result as { response?: unknown }).response;
    const parsed: unknown = typeof response === "string" ? JSON.parse(extractJson(response)) : response;
    const facts = (parsed as { facts?: unknown } | null)?.facts;
    if (!Array.isArray(facts)) {
      throw new Error("Model response did not contain a facts array");
    }
    return facts.filter((f): f is string => typeof f === "string");
  }
}

function formatTranscript(messages: { role: Role; content: string }[]): string {
  return messages
    .map((m) => {
      const speaker = m.role === "user" ? "User" : "Assistant";
      const content =
        m.content.length > MAX_TRANSCRIPT_MESSAGE_CHARS
          ? `${m.content.slice(0, MAX_TRANSCRIPT_MESSAGE_CHARS)}…`
          : m.content;
      return `${speaker}: ${content}`;
    })
    .join("\n\n");
}

function responseText(result: unknown): string {
  const response = (result as { response?: unknown } | null)?.response;
  return typeof response === "string" ? response : "";
}

/** Tolerates models that wrap JSON in prose or code fences. */
function extractJson(text: string): string {
  const start = text.indexOf("{");
  const end = text.lastIndexOf("}");
  return start !== -1 && end > start ? text.slice(start, end + 1) : text;
}
