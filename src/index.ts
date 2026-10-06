import { ChatSession, MAX_MESSAGE_CHARS, MODEL } from "./chat-session";
import { MemoryWorkflow } from "./memory-workflow";

export { ChatSession, MemoryWorkflow };

const SESSION_ID_PATTERN = /^[A-Za-z0-9-]{8,64}$/;
const SESSION_ROUTE = /^\/api\/sessions\/([^/]+)(\/[a-z/]*)?$/;

function json(data: unknown, status = 200): Response {
  return Response.json(data, { status, headers: { "cache-control": "no-store" } });
}

function errorResponse(message: string, status: number): Response {
  return json({ error: message }, status);
}

async function handleChat(request: Request, session: DurableObjectStub<ChatSession>): Promise<Response> {
  // Requiring JSON forces a CORS preflight for cross-origin callers.
  if (!request.headers.get("content-type")?.toLowerCase().includes("application/json")) {
    return errorResponse("Expected an application/json body", 415);
  }

  let body: unknown;
  try {
    body = await request.json();
  } catch {
    return errorResponse("Invalid JSON body", 400);
  }

  const raw = (body as { message?: unknown } | null)?.message;
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
    body: JSON.stringify({ message }),
  });
}

export default {
  async fetch(request, env): Promise<Response> {
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
  },
} satisfies ExportedHandler<Env>;
