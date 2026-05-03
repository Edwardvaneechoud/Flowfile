// SSE consumer for the AI chat stream (W20).
//
// The browser's native EventSource only supports GET, but POST /ai/chat/stream
// has a JSON body — so this client hand-rolls a minimal SSE parser over a
// fetch + ReadableStream. Future W42 will extend this with `Last-Event-ID`
// echo for resumption; the parser is already cursor-friendly, so that change
// is additive.
//
// Wire format produced by `flowfile_core/ai/streaming.py`:
//   - Comment lines `: keepalive\n\n` every 15s — silently dropped.
//   - `event: chunk\ndata: {"content_delta": "..."}\n\n`
//   - `event: tool_call\nid: <call_id>\ndata: {...}\n\n`  (W31+; not emitted
//     by W20's read-only chat surface but the parser handles them anyway).
//   - `event: done\ndata: {"finish_reason": "stop"}\n\n`
//   - `event: error\ndata: {"message": "..."}\n\n`

import { flowfileCorebaseURL } from "../../config/constants";
import authService from "./auth.service";

export interface ChatMessageBody {
  role: "system" | "user" | "assistant";
  content: string;
}

export interface ChatStreamRequest {
  provider: string;
  model?: string | null;
  surface?: string | null;
  messages: ChatMessageBody[];
  max_tokens?: number | null;
}

export interface ChatStreamHandlers {
  onChunk?: (delta: string) => void;
  onDone?: (finishReason: string) => void;
  onError?: (message: string) => void;
}

export class AiStreamHttpError extends Error {
  constructor(
    public readonly status: number,
    public readonly detail: string,
  ) {
    super(detail || `HTTP ${status}`);
    this.name = "AiStreamHttpError";
  }
}

const DOUBLE_NEWLINE = /\r?\n\r?\n/;

interface ParsedEvent {
  event: string | null;
  data: string;
  id: string | null;
}

const parseEventBlock = (block: string): ParsedEvent | null => {
  // Comment-only lines (`: keepalive`) carry no event/data and should be
  // dropped silently — proxies see traffic, the client doesn't.
  let event: string | null = null;
  let id: string | null = null;
  const dataLines: string[] = [];
  let sawAnyField = false;

  for (const rawLine of block.split(/\r?\n/)) {
    if (rawLine.startsWith(":")) continue; // comment
    if (rawLine === "") continue;
    sawAnyField = true;
    if (rawLine.startsWith("event: ")) {
      event = rawLine.slice("event: ".length);
    } else if (rawLine.startsWith("id: ")) {
      id = rawLine.slice("id: ".length);
    } else if (rawLine.startsWith("data: ")) {
      dataLines.push(rawLine.slice("data: ".length));
    }
  }
  if (!sawAnyField) return null;
  return { event, id, data: dataLines.join("\n") };
};

const dispatch = (parsed: ParsedEvent, handlers: ChatStreamHandlers): void => {
  if (parsed.event === null) return; // keepalive comment block
  let payload: Record<string, unknown> = {};
  try {
    payload = parsed.data ? JSON.parse(parsed.data) : {};
  } catch (err) {
    console.error("aiStreamClient: failed to parse SSE data", err, parsed.data);
    handlers.onError?.("malformed SSE payload from server");
    return;
  }

  switch (parsed.event) {
    case "chunk": {
      const delta = typeof payload.content_delta === "string" ? payload.content_delta : "";
      if (delta) handlers.onChunk?.(delta);
      break;
    }
    case "done": {
      const finish = typeof payload.finish_reason === "string" ? payload.finish_reason : "stop";
      handlers.onDone?.(finish);
      break;
    }
    case "error": {
      const message = typeof payload.message === "string" ? payload.message : "AI stream error";
      handlers.onError?.(message);
      break;
    }
    case "tool_call": {
      // W20 is read-only, but the parser tolerates tool_call so W31 can plug
      // tool dispatch in without rewriting the wire layer.
      break;
    }
    default:
      // Forward-compatible — unknown event names ignored.
      break;
  }
};

const readErrorBody = async (response: Response): Promise<string> => {
  try {
    const data = await response.json();
    if (data && typeof (data as { detail?: unknown }).detail === "string") {
      return (data as { detail: string }).detail;
    }
    return JSON.stringify(data);
  } catch {
    try {
      return await response.text();
    } catch {
      return `HTTP ${response.status}`;
    }
  }
};

export interface ExplainRunFailureRequest {
  flow_id: number;
  node_id: number;
  provider: string;
  model?: string | null;
  error_message?: string | null;
  samples_mode?: "off" | "regex";
  max_tokens?: number | null;
}

export interface GenerateDocumentationRequest {
  flow_id: number;
  provider: string;
  model?: string | null;
  samples_mode?: "off" | "regex";
  max_tokens?: number | null;
}

const _consumeSseResponse = async (
  response: Response,
  handlers: ChatStreamHandlers,
): Promise<void> => {
  // Shared SSE consumer for the chat-stream and the explain-run-failure
  // routes. Both endpoints emit the same wire format from
  // ``flowfile_core/ai/streaming.py``, so the consumer is route-agnostic.
  if (!response.body) {
    handlers.onError?.("Server returned an empty stream.");
    return;
  }

  const reader = response.body.getReader();
  const decoder = new TextDecoder("utf-8");
  let buffer = "";

  try {
    let streamDone = false;
    while (!streamDone) {
      const { value, done } = await reader.read();
      if (done) {
        streamDone = true;
        break;
      }
      buffer += decoder.decode(value, { stream: true });

      // SSE event blocks are separated by a blank line (\n\n).
      let separatorIndex = buffer.search(DOUBLE_NEWLINE);
      while (separatorIndex !== -1) {
        const block = buffer.slice(0, separatorIndex);
        const matched = buffer.slice(separatorIndex).match(/^\r?\n\r?\n/);
        const advance = (matched?.[0] ?? "\n\n").length;
        buffer = buffer.slice(separatorIndex + advance);

        const parsed = parseEventBlock(block);
        if (parsed !== null) dispatch(parsed, handlers);

        separatorIndex = buffer.search(DOUBLE_NEWLINE);
      }
    }
  } finally {
    try {
      reader.releaseLock();
    } catch {
      /* already released — fine */
    }
  }
};

const _postSseRequest = async (
  path: string,
  body: unknown,
  handlers: ChatStreamHandlers,
  signal?: AbortSignal,
): Promise<void> => {
  const token = await authService.getToken();
  if (!token) {
    handlers.onError?.("Not authenticated. Please log in again.");
    return;
  }

  const url = new URL(path, flowfileCorebaseURL).toString();

  const response = await fetch(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Accept: "text/event-stream",
      Authorization: `Bearer ${token}`,
    },
    body: JSON.stringify(body),
    signal,
    credentials: "include",
  });

  if (!response.ok) {
    const detail = await readErrorBody(response);
    throw new AiStreamHttpError(response.status, detail);
  }

  await _consumeSseResponse(response, handlers);
};

export const streamChat = async (
  body: ChatStreamRequest,
  handlers: ChatStreamHandlers,
  signal?: AbortSignal,
): Promise<void> => {
  await _postSseRequest("ai/chat/stream", body, handlers, signal);
};

export const streamRunFailureExplanation = async (
  body: ExplainRunFailureRequest,
  handlers: ChatStreamHandlers,
  signal?: AbortSignal,
): Promise<void> => {
  // W23 — POSTs the failure context to ``/ai/explain_run_failure``. The
  // backend builds the rich W22 prompt server-side; the client just hands
  // off ``{flow_id, node_id, error_message?, ...}`` and consumes the same
  // SSE wire format ``streamChat`` uses.
  await _postSseRequest("ai/explain_run_failure", body, handlers, signal);
};

export const streamGenerateDocumentation = async (
  body: GenerateDocumentationRequest,
  handlers: ChatStreamHandlers,
  signal?: AbortSignal,
): Promise<void> => {
  // W50 — POSTs ``{flow_id, provider, ...}`` to ``/ai/generate_documentation``.
  // The backend pins every node in the flow into W22's render_prompt_context
  // and appends a markdown-shape contract; the client just consumes the same
  // SSE wire format the chat + run-failure routes use.
  await _postSseRequest("ai/generate_documentation", body, handlers, signal);
};

export const _internal = { parseEventBlock, dispatch };
