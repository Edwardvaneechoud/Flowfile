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
  // W28 — backend builds a context-rich PromptContext via W22 when set.
  // Omitted = backwards-compatible W26 identity-only prompt.
  flow_id?: number | null;
  selected_node_ids?: number[] | null;
  // W24's parsed mention strings (e.g. ["@flow", "@node:filter_3"]).
  // When omitted and no selection is set, the backend defaults to
  // ``@flow`` so chat is grounded in the whole graph by default.
  mentions?: string[] | null;
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

export type InlineActionType =
  | "explain"
  | "optimise"
  | "document"
  | "regenerate_code"
  | "suggest_filters";

export interface InlineActionRequest {
  flow_id: number;
  node_id: number;
  action: InlineActionType;
  provider: string;
  model?: string | null;
  samples_mode?: "off" | "regex";
  max_tokens?: number | null;
}

export interface LineageQuestionRequest {
  flow_id: number;
  question: string;
  provider: string;
  model?: string | null;
  focus_node_id?: number | null;
  history_limit?: number | null;
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

export const streamInlineAction = async (
  body: InlineActionRequest,
  handlers: ChatStreamHandlers,
  signal?: AbortSignal,
): Promise<void> => {
  // W21 — POSTs ``{flow_id, node_id, action, provider, ...}`` to
  // ``/ai/inline_action``. The backend builds the schema-grounded W22 prompt
  // and appends an action-specific instruction block; the client consumes
  // the same SSE wire format as the chat / run-failure / docgen routes.
  await _postSseRequest("ai/inline_action", body, handlers, signal);
};

export const streamLineageQuestion = async (
  body: LineageQuestionRequest,
  handlers: ChatStreamHandlers,
  signal?: AbortSignal,
): Promise<void> => {
  // W51 — POSTs ``{flow_id, question, provider, focus_node_id?, ...}`` to
  // ``/ai/lineage_question``. The backend resolves the flow's catalog
  // registration via ``flow.flow_settings.source_registration_id`` (or
  // falls back to ``latest_run_info``), builds a per-node history block,
  // and appends the user's question; the client consumes the same SSE
  // wire format as the chat / run-failure / docgen / inline-action routes.
  await _postSseRequest("ai/lineage_question", body, handlers, signal);
};

// --------------------------------------------------------------------------- //
// W40 — multi-turn planner agent                                                //
// --------------------------------------------------------------------------- //

export interface AgentStartRequest {
  flow_id: number;
  prompt: string;
  surface?: "agent" | "agent_complex";
  samples_mode?: "off" | "regex";
  provider: string;
  model?: string | null;
  max_steps?: number;
  max_tokens?: number;
  max_retries_per_step?: number;
  session_id?: string | null;
}

export interface AgentDriftDetail {
  missing_node_ids: number[];
  external_added_node_ids: number[];
  /** dict[int, str] from server serialises with string keys. Optional. */
  node_types?: Record<string, string> | null;
}

/** W38 op_kind classifies a tool call for UI gating + styling. */
export type AgentOpKind = "meta" | "graph" | "schema" | "codegen" | "unknown";

export interface AgentToolCallProposed {
  id: string;
  name: string;
  arguments: Record<string, unknown>;
  /** W38 — meta ops are hidden from the user-visible chat trail. */
  op_kind?: AgentOpKind;
  /** W38 — model's plain-English "what this step does"; null if no preamble. */
  rationale?: string | null;
  /** W38 — server-generated fallback when ``rationale`` is null. */
  arg_summary?: string | null;
}

export interface AgentToolCallStaged {
  id: string;
  name: string;
  node_id: number | null;
  predicted_output_schema: Record<string, unknown>[] | null;
  warnings: string[];
  op_kind?: AgentOpKind;
  rationale?: string | null;
  arg_summary?: string | null;
}

export interface AgentToolCallRejected {
  id: string;
  name: string;
  reason: string;
  detail: string;
  op_kind?: AgentOpKind;
  rationale?: string | null;
  arg_summary?: string | null;
}

export interface AgentCompleteResult {
  session_id: string;
  diff_id: string | null;
  op_count: number;
  rationale: string | null;
  diff_payload: Record<string, unknown> | null;
}

export interface AgentSessionHandlers {
  onThinking?: (text: string) => void;
  onToolCallProposed?: (tc: AgentToolCallProposed) => void;
  onToolCallStaged?: (entry: AgentToolCallStaged) => void;
  onToolCallWarned?: (entry: AgentToolCallStaged) => void;
  onToolCallRejected?: (refusal: AgentToolCallRejected) => void;
  onDriftDetected?: (drift: AgentDriftDetail, sessionId: string) => void;
  onPaused?: (reason: string, sessionId: string) => void;
  onRetry?: (attempt: number, max: number) => void;
  onAbort?: (sessionId: string) => void;
  onComplete?: (result: AgentCompleteResult) => void;
  onInfo?: (payload: Record<string, unknown>) => void;
  onError?: (message: string) => void;
}

const dispatchPlannerEvent = (parsed: ParsedEvent, handlers: AgentSessionHandlers): void => {
  if (parsed.event === null) return; // keepalive comment
  let payload: Record<string, unknown> = {};
  try {
    payload = parsed.data ? JSON.parse(parsed.data) : {};
  } catch (err) {
    console.error("aiStreamClient: failed to parse planner SSE data", err, parsed.data);
    handlers.onError?.("malformed planner SSE payload from server");
    return;
  }

  switch (parsed.event) {
    case "thinking": {
      const text = typeof payload.text === "string" ? payload.text : "";
      if (text) handlers.onThinking?.(text);
      break;
    }
    case "tool_call_proposed":
      handlers.onToolCallProposed?.(payload as unknown as AgentToolCallProposed);
      break;
    case "tool_call_staged":
      handlers.onToolCallStaged?.(payload as unknown as AgentToolCallStaged);
      break;
    case "tool_call_warned":
      handlers.onToolCallWarned?.(payload as unknown as AgentToolCallStaged);
      break;
    case "tool_call_rejected":
      handlers.onToolCallRejected?.(payload as unknown as AgentToolCallRejected);
      break;
    case "drift_detected": {
      const drift = (payload as { drift?: AgentDriftDetail }).drift;
      const sid = (payload as { session_id?: string }).session_id ?? "";
      if (drift) handlers.onDriftDetected?.(drift, sid);
      break;
    }
    case "paused": {
      const reason = typeof payload.reason === "string" ? payload.reason : "paused";
      const sid = typeof payload.session_id === "string" ? payload.session_id : "";
      handlers.onPaused?.(reason, sid);
      break;
    }
    case "retry": {
      const attempt = typeof payload.attempt === "number" ? payload.attempt : 1;
      const max = typeof payload.max === "number" ? payload.max : 3;
      handlers.onRetry?.(attempt, max);
      break;
    }
    case "abort": {
      const sid = typeof payload.session_id === "string" ? payload.session_id : "";
      handlers.onAbort?.(sid);
      break;
    }
    case "complete":
      handlers.onComplete?.(payload as unknown as AgentCompleteResult);
      break;
    case "error": {
      const msg = typeof payload.message === "string" ? payload.message : "AI agent error";
      handlers.onError?.(msg);
      break;
    }
    case "info":
      handlers.onInfo?.(payload);
      break;
    default:
      break;
  }
};

const _consumePlannerSse = async (
  response: Response,
  handlers: AgentSessionHandlers,
): Promise<void> => {
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
      let separatorIndex = buffer.search(DOUBLE_NEWLINE);
      while (separatorIndex !== -1) {
        const block = buffer.slice(0, separatorIndex);
        const matched = buffer.slice(separatorIndex).match(/^\r?\n\r?\n/);
        const advance = (matched?.[0] ?? "\n\n").length;
        buffer = buffer.slice(separatorIndex + advance);
        const parsed = parseEventBlock(block);
        if (parsed !== null) dispatchPlannerEvent(parsed, handlers);
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

export const streamAgentSession = async (
  body: AgentStartRequest,
  handlers: AgentSessionHandlers,
  signal?: AbortSignal,
): Promise<void> => {
  // W40 — POSTs the user's goal to ``/ai/agent/start``. The backend opens a
  // session, registers a snapshot, and streams ``PlannerEvent``s as SSE.
  // Per-event types: ``thinking`` / ``tool_call_proposed`` /
  // ``tool_call_staged`` / ``tool_call_warned`` / ``tool_call_rejected`` /
  // ``drift_detected`` / ``paused`` / ``retry`` / ``abort`` / ``complete`` /
  // ``error`` / ``info``. ``id:`` carries ``f"{session_id}.{step_count}"``
  // for W42 resumption.
  const token = await authService.getToken();
  if (!token) {
    handlers.onError?.("Not authenticated. Please log in again.");
    return;
  }
  const url = new URL("ai/agent/start", flowfileCorebaseURL).toString();
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
  await _consumePlannerSse(response, handlers);
};

export const resumeAgentSessionStream = async (
  sessionId: string,
  handlers: AgentSessionHandlers,
  signal?: AbortSignal,
): Promise<void> => {
  // W40 — resume after drift via SSE. Body ``{action: "continue"}`` is the
  // SSE-stream form; ``"discard"`` is JSON-only and lives in api/ai.api.ts.
  const token = await authService.getToken();
  if (!token) {
    handlers.onError?.("Not authenticated. Please log in again.");
    return;
  }
  const url = new URL(
    `ai/agent/${encodeURIComponent(sessionId)}/resume`,
    flowfileCorebaseURL,
  ).toString();
  const response = await fetch(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Accept: "text/event-stream",
      Authorization: `Bearer ${token}`,
    },
    body: JSON.stringify({ action: "continue" }),
    signal,
    credentials: "include",
  });
  if (!response.ok) {
    const detail = await readErrorBody(response);
    throw new AiStreamHttpError(response.status, detail);
  }
  await _consumePlannerSse(response, handlers);
};

// --------------------------------------------------------------------------- //
// W58 — Chat → Agent auto-promotion routing                                     //
// --------------------------------------------------------------------------- //

export interface RouteHistoryEntry {
  role: "user" | "assistant";
  content: string;
}

export interface RouteRequestBody {
  message: string;
  provider: string;
  model?: string | null;
  /** Recent chat turns, oldest first, excluding the current `message`. The
   * backend caps this at the configured turns budget; oversized history
   * is trimmed server-side. */
  history?: RouteHistoryEntry[];
}

export type RouteVerdict = "chat" | "agent";
export type RouteKind = "build" | "chat" | "ambiguous";

export interface RouteResponseBody {
  verdict: RouteVerdict;
  kind: RouteKind;
  confidence: number;
  reason: string;
  /** End-to-end classifier latency in ms. Surfaced for telemetry; the
   * banner doesn't render it. */
  latencyMs: number;
}

interface PyRouteResponseBody {
  verdict: RouteVerdict;
  kind: RouteKind;
  confidence: number;
  reason: string;
  latency_ms: number;
}

export const routeMessage = async (
  body: RouteRequestBody,
  signal?: AbortSignal,
): Promise<RouteResponseBody> => {
  // W58 — POSTs ``{message, provider, model?}`` to ``/ai/route``. The
  // backend's classifier collapses every internal failure mode (timeout,
  // parse error, provider hiccup) into ``verdict="chat"``, so the only
  // non-200 outcomes here are the standard 4xx provider / auth / flag
  // failures. ``AiStreamHttpError`` mirrors the existing chat-stream
  // error shape so the store's failure handling stays uniform.
  const token = await authService.getToken();
  if (!token) {
    throw new AiStreamHttpError(401, "Not authenticated. Please log in again.");
  }

  const url = new URL("ai/route", flowfileCorebaseURL).toString();
  const response = await fetch(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Accept: "application/json",
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

  const raw = (await response.json()) as PyRouteResponseBody;
  return {
    verdict: raw.verdict,
    kind: raw.kind,
    confidence: raw.confidence,
    reason: raw.reason,
    latencyMs: raw.latency_ms,
  };
};

export const _internal = { parseEventBlock, dispatch, dispatchPlannerEvent };
