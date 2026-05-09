// SSE consumer for the AI chat stream.
//
// The browser's native EventSource only supports GET, but POST
// /ai/chat/stream has a JSON body — so this client hand-rolls a
// minimal SSE parser over fetch + ReadableStream. The parser is
// cursor-friendly so resume-with-`Last-Event-ID` extensions are
// additive.
//
// Wire format produced by `flowfile_core/ai/streaming.py`:
//   - Comment lines `: keepalive\n\n` every 15s — silently dropped.
//   - `event: chunk\ndata: {"content_delta": "..."}\n\n`
//   - `event: tool_call\nid: <call_id>\ndata: {...}\n\n`  (not emitted
//     by the read-only chat surface but the parser handles them anyway).
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
  // Backend builds a context-rich PromptContext when set.
  // Omitted = identity-only prompt.
  flow_id?: number | null;
  selected_node_ids?: number[] | null;
  // Parsed mention strings (e.g. ["@flow", "@node:filter_3"]). When
  // omitted and no selection is set, the backend defaults to ``@flow``
  // so chat is grounded in the whole graph by default.
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
      // The chat surface is read-only, but the parser tolerates
      // tool_call so other surfaces share the wire layer.
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

export type InlineActionType = "explain" | "add_description" | "regenerate_code";

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
  // POSTs the failure context to ``/ai/explain_run_failure``. The
  // backend builds the rich prompt server-side; the client just hands
  // off ``{flow_id, node_id, error_message?, ...}`` and consumes the
  // same SSE wire format ``streamChat`` uses.
  await _postSseRequest("ai/explain_run_failure", body, handlers, signal);
};

export const streamGenerateDocumentation = async (
  body: GenerateDocumentationRequest,
  handlers: ChatStreamHandlers,
  signal?: AbortSignal,
): Promise<void> => {
  // POSTs ``{flow_id, provider, ...}`` to ``/ai/generate_documentation``.
  // The backend pins every node in the flow into ``render_prompt_context``
  // and appends a markdown-shape contract; the client just consumes
  // the same SSE wire format the chat + run-failure routes use.
  await _postSseRequest("ai/generate_documentation", body, handlers, signal);
};

export const streamInlineAction = async (
  body: InlineActionRequest,
  handlers: ChatStreamHandlers,
  signal?: AbortSignal,
): Promise<void> => {
  // POSTs ``{flow_id, node_id, action, provider, ...}`` to
  // ``/ai/inline_action``. The backend builds the schema-grounded
  // prompt and appends an action-specific instruction block; the
  // client consumes the same SSE wire format as the chat /
  // run-failure / docgen routes.
  await _postSseRequest("ai/inline_action", body, handlers, signal);
};

export const streamLineageQuestion = async (
  body: LineageQuestionRequest,
  handlers: ChatStreamHandlers,
  signal?: AbortSignal,
): Promise<void> => {
  // POSTs ``{flow_id, question, provider, focus_node_id?, ...}`` to
  // ``/ai/lineage_question``. The backend resolves the flow's catalog
  // registration via ``flow.flow_settings.source_registration_id`` (or
  // falls back to ``latest_run_info``), builds a per-node history
  // block, and appends the user's question; the client consumes the
  // same SSE wire format as the chat / run-failure / docgen /
  // inline-action routes.
  await _postSseRequest("ai/lineage_question", body, handlers, signal);
};

// --------------------------------------------------------------------------- //
// Multi-turn planner agent                                                     //
// --------------------------------------------------------------------------- //

export interface AgentStartRequest {
  flow_id: number;
  prompt: string;
  surface?: "agent" | "agent_complex" | "agent_staged" | "agent_live";
  samples_mode?: "off" | "regex";
  provider: string;
  model?: string | null;
  max_steps?: number;
  max_tokens?: number;
  max_retries_per_step?: number;
  session_id?: string | null;
  /**
   * Node ids the user has selected on the canvas at start time.
   * The planner reads this in ``_resolve_insertion_context`` as a
   * fallback upstream signal when the LLM does not emit explicit
   * ``upstream_node_ids``. When the request is built inside the
   * store, the store pulls selection from
   * ``useFlowStore().vueFlowInstance`` automatically; callers can
   * override by setting this field explicitly.
   */
  selected_node_ids?: number[] | null;
  /**
   * When ``true`` the agent state machine starts at
   * ``stage="classify"`` instead of the default ``stage="plan"``.
   * Set by ``_dispatchPromotedAgent`` (auto-promote-from-chat) since
   * the chat-mode response that preceded the promotion already
   * produced a plan-shaped narrative; emitting a fresh plan would
   * burn an extra round and duplicate the chat output. Direct
   * agent runs leave this falsy so the plan stage fires.
   */
  skip_plan?: boolean;
  /**
   * Opt-in: when ``true`` the agent runs one extra LLM round at
   * ``stage="verify_completion"`` after classify picks
   * ``op_kind="other"`` (intending to terminate). The LLM walks the
   * plan as a checklist and either certifies completion
   * (``is_complete=true`` → loop terminates) or sends control back
   * to ``classify`` for the missing op (``is_complete=false``).
   * Default off because of the extra LLM round per run.
   */
  verify_plan_completion?: boolean;
}

export interface AgentDriftDetail {
  missing_node_ids: number[];
  external_added_node_ids: number[];
  /** dict[int, str] from server serialises with string keys. Optional. */
  node_types?: Record<string, string> | null;
}

/** Op_kind classifies a tool call for UI gating + styling. */
export type AgentOpKind = "meta" | "graph" | "schema" | "codegen" | "unknown";

export interface AgentToolCallProposed {
  id: string;
  name: string;
  arguments: Record<string, unknown>;
  /** Meta ops are hidden from the user-visible chat trail. */
  op_kind?: AgentOpKind;
  /** Model's plain-English "what this step does"; null if no preamble. */
  rationale?: string | null;
  /** Server-generated fallback when ``rationale`` is null. */
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

/** ``tool_call_applied`` event payload. Emitted by the
 *  ``agent_live`` planner after a successful apply + post-apply
 *  observation (the new node is already in ``flow.nodes`` server-side).
 *  Frontend triggers a canvas refresh so the user sees the node
 *  materialise immediately. */
export interface AgentToolCallApplied {
  id: string;
  name: string;
  node_id: number;
  output_schema: Record<string, unknown>[];
  sample_rows?: Record<string, unknown>[];
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

/** `stage_advanced` event payload. Emitted by the agent_staged planner
 * each time the state machine transitions: classify → pick_type →
 * pick_upstream → fill_settings, and back to classify after a node is
 * staged. The frontend uses this to render a per-stage badge.
 *
 * `from` and `to` carry the stage names; `op_kind`,
 * `picked_node_type`, `picked_upstream_ids` are present on the
 * relevant transitions only. `completed_op` is the tool name when the
 * transition fires after a successful add or single-stage non-add op. */
export interface AgentStageAdvanced {
  from: string;
  to: string;
  op_kind?: string | null;
  picked_node_type?: string | null;
  picked_upstream_ids?: number[];
  right_input_node_id?: number | null;
  completed_op?: string;
  rationale?: string;
  session_id?: string;
}

export interface AgentSessionHandlers {
  onThinking?: (text: string) => void;
  onToolCallProposed?: (tc: AgentToolCallProposed) => void;
  onToolCallStaged?: (entry: AgentToolCallStaged) => void;
  onToolCallWarned?: (entry: AgentToolCallStaged) => void;
  onToolCallRejected?: (refusal: AgentToolCallRejected) => void;
  onToolCallApplied?: (entry: AgentToolCallApplied) => void;
  onDriftDetected?: (drift: AgentDriftDetail, sessionId: string) => void;
  onPaused?: (reason: string, sessionId: string) => void;
  onRetry?: (attempt: number, max: number) => void;
  onAbort?: (sessionId: string) => void;
  onComplete?: (result: AgentCompleteResult) => void;
  /** Emitted instead of ``onComplete`` when the planner ends on a
   * clarifying question. The store flips to ``awaiting_user_input``
   * and the frontend routes the next user message through
   * ``streamAgentFollowup``. */
  onAwaitingUserInput?: (result: AgentAwaitingUserInputResult) => void;
  /** Emitted by the agent_staged surface on each stage transition. */
  onStageAdvanced?: (payload: AgentStageAdvanced) => void;
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
    case "tool_call_applied":
      handlers.onToolCallApplied?.(payload as unknown as AgentToolCallApplied);
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
    case "awaiting_user_input":
      handlers.onAwaitingUserInput?.(payload as unknown as AgentAwaitingUserInputResult);
      break;
    case "stage_advanced":
      handlers.onStageAdvanced?.(payload as unknown as AgentStageAdvanced);
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
  lastEventId?: string,
): Promise<void> => {
  // POSTs the user's goal to ``/ai/agent/start``. The backend opens a
  // session, registers a snapshot, and streams ``PlannerEvent``s as
  // SSE. Per-event types: ``thinking`` / ``tool_call_proposed`` /
  // ``tool_call_staged`` / ``tool_call_warned`` /
  // ``tool_call_rejected`` / ``drift_detected`` / ``paused`` /
  // ``retry`` / ``abort`` / ``complete`` / ``error`` / ``info``.
  // ``id:`` carries ``f"{session_id}.{step_count}"`` for resumption.
  //
  // When ``lastEventId`` is set, the server's replay buffer flushes any
  // buffered frames newer than the cursor before live streaming
  // resumes. The /start route accepts the header for symmetry; in
  // normal use the cursor only matters on /resume.
  const token = await authService.getToken();
  if (!token) {
    handlers.onError?.("Not authenticated. Please log in again.");
    return;
  }
  const url = new URL("ai/agent/start", flowfileCorebaseURL).toString();
  const headers: Record<string, string> = {
    "Content-Type": "application/json",
    Accept: "text/event-stream",
    Authorization: `Bearer ${token}`,
  };
  if (lastEventId) {
    headers["Last-Event-ID"] = lastEventId;
  }
  const response = await fetch(url, {
    method: "POST",
    headers,
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

export type AgentFollowupAction = "rejected_diff" | "user_message";

export interface AgentFollowupRequest {
  /** ``"rejected_diff"`` — user rejected the previously staged diff;
   * ``"user_message"`` — user typed a new message after a ``complete`` /
   * ``awaiting_user_input`` */
  action: AgentFollowupAction;
  /** Free-text payload — user's rejection note (optional) for
   * ``rejected_diff``, the user's typed message (required) for
   * ``user_message``. */
  message?: string | null;
  /** Diff id the user just rejected; if omitted, the backend falls back to
   * ``session.diff_id``. Only relevant for ``action="rejected_diff"``. */
  rejected_diff_id?: string | null;
}

/** Emitted (instead of ``complete``) when a planner round ends with a
 * clarifying question and no staged ops. The frontend renders this as
 * *"Agent waiting for your reply…"* and routes the next user message
 * through the followup endpoint. */
export interface AgentAwaitingUserInputResult {
  session_id: string;
  question: string;
}

export const streamAgentFollowup = async (
  sessionId: string,
  body: AgentFollowupRequest,
  handlers: AgentSessionHandlers,
  signal?: AbortSignal,
  lastEventId?: string,
): Promise<void> => {
  // POSTs ``{action, message?, rejected_diff_id?}`` to
  // ``/ai/agent/{session_id}/followup``. The backend appends a
  // synthetic ``role="user"`` turn (rejection note OR user message)
  // to the planner's conversation, re-snapshots the graph, and
  // re-enters the loop. The SSE wire format matches ``/start`` /
  // ``/resume`` so the same ``AgentSessionHandlers`` consumes it.
  const token = await authService.getToken();
  if (!token) {
    handlers.onError?.("Not authenticated. Please log in again.");
    return;
  }
  const url = new URL(
    `ai/agent/${encodeURIComponent(sessionId)}/followup`,
    flowfileCorebaseURL,
  ).toString();
  const headers: Record<string, string> = {
    "Content-Type": "application/json",
    Accept: "text/event-stream",
    Authorization: `Bearer ${token}`,
  };
  if (lastEventId) {
    headers["Last-Event-ID"] = lastEventId;
  }
  const response = await fetch(url, {
    method: "POST",
    headers,
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
  lastEventId?: string,
): Promise<void> => {
  // Resume after drift via SSE. Body ``{action: "continue"}`` is the
  // SSE-stream form; ``"discard"`` is JSON-only and lives in
  // api/ai.api.ts. ``lastEventId`` is forwarded as the
  // ``Last-Event-ID`` header so the server's replay buffer flushes
  // buffered frames newer than the cursor before the live planner
  // resumes.
  const token = await authService.getToken();
  if (!token) {
    handlers.onError?.("Not authenticated. Please log in again.");
    return;
  }
  const url = new URL(
    `ai/agent/${encodeURIComponent(sessionId)}/resume`,
    flowfileCorebaseURL,
  ).toString();
  const headers: Record<string, string> = {
    "Content-Type": "application/json",
    Accept: "text/event-stream",
    Authorization: `Bearer ${token}`,
  };
  if (lastEventId) {
    headers["Last-Event-ID"] = lastEventId;
  }
  const response = await fetch(url, {
    method: "POST",
    headers,
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
// Chat → Agent auto-promotion routing                                          //
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
  // POSTs ``{message, provider, model?}`` to ``/ai/route``. The
  // backend's classifier collapses every internal failure mode
  // (timeout, parse error, provider hiccup) into ``verdict="chat"``,
  // so the only non-200 outcomes here are the standard 4xx provider /
  // auth / flag failures. ``AiStreamHttpError`` mirrors the existing
  // chat-stream error shape so the store's failure handling stays
  // uniform.
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
