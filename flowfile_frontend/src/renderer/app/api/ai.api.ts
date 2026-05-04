// Frontend API surface for the AI subsystem (W20 + W23 + W34 + W50).
//
// Right now this is a re-export shim around five seams:
//   - `streamChat` for the read-only chat stream (W20).
//   - `streamRunFailureExplanation` for "Fix with AI" on a failed node (W23).
//   - `streamGenerateDocumentation` for the canvas-level "Generate
//     documentation" action (W50).
//   - `fetchFormulaSuggestions` / `fetchJoinKeySuggestions` for settings
//     autocomplete (W34) — fast non-streaming JSON.
//   - the W12 BYOK provider listing reused so the chat panel can pick a
//     configured provider without forcing the user back to the settings tab.
//
// W30+ will add tool-catalog endpoints, W22 will add context-aware variants
// of `streamChat`, W43 will add per-flow session listing — all extending
// this module rather than replacing it.

import axios from "../services/axios.config";
import { fetchAiProviders } from "../views/AiProvidersView/api";
import { AiDisabledError, AI_DISABLED_DETAIL } from "../views/AiProvidersView/api";
import {
  streamChat,
  streamGenerateDocumentation,
  streamInlineAction,
  streamLineageQuestion,
  streamRunFailureExplanation,
} from "../services/aiStreamClient";

export type {
  ChatMessageBody,
  ChatStreamRequest,
  ChatStreamHandlers,
  ExplainRunFailureRequest,
  GenerateDocumentationRequest,
  InlineActionRequest,
  InlineActionType,
  LineageQuestionRequest,
} from "../services/aiStreamClient";
export { AiStreamHttpError } from "../services/aiStreamClient";
export {
  streamChat,
  streamGenerateDocumentation,
  streamInlineAction,
  streamLineageQuestion,
  streamRunFailureExplanation,
  fetchAiProviders,
};
export { AiDisabledError, AI_DISABLED_DETAIL };

// --------------------------------------------------------------------------
// Settings autocomplete (W34) — non-streaming JSON wrappers around
// /ai/autocomplete/{formula,join_keys}.
// --------------------------------------------------------------------------

export interface FormulaSuggestion {
  insertText: string;
  label: string;
  description: string | null;
  verified: boolean;
}

export interface FormulaSuggestionsResponse {
  suggestions: FormulaSuggestion[];
  degraded: boolean;
  reason: string | null;
}

export interface JoinKeyPair {
  leftCol: string;
  rightCol: string;
  confidence: number;
  rationale: string | null;
}

export interface JoinKeySuggestionsResponse {
  keyPairs: JoinKeyPair[];
  degraded: boolean;
  reason: string | null;
}

export interface FormulaAutocompleteRequest {
  flowId: number;
  nodeId: number | string;
  partialText: string;
  intent?: string | null;
  provider?: string;
  model?: string | null;
  maxSuggestions?: number;
  timeout?: number;
}

export interface JoinKeyAutocompleteRequest {
  flowId: number;
  leftNodeId: number | string;
  rightNodeId: number | string;
  how?: string;
  provider?: string;
  model?: string | null;
  maxPairs?: number;
  timeout?: number;
}

interface PyFormulaSuggestion {
  insert_text: string;
  label: string;
  description: string | null;
  verified: boolean;
}

interface PyFormulaSuggestionsResponse {
  suggestions: PyFormulaSuggestion[];
  degraded: boolean;
  reason: string | null;
}

interface PyJoinKeyPair {
  left_col: string;
  right_col: string;
  confidence: number;
  rationale: string | null;
}

interface PyJoinKeySuggestionsResponse {
  key_pairs: PyJoinKeyPair[];
  degraded: boolean;
  reason: string | null;
}

const fromPyFormulaSuggestion = (raw: PyFormulaSuggestion): FormulaSuggestion => ({
  insertText: raw.insert_text,
  label: raw.label,
  description: raw.description,
  verified: raw.verified,
});

const fromPyJoinKeyPair = (raw: PyJoinKeyPair): JoinKeyPair => ({
  leftCol: raw.left_col,
  rightCol: raw.right_col,
  confidence: raw.confidence,
  rationale: raw.rationale,
});

// 503 with W17's DISABLED_DETAIL becomes AiDisabledError so callers can
// render a dedicated empty-state without sniffing axios error shapes.
const isAiDisabledError = (error: unknown): boolean => {
  const detail = (error as { response?: { data?: { detail?: unknown }; status?: number } })
    ?.response?.data?.detail;
  const status = (error as { response?: { status?: number } })?.response?.status;
  return status === 503 && typeof detail === "string" && detail === AI_DISABLED_DETAIL;
};

export const fetchFormulaSuggestions = async (
  body: FormulaAutocompleteRequest,
  signal?: AbortSignal,
): Promise<FormulaSuggestionsResponse> => {
  const payload: Record<string, unknown> = {
    flow_id: body.flowId,
    node_id: body.nodeId,
    partial_text: body.partialText,
  };
  if (body.intent !== undefined && body.intent !== null) payload.intent = body.intent;
  if (body.provider !== undefined) payload.provider = body.provider;
  if (body.model !== undefined && body.model !== null) payload.model = body.model;
  if (body.maxSuggestions !== undefined) payload.max_suggestions = body.maxSuggestions;
  if (body.timeout !== undefined) payload.timeout = body.timeout;

  try {
    const response = await axios.post<PyFormulaSuggestionsResponse>(
      "/ai/autocomplete/formula",
      payload,
      { signal },
    );
    return {
      suggestions: response.data.suggestions.map(fromPyFormulaSuggestion),
      degraded: response.data.degraded,
      reason: response.data.reason,
    };
  } catch (error) {
    if (isAiDisabledError(error)) throw new AiDisabledError();
    throw error;
  }
};

export const fetchJoinKeySuggestions = async (
  body: JoinKeyAutocompleteRequest,
  signal?: AbortSignal,
): Promise<JoinKeySuggestionsResponse> => {
  const payload: Record<string, unknown> = {
    flow_id: body.flowId,
    left_node_id: body.leftNodeId,
    right_node_id: body.rightNodeId,
  };
  if (body.how !== undefined) payload.how = body.how;
  if (body.provider !== undefined) payload.provider = body.provider;
  if (body.model !== undefined && body.model !== null) payload.model = body.model;
  if (body.maxPairs !== undefined) payload.max_pairs = body.maxPairs;
  if (body.timeout !== undefined) payload.timeout = body.timeout;

  try {
    const response = await axios.post<PyJoinKeySuggestionsResponse>(
      "/ai/autocomplete/join_keys",
      payload,
      { signal },
    );
    return {
      keyPairs: response.data.key_pairs.map(fromPyJoinKeyPair),
      degraded: response.data.degraded,
      reason: response.data.reason,
    };
  } catch (error) {
    if (isAiDisabledError(error)) throw new AiDisabledError();
    throw error;
  }
};

// --------------------------------------------------------------------------
// Edge ghost-node suggestions (W32) — non-streaming JSON wrapper around
// /ai/suggest_next_node. Matches the autocomplete shape: a hover-fast
// synchronous call with a degraded fallback when the LLM can't produce a
// schema-grounded result.
// --------------------------------------------------------------------------

export interface SchemaColumn {
  name: string;
  dataType: string | null;
  nullable: boolean | null;
}

export interface NextNodeSuggestion {
  nodeType: string;
  settings: Record<string, unknown>;
  label: string;
  description: string | null;
  predictedOutputSchema: SchemaColumn[] | null;
  rationale: string | null;
}

export interface NextNodeSuggestionsResponse {
  suggestions: NextNodeSuggestion[];
  degraded: boolean;
  reason: string | null;
}

export interface SuggestNextNodeRequest {
  flowId: number;
  upstreamNodeId: number | string;
  provider?: string;
  model?: string | null;
  intent?: string | null;
  maxSuggestions?: number;
  timeout?: number;
}

interface PySchemaColumn {
  name: string;
  data_type: string | null;
  nullable: boolean | null;
}

interface PyNextNodeSuggestion {
  node_type: string;
  settings: Record<string, unknown>;
  label: string;
  description: string | null;
  predicted_output_schema: PySchemaColumn[] | null;
  rationale: string | null;
}

interface PyNextNodeSuggestionsResponse {
  suggestions: PyNextNodeSuggestion[];
  degraded: boolean;
  reason: string | null;
}

const fromPySchemaColumn = (raw: PySchemaColumn): SchemaColumn => ({
  name: raw.name,
  dataType: raw.data_type,
  nullable: raw.nullable,
});

const fromPyNextNodeSuggestion = (raw: PyNextNodeSuggestion): NextNodeSuggestion => ({
  nodeType: raw.node_type,
  settings: raw.settings,
  label: raw.label,
  description: raw.description,
  predictedOutputSchema: raw.predicted_output_schema
    ? raw.predicted_output_schema.map(fromPySchemaColumn)
    : null,
  rationale: raw.rationale,
});

export const fetchNextNodeSuggestions = async (
  body: SuggestNextNodeRequest,
  signal?: AbortSignal,
): Promise<NextNodeSuggestionsResponse> => {
  const payload: Record<string, unknown> = {
    flow_id: body.flowId,
    upstream_node_id: body.upstreamNodeId,
  };
  if (body.provider !== undefined) payload.provider = body.provider;
  if (body.model !== undefined && body.model !== null) payload.model = body.model;
  if (body.intent !== undefined && body.intent !== null) payload.intent = body.intent;
  if (body.maxSuggestions !== undefined) payload.max_suggestions = body.maxSuggestions;
  if (body.timeout !== undefined) payload.timeout = body.timeout;

  try {
    const response = await axios.post<PyNextNodeSuggestionsResponse>(
      "/ai/suggest_next_node",
      payload,
      { signal },
    );
    return {
      suggestions: response.data.suggestions.map(fromPyNextNodeSuggestion),
      degraded: response.data.degraded,
      reason: response.data.reason,
    };
  } catch (error) {
    if (isAiDisabledError(error)) throw new AiDisabledError();
    throw error;
  }
};

// --------------------------------------------------------------------------
// Cmd+K command palette (W33) — non-streaming JSON wrapper around
// /ai/command_palette. Returns the staged GraphDiff in the same shape
// W35's `useAiDiffStore.setCurrentDiff(...)` expects, so the frontend
// composes the existing diff panel without a follow-up GET.
// --------------------------------------------------------------------------

import type { GraphDiffPayload } from "../features/ai/aiDiffTypes";

export interface CommandPaletteInsertionContext {
  upstreamNodeIds: number[];
  rightInputNodeId?: number | null;
  posX?: number;
  posY?: number;
}

export interface CommandPaletteRequest {
  flowId: number;
  prompt: string;
  provider: string;
  model?: string | null;
  selectedNodeIds?: number[];
  insertionContext?: CommandPaletteInsertionContext | null;
  maxTokens?: number;
  sessionId?: string;
  timeout?: number;
}

export interface CommandPaletteRefusal {
  toolName: string;
  refusalReason: string | null;
  refusalDetail: string | null;
  warnings: string[];
}

export type CommandPaletteDegradedReason =
  | "timeout"
  | "no_tool_calls"
  | "provider_error"
  | "all_refused"
  | "empty_catalog";

export interface CommandPaletteResponse {
  diffId: string | null;
  opCount: number;
  rationale: string | null;
  degraded: boolean;
  reason: CommandPaletteDegradedReason | null;
  diff: GraphDiffPayload | null;
  refused: CommandPaletteRefusal[];
}

interface PyCommandPaletteRefusal {
  tool_name: string;
  refusal_reason: string | null;
  refusal_detail: string | null;
  warnings: string[];
}

interface PyCommandPaletteResponse {
  diff_id: string | null;
  op_count: number;
  rationale: string | null;
  degraded: boolean;
  reason: CommandPaletteDegradedReason | null;
  diff: GraphDiffPayload | null;
  refused: PyCommandPaletteRefusal[];
}

const fromPyRefusal = (raw: PyCommandPaletteRefusal): CommandPaletteRefusal => ({
  toolName: raw.tool_name,
  refusalReason: raw.refusal_reason,
  refusalDetail: raw.refusal_detail,
  warnings: raw.warnings ?? [],
});

export const submitCommandPalette = async (
  body: CommandPaletteRequest,
  signal?: AbortSignal,
): Promise<CommandPaletteResponse> => {
  const payload: Record<string, unknown> = {
    flow_id: body.flowId,
    prompt: body.prompt,
    provider: body.provider,
  };
  if (body.model !== undefined && body.model !== null) payload.model = body.model;
  if (body.selectedNodeIds !== undefined) payload.selected_node_ids = body.selectedNodeIds;
  if (body.insertionContext !== undefined && body.insertionContext !== null) {
    payload.insertion_context = {
      upstream_node_ids: body.insertionContext.upstreamNodeIds,
      right_input_node_id: body.insertionContext.rightInputNodeId ?? null,
      pos_x: body.insertionContext.posX ?? 0.0,
      pos_y: body.insertionContext.posY ?? 0.0,
    };
  }
  if (body.maxTokens !== undefined) payload.max_tokens = body.maxTokens;
  if (body.sessionId !== undefined) payload.session_id = body.sessionId;
  if (body.timeout !== undefined) payload.timeout = body.timeout;

  try {
    const response = await axios.post<PyCommandPaletteResponse>("/ai/command_palette", payload, {
      signal,
    });
    const data = response.data;
    return {
      diffId: data.diff_id,
      opCount: data.op_count,
      rationale: data.rationale,
      degraded: data.degraded,
      reason: data.reason,
      diff: data.diff,
      refused: (data.refused ?? []).map(fromPyRefusal),
    };
  } catch (error) {
    if (isAiDisabledError(error)) throw new AiDisabledError();
    throw error;
  }
};

// --------------------------------------------------------------------------
// Multi-turn planner agent (W40) — non-streaming sibling endpoints. The SSE
// start + resume-continue paths live in services/aiStreamClient.ts; this file
// owns the JSON-only abort / discard-resume / status-snapshot fetches.
// --------------------------------------------------------------------------

export interface AgentDriftDetail {
  missingNodeIds: number[];
  mutatedNodeIds: number[];
  schemaChangedNodeIds: number[];
}

export interface AgentSessionState {
  sessionId: string;
  flowId: number;
  status: "running" | "paused_drift" | "awaiting_user" | "completed" | "aborted" | "failed";
  surface: "agent" | "agent_complex";
  samplesMode: "off" | "regex";
  stepCount: number;
  maxSteps: number;
  stagedCount: number;
  diffId: string | null;
  rationale: string | null;
  pauseReason: string | null;
  driftDetail: AgentDriftDetail | null;
  createdAt: string;
  updatedAt: string;
}

export interface AgentAbortResponse {
  status: "aborted";
  sessionId: string;
  partialDiffId: string | null;
}

export interface AgentDiscardResponse {
  status: "discarded";
  sessionId: string;
}

interface PyAgentDriftDetail {
  missing_node_ids: number[];
  mutated_node_ids: number[];
  schema_changed_node_ids: number[];
}

interface PyAgentSessionState {
  session_id: string;
  flow_id: number;
  status: AgentSessionState["status"];
  surface: AgentSessionState["surface"];
  samples_mode: AgentSessionState["samplesMode"];
  step_count: number;
  max_steps: number;
  staged_count: number;
  diff_id: string | null;
  rationale: string | null;
  pause_reason: string | null;
  drift_detail: PyAgentDriftDetail | null;
  created_at: string;
  updated_at: string;
}

const fromPyDriftDetail = (raw: PyAgentDriftDetail): AgentDriftDetail => ({
  missingNodeIds: raw.missing_node_ids,
  mutatedNodeIds: raw.mutated_node_ids,
  schemaChangedNodeIds: raw.schema_changed_node_ids,
});

export const getAgentSession = async (
  sessionId: string,
  signal?: AbortSignal,
): Promise<AgentSessionState> => {
  try {
    const response = await axios.get<PyAgentSessionState>(
      `/ai/agent/${encodeURIComponent(sessionId)}`,
      { signal },
    );
    const data = response.data;
    return {
      sessionId: data.session_id,
      flowId: data.flow_id,
      status: data.status,
      surface: data.surface,
      samplesMode: data.samples_mode,
      stepCount: data.step_count,
      maxSteps: data.max_steps,
      stagedCount: data.staged_count,
      diffId: data.diff_id,
      rationale: data.rationale,
      pauseReason: data.pause_reason,
      driftDetail: data.drift_detail ? fromPyDriftDetail(data.drift_detail) : null,
      createdAt: data.created_at,
      updatedAt: data.updated_at,
    };
  } catch (error) {
    if (isAiDisabledError(error)) throw new AiDisabledError();
    throw error;
  }
};

export const abortAgentSession = async (
  sessionId: string,
  signal?: AbortSignal,
): Promise<AgentAbortResponse> => {
  try {
    const response = await axios.post<{
      status: "aborted";
      session_id: string;
      partial_diff_id: string | null;
    }>(`/ai/agent/${encodeURIComponent(sessionId)}/abort`, {}, { signal });
    return {
      status: response.data.status,
      sessionId: response.data.session_id,
      partialDiffId: response.data.partial_diff_id,
    };
  } catch (error) {
    if (isAiDisabledError(error)) throw new AiDisabledError();
    throw error;
  }
};

export const discardAgentSession = async (
  sessionId: string,
  signal?: AbortSignal,
): Promise<AgentDiscardResponse> => {
  try {
    const response = await axios.post<{ status: "discarded"; session_id: string }>(
      `/ai/agent/${encodeURIComponent(sessionId)}/resume`,
      { action: "discard" },
      { signal },
    );
    return {
      status: response.data.status,
      sessionId: response.data.session_id,
    };
  } catch (error) {
    if (isAiDisabledError(error)) throw new AiDisabledError();
    throw error;
  }
};
