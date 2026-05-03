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
