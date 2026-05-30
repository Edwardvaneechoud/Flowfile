// Axios + SSE wrappers for the on-demand local-model endpoints
// (/ai/local-model/*). Mirrors AiProvidersView/api.ts: camelCase TS,
// snake_case mappers at the boundary. Install streams progress as SSE over
// POST, so it uses fetch + the shared bearer token (native EventSource is
// GET-only) — the same pattern as services/aiStreamClient.ts.

import { flowfileCorebaseURL } from "../../../config/constants";
import authService from "../../services/auth.service";
import axios from "../../services/axios.config";
import { AI_DISABLED_DETAIL, AiDisabledError } from "./api";

// One installable model in the catalog (mirrors manager.MODELS + status()).
export interface LocalModelEntry {
  id: string;
  name: string;
  approxDownloadMb: number;
  description: string;
  installed: boolean;
}

export interface LocalModelStatus {
  available: boolean;
  binaryInstalled: boolean;
  // ``installed`` = binary + the *selected* model present (back-compat flag the
  // chat-drawer provider injection reads to decide whether to offer "local").
  installed: boolean;
  modelInstalled: boolean;
  running: boolean;
  runningModelId: string | null;
  selectedModelId: string;
  modelName: string;
  approxDownloadMb: number;
  anyModelInstalled: boolean;
  models: LocalModelEntry[];
  installDir: string;
  // Context window (tokens) the next server boot uses, plus the allowed range.
  ctxSize: number;
  ctxSizeMin: number;
  ctxSizeMax: number;
}

interface PyLocalModelEntry {
  id: string;
  name: string;
  approx_download_mb: number;
  description: string;
  installed: boolean;
}

interface PyLocalModelStatus {
  available: boolean;
  binary_installed: boolean;
  installed: boolean;
  model_installed: boolean;
  running: boolean;
  running_model_id: string | null;
  selected_model_id: string;
  model_name: string;
  approx_download_mb: number;
  any_model_installed: boolean;
  models: PyLocalModelEntry[];
  install_dir: string;
  ctx_size: number;
  ctx_size_min: number;
  ctx_size_max: number;
}

const fromPyStatus = (raw: PyLocalModelStatus): LocalModelStatus => ({
  available: raw.available,
  binaryInstalled: raw.binary_installed,
  installed: raw.installed,
  modelInstalled: raw.model_installed,
  running: raw.running,
  runningModelId: raw.running_model_id,
  selectedModelId: raw.selected_model_id,
  modelName: raw.model_name,
  approxDownloadMb: raw.approx_download_mb,
  anyModelInstalled: raw.any_model_installed,
  models: (raw.models ?? []).map((m) => ({
    id: m.id,
    name: m.name,
    approxDownloadMb: m.approx_download_mb,
    description: m.description,
    installed: m.installed,
  })),
  installDir: raw.install_dir,
  ctxSize: raw.ctx_size,
  ctxSizeMin: raw.ctx_size_min,
  ctxSizeMax: raw.ctx_size_max,
});

const LOCAL_BASE = "/ai/local-model";

const isAiDisabled = (error: unknown): boolean => {
  const detail = (error as { response?: { data?: { detail?: unknown } } })?.response?.data?.detail;
  const status = (error as { response?: { status?: number } })?.response?.status;
  return status === 503 && detail === AI_DISABLED_DETAIL;
};

export const fetchLocalModelStatus = async (): Promise<LocalModelStatus> => {
  try {
    const response = await axios.get<PyLocalModelStatus>(`${LOCAL_BASE}/status`);
    return fromPyStatus(response.data);
  } catch (error) {
    if (isAiDisabled(error)) throw new AiDisabledError();
    throw error;
  }
};

export const startLocalModel = async (): Promise<LocalModelStatus> => {
  const response = await axios.post<PyLocalModelStatus>(`${LOCAL_BASE}/start`);
  return fromPyStatus(response.data);
};

export const stopLocalModel = async (): Promise<LocalModelStatus> => {
  const response = await axios.post<PyLocalModelStatus>(`${LOCAL_BASE}/stop`);
  return fromPyStatus(response.data);
};

// Delete one model's GGUF (modelId) or, when omitted, the whole runtime
// (binary + every model).
export const deleteLocalModel = async (modelId?: string): Promise<LocalModelStatus> => {
  const response = await axios.delete<PyLocalModelStatus>(LOCAL_BASE, {
    params: modelId ? { model_id: modelId } : undefined,
  });
  return fromPyStatus(response.data);
};

// Make an already-installed model the active one (recycles a running server).
export const selectLocalModel = async (modelId: string): Promise<LocalModelStatus> => {
  const response = await axios.post<PyLocalModelStatus>(`${LOCAL_BASE}/select`, {
    model_id: modelId,
  });
  return fromPyStatus(response.data);
};

// Set the context-window size (tokens). Backend clamps to its range and stops
// any running server so the next use boots with the new size.
export const setLocalCtxSize = async (ctxSize: number): Promise<LocalModelStatus> => {
  const response = await axios.post<PyLocalModelStatus>(`${LOCAL_BASE}/ctx-size`, {
    ctx_size: ctxSize,
  });
  return fromPyStatus(response.data);
};

// Stable provider id used to surface the local model in the chat drawer's
// provider picker. NOT a BYOK provider (it isn't in the backend PROVIDERS
// map) — the store special-cases it to route chat / generate through the
// /ai/local-model/* endpoints instead of the BYOK chat path.
export const LOCAL_PROVIDER_ID = "local";

// User-facing display name for the local provider. The wire id stays "local";
// only the label users see is "On-device AI".
export const LOCAL_PROVIDER_LABEL = "On-device AI";

// NOTE: local chat is NOT a separate path. The backend exposes "local" as a
// resolvable provider on /ai/chat/stream (and every read-only surface), so the
// frontend drives it through the shared streamChat client — that's what gives
// it the flow context (subgraph + schemas) cloud providers get.

export interface GenerateFlowResult {
  diffId: string;
  opCount: number;
  created: Array<{ id: string; type: string; node_id: number }>;
  warnings: string[];
  rationale: string;
  // Full GraphDiff (snake_case wire shape) for the diff-review panel.
  diffPayload: Record<string, unknown> | null;
}

interface PyGenerateFlowResult {
  diff_id: string;
  op_count: number;
  created: Array<{ id: string; type: string; node_id: number }>;
  warnings: string[];
  rationale: string;
  diff_payload: Record<string, unknown> | null;
}

// POST /ai/generate — provider-agnostic whole-flow generation (the "Simple
// build" surface). Works for any provider (local or cloud). ``mode="simple"``
// builds the diff with no validation until apply; ``"one_shot"`` validates each
// node through the executor (for bigger models). Returns the staged diff id +
// the full diff payload so the caller can apply it via the existing diff-accept
// route.
export const generateFlow = async (
  flowId: number,
  userRequest: string,
  provider: string,
  model: string | null = null,
  mode: "simple" | "one_shot" = "simple",
  maxTokens?: number | null,
): Promise<GenerateFlowResult> => {
  const response = await axios.post<PyGenerateFlowResult>("/ai/generate", {
    flow_id: flowId,
    user_request: userRequest,
    provider,
    model,
    mode,
    max_tokens: maxTokens ?? null,
  });
  const raw = response.data;
  return {
    diffId: raw.diff_id,
    opCount: raw.op_count,
    created: raw.created,
    warnings: raw.warnings,
    rationale: raw.rationale,
    diffPayload: raw.diff_payload,
  };
};

export interface LocalInstallProgress {
  phase: string;
  received?: number;
  total?: number | null;
  message?: string;
  path?: string;
}

export interface LocalInstallHandlers {
  onProgress?: (ev: LocalInstallProgress) => void;
  onError?: (message: string) => void;
}

// POST /ai/local-model/install streams `event: progress` blocks plus a
// terminal `event: error` on failure. Resolves when the stream ends; errors
// are surfaced via `onError` (the caller re-checks status afterwards).
export const streamLocalModelInstall = async (
  handlers: LocalInstallHandlers,
  signal?: AbortSignal,
  modelId?: string,
): Promise<void> => {
  const token = await authService.getToken();
  if (!token) {
    handlers.onError?.("Not authenticated. Please log in again.");
    return;
  }
  const url = new URL("ai/local-model/install", flowfileCorebaseURL);
  if (modelId) url.searchParams.set("model_id", modelId);
  const response = await fetch(url, {
    method: "POST",
    headers: {
      Accept: "text/event-stream",
      Authorization: `Bearer ${token}`,
    },
    signal,
    credentials: "include",
  });

  if (!response.ok || !response.body) {
    let detail = `HTTP ${response.status}`;
    try {
      const data = await response.json();
      if (data && typeof (data as { detail?: unknown }).detail === "string") {
        detail = (data as { detail: string }).detail;
      }
    } catch {
      /* non-JSON body — keep the status string */
    }
    handlers.onError?.(detail);
    return;
  }

  const reader = response.body.getReader();
  const decoder = new TextDecoder("utf-8");
  let buffer = "";

  const flushBlock = (block: string): void => {
    let event: string | null = null;
    const dataLines: string[] = [];
    for (const line of block.split(/\r?\n/)) {
      if (line.startsWith(":") || line === "") continue;
      if (line.startsWith("event: ")) event = line.slice("event: ".length);
      else if (line.startsWith("data: ")) dataLines.push(line.slice("data: ".length));
    }
    if (!event) return;
    let payload: Record<string, unknown> = {};
    try {
      payload = dataLines.length ? JSON.parse(dataLines.join("\n")) : {};
    } catch {
      return;
    }
    if (event === "progress") {
      handlers.onProgress?.(payload as unknown as LocalInstallProgress);
    } else if (event === "error") {
      handlers.onError?.(typeof payload.message === "string" ? payload.message : "install failed");
    }
  };

  try {
    let streamDone = false;
    while (!streamDone) {
      const { value, done } = await reader.read();
      if (done) {
        streamDone = true;
        break;
      }
      buffer += decoder.decode(value, { stream: true });
      let idx = buffer.search(/\r?\n\r?\n/);
      while (idx !== -1) {
        const block = buffer.slice(0, idx);
        const matched = buffer.slice(idx).match(/^\r?\n\r?\n/);
        buffer = buffer.slice(idx + (matched?.[0]?.length ?? 2));
        flushBlock(block);
        idx = buffer.search(/\r?\n\r?\n/);
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
