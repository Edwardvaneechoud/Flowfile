// Axios + SSE wrappers for the on-demand local-model endpoints
// (/ai/local-model/*). Mirrors AiProvidersView/api.ts: camelCase TS,
// snake_case mappers at the boundary. Install streams progress as SSE over
// POST, so it uses fetch + the shared bearer token (native EventSource is
// GET-only) — the same pattern as services/aiStreamClient.ts.

import { flowfileCorebaseURL } from "../../../config/constants";
import authService from "../../services/auth.service";
import axios from "../../services/axios.config";
import { AI_DISABLED_DETAIL, AiDisabledError } from "./api";

export interface LocalModelStatus {
  available: boolean;
  installed: boolean;
  binaryInstalled: boolean;
  modelInstalled: boolean;
  running: boolean;
  modelName: string;
  modelFile: string;
  approxDownloadMb: number;
  installDir: string;
}

interface PyLocalModelStatus {
  available: boolean;
  installed: boolean;
  binary_installed: boolean;
  model_installed: boolean;
  running: boolean;
  model_name: string;
  model_file: string;
  approx_download_mb: number;
  install_dir: string;
}

const fromPyStatus = (raw: PyLocalModelStatus): LocalModelStatus => ({
  available: raw.available,
  installed: raw.installed,
  binaryInstalled: raw.binary_installed,
  modelInstalled: raw.model_installed,
  running: raw.running,
  modelName: raw.model_name,
  modelFile: raw.model_file,
  approxDownloadMb: raw.approx_download_mb,
  installDir: raw.install_dir,
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

export const deleteLocalModel = async (): Promise<LocalModelStatus> => {
  const response = await axios.delete<PyLocalModelStatus>(LOCAL_BASE);
  return fromPyStatus(response.data);
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
): Promise<void> => {
  const token = await authService.getToken();
  if (!token) {
    handlers.onError?.("Not authenticated. Please log in again.");
    return;
  }
  const url = new URL("ai/local-model/install", flowfileCorebaseURL).toString();
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
