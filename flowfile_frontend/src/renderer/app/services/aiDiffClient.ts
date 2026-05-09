// Diff staging client for the AI subsystem.
//
// Three thin POST + JSON wrappers over `/ai/diff/*`. No streaming, no
// reactivity, no Pinia store touches — owns the UI integration. Shares
// the bearer-token + base-URL pattern with `aiStreamClient.ts`.
//
// Wire shapes mirror `flowfile_core/ai/diff_routes.py`:
//   - POST /ai/diff/stage              → { diff_id, op_count }
//   - POST /ai/diff/{diff_id}/accept   → { status: "accepted", ... }
//   - POST /ai/diff/{diff_id}/reject   → { status: "rejected", ... }

import { flowfileCorebaseURL } from "../../config/constants";
import authService from "./auth.service";

export interface StagedToolResultBody {
  tool_name: string;
  audit_id?: number | null;
  staged_node_payload?: Record<string, unknown>;
}

export interface StageDiffRequest {
  session_id: string;
  flow_id: number;
  staged_results: StagedToolResultBody[];
  rationale?: string | null;
}

export interface StageDiffResponse {
  diff_id: string;
  op_count: number;
}

export interface AcceptDiffRequest {
  flow_id: number;
}

export interface AcceptDiffResponse {
  status: "accepted";
  diff_id: string;
  applied_node_ids: number[];
  modified_node_ids?: number[];
  applied_connection_count: number;
  removed_node_ids: number[];
  removed_connection_count: number;
  audit_ids_updated: number[];
  history_action: string;
}

export interface RejectDiffResponse {
  status: "rejected";
  diff_id: string;
  audit_ids_updated: number[];
}

export class AiDiffHttpError extends Error {
  constructor(
    public readonly status: number,
    public readonly detail: unknown,
  ) {
    super(typeof detail === "string" ? detail : `HTTP ${status}`);
    this.name = "AiDiffHttpError";
  }
}

const readErrorDetail = async (response: Response): Promise<unknown> => {
  try {
    const data = await response.json();
    if (data && typeof (data as { detail?: unknown }).detail !== "undefined") {
      return (data as { detail: unknown }).detail;
    }
    return data;
  } catch {
    try {
      return await response.text();
    } catch {
      return `HTTP ${response.status}`;
    }
  }
};

const postJson = async <TBody, TResponse>(
  path: string,
  body: TBody | undefined,
  signal?: AbortSignal,
): Promise<TResponse> => {
  const token = await authService.getToken();
  if (!token) {
    throw new AiDiffHttpError(401, "Not authenticated. Please log in again.");
  }

  const url = new URL(path, flowfileCorebaseURL).toString();
  const response = await fetch(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Accept: "application/json",
      Authorization: `Bearer ${token}`,
    },
    body: body !== undefined ? JSON.stringify(body) : undefined,
    signal,
    credentials: "include",
  });

  if (!response.ok) {
    const detail = await readErrorDetail(response);
    throw new AiDiffHttpError(response.status, detail);
  }

  return (await response.json()) as TResponse;
};

export const stageDiff = async (
  body: StageDiffRequest,
  signal?: AbortSignal,
): Promise<StageDiffResponse> =>
  postJson<StageDiffRequest, StageDiffResponse>("ai/diff/stage", body, signal);

export const acceptDiff = async (
  diffId: string,
  body: AcceptDiffRequest,
  signal?: AbortSignal,
): Promise<AcceptDiffResponse> =>
  postJson<AcceptDiffRequest, AcceptDiffResponse>(
    `ai/diff/${encodeURIComponent(diffId)}/accept`,
    body,
    signal,
  );

export const rejectDiff = async (
  diffId: string,
  signal?: AbortSignal,
): Promise<RejectDiffResponse> =>
  postJson<undefined, RejectDiffResponse>(
    `ai/diff/${encodeURIComponent(diffId)}/reject`,
    undefined,
    signal,
  );
