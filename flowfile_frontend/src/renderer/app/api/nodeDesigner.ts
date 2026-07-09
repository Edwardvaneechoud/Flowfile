// Typed axios wrappers for the custom-node designer endpoints. Uses the global
// axios instance (baseURL + auth interceptors are configured on it in
// services/axios.config.ts). Paths are hyphenated with NO trailing slash — they
// must match the FastAPI routes exactly or a 307 redirect drops the body in Docker.
import axios, { AxiosError } from "axios";
import type { DesignerState, ParseResult } from "../pages/nodeDesigner/designerState";

const BASE = "/user_defined_components";

/** Existing NodeInfo row shape returned by list/save. */
export interface CustomNodeInfoResponse {
  file_name: string;
  node_name?: string;
  node_category?: string;
  title?: string;
  intro?: string;
  node_icon?: string;
  node_key?: string;
  environment?: "local" | "kernel";
  source_hash?: string;
  error?: string | null;
}

export type SaveMode = "designer" | "code";

export interface SaveCustomNodeBody {
  file_name: string;
  mode?: SaveMode;
  designer_state?: DesignerState | null;
  code?: string | null;
  expected_hash?: string | null;
}

export interface SaveCustomNodeResponse {
  success: boolean;
  file_name: string;
  node: CustomNodeInfoResponse;
  parse_result?: ParseResult | null;
  code?: string;
  file_hash?: string;
  load_error: string | null;
}

export interface GetCustomNodeResponse {
  file_name: string;
  code: string;
  parse_result: ParseResult;
  file_hash: string;
  // Legacy fields kept for back-compat during the transition.
  content?: string;
  metadata?: Record<string, unknown>;
  sections?: unknown[];
  processCode?: string;
  designer_state?: DesignerState | null;
  supports_visual_edit?: boolean;
}

export type DryRunErrorKind =
  | "syntax"
  | "load"
  | "settings"
  | "execution"
  | "timeout"
  | "no_sample_data"
  | null;

export interface DryRunColumn {
  name: string;
  data_type: string;
}

export interface DryRunOutput {
  name: string;
  columns: DryRunColumn[];
  rows: unknown[][] | Record<string, unknown>[];
  row_count: number;
  truncated: boolean;
}

export interface DryRunBody {
  designer_state?: DesignerState | null;
  code?: string | null;
  settings_values: Record<string, Record<string, unknown>>;
  sample_inputs: Array<Record<string, unknown[]>> | null;
  row_limit?: number;
  timeout_seconds?: number;
  kernel_id?: string | null; // run on this Docker kernel instead of the worker (kernel-env nodes)
}

export interface DryRunResponse {
  success: boolean;
  outputs: DryRunOutput[];
  logs: string[];
  error: string | null;
  error_kind: DryRunErrorKind;
  traceback: string | null;
  duration_ms: number;
  executed_in: "worker" | "in_core" | "kernel";
}

/** Raised by save() when the on-disk file hash no longer matches expected_hash. */
export class SaveConflictError extends Error {
  constructor(public readonly detail: unknown) {
    super("Custom node file changed on disk");
    this.name = "SaveConflictError";
  }
}

export async function listCustomNodes(): Promise<CustomNodeInfoResponse[]> {
  const response = await axios.get<CustomNodeInfoResponse[]>(`${BASE}/list-custom-nodes`);
  return response.data;
}

export async function getCustomNode(fileName: string): Promise<GetCustomNodeResponse> {
  const response = await axios.get<GetCustomNodeResponse>(
    `${BASE}/get-custom-node/${encodeURIComponent(fileName)}`,
  );
  return response.data;
}

export async function saveCustomNode(body: SaveCustomNodeBody): Promise<SaveCustomNodeResponse> {
  try {
    const response = await axios.post<SaveCustomNodeResponse>(`${BASE}/save-custom-node`, body);
    return response.data;
  } catch (err) {
    const axiosErr = err as AxiosError;
    if (axiosErr.response?.status === 409) {
      throw new SaveConflictError(axiosErr.response.data);
    }
    throw err;
  }
}

export async function previewCustomNode(designerState: DesignerState): Promise<string> {
  const response = await axios.post<{ code: string }>(`${BASE}/preview-custom-node`, {
    designer_state: designerState,
  });
  return response.data.code;
}

export async function dryRunCustomNode(body: DryRunBody): Promise<DryRunResponse> {
  const response = await axios.post<DryRunResponse>(`${BASE}/dry-run`, body);
  return response.data;
}

export async function deleteCustomNode(fileName: string): Promise<void> {
  await axios.delete(`${BASE}/delete-custom-node/${encodeURIComponent(fileName)}`);
}

export interface RescanResponse {
  total: number;
  loaded: number;
  broken: Array<{ file_name: string; error: string }>;
}

export async function rescanCustomNodes(): Promise<RescanResponse> {
  const response = await axios.post<RescanResponse>(`${BASE}/rescan`);
  return response.data;
}

// ---- Custom-node mounts (extra directories) + combined catalog listing ----
// Router is mounted at /custom-node-mounts with NO trailing slash; the empty
// path collection endpoints are exactly "/custom-node-mounts".
const MOUNTS_BASE = "/custom-node-mounts";

export interface MountInfo {
  path: string;
  added_at: string;
  exists: boolean;
  node_count: number;
  error_count: number;
}

export interface MountMutationResponse {
  path: string;
  total_nodes: number;
  mounts: MountInfo[];
}

export interface CatalogCustomNode {
  node_key: string;
  node_name: string;
  node_category: string;
  file_name: string;
  environment: "local" | "kernel";
  error: string | null;
  source: string; // "default" | absolute mount path
  source_label: string; // "Default" | mount dir basename
}

export async function listCustomNodeMounts(): Promise<MountInfo[]> {
  const response = await axios.get<MountInfo[]>(MOUNTS_BASE);
  return response.data;
}

export async function addCustomNodeMount(path: string): Promise<MountMutationResponse> {
  const response = await axios.post<MountMutationResponse>(MOUNTS_BASE, { path });
  return response.data;
}

export async function removeCustomNodeMount(path: string): Promise<MountMutationResponse> {
  const response = await axios.delete<MountMutationResponse>(MOUNTS_BASE, {
    params: { path },
  });
  return response.data;
}

export async function listCatalogCustomNodes(): Promise<CatalogCustomNode[]> {
  const response = await axios.get<CatalogCustomNode[]>(`${MOUNTS_BASE}/nodes`);
  return response.data;
}
