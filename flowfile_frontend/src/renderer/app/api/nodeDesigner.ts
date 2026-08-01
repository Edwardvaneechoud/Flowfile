// Typed axios wrappers for the custom-node designer endpoints. Uses the global
// axios instance (baseURL + auth interceptors are configured on it in
// services/axios.config.ts). Paths are hyphenated with NO trailing slash — they
// must match the FastAPI routes exactly or a 307 redirect drops the body in Docker.
import axios, { AxiosError } from "axios";
import type { DesignerState, ParseResult } from "../pages/nodeDesigner/designerState";
import type { RawDataInput } from "../pages/nodeDesigner/sampleData";

const BASE = "/user_defined_components";

/** Existing NodeInfo row shape returned by list/save. */
export interface CustomNodeInfoResponse {
  file_name: string;
  node_name?: string;
  node_category?: string;
  title?: string;
  intro?: string;
  author?: string;
  version?: string;
  tags?: string[];
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
  // Legacy alias for `code`; pre-designer-rewrite callers read `content`.
  content?: string;
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
  sample_inputs: RawDataInput[] | null;
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
  title: string;
  intro: string;
  tags: string[];
  node_icon: string;
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

// ---- Publish bundle + publish-prep screenshots ----
// Router mounted at /community_nodes with NO trailing slash.
const COMMUNITY_BASE = "/community_nodes";

export type PublishSeverity = "error" | "warning";

export interface PublishIssue {
  code: string;
  message: string;
  severity: PublishSeverity;
}

export interface PublishBundleBody {
  file_name: string;
  license: string;
  repository?: string;
  description?: string;
  category?: string;
  readme?: string;
  changelog?: string;
}

export interface PublishCompletenessResponse {
  ok: boolean;
  issues: PublishIssue[];
  node_id?: string;
  version?: string;
  published_version?: string | null;
}

export interface PublishScreenshot {
  file_name: string;
}

/** Raised by publishBundle() when the backend 422s with the INCOMPLETE issue list. */
export class PublishIncompleteError extends Error {
  constructor(public readonly issues: PublishIssue[]) {
    super("Node is not ready to publish");
    this.name = "PublishIncompleteError";
  }
}

async function issuesFromErrorBody(data: unknown): Promise<PublishIssue[]> {
  try {
    // responseType "blob" means a 422 error body arrives as a Blob, not parsed JSON.
    let parsed: unknown = data;
    if (data instanceof Blob) parsed = JSON.parse(await data.text());
    else if (typeof data === "string") parsed = JSON.parse(data);
    const detail = (parsed as { detail?: { issues?: PublishIssue[] } })?.detail;
    return detail?.issues ?? [];
  } catch {
    return [];
  }
}

/** Dry check_only POST: the completeness report (errors + warnings) as JSON. */
export async function checkPublishCompleteness(
  body: PublishBundleBody,
): Promise<PublishCompletenessResponse> {
  const response = await axios.post<PublishCompletenessResponse>(
    `${COMMUNITY_BASE}/publish-bundle`,
    { ...body, check_only: true },
  );
  return response.data;
}

export async function publishBundle(body: PublishBundleBody): Promise<Blob> {
  try {
    const response = await axios.post(`${COMMUNITY_BASE}/publish-bundle`, body, {
      responseType: "blob",
    });
    return response.data as Blob;
  } catch (err) {
    const axiosErr = err as AxiosError;
    if (axiosErr.response?.status === 422) {
      throw new PublishIncompleteError(await issuesFromErrorBody(axiosErr.response.data));
    }
    throw err;
  }
}

export async function fetchPublishReadme(fileStem: string): Promise<string> {
  const response = await axios.get<{ readme: string }>(
    `${COMMUNITY_BASE}/readme/${encodeURIComponent(fileStem)}`,
  );
  return response.data.readme;
}

export async function savePublishReadme(fileStem: string, readme: string): Promise<void> {
  await axios.put(`${COMMUNITY_BASE}/readme/${encodeURIComponent(fileStem)}`, { readme });
}

export async function listPublishScreenshots(fileStem: string): Promise<PublishScreenshot[]> {
  const response = await axios.get<PublishScreenshot[]>(
    `${COMMUNITY_BASE}/screenshots/${encodeURIComponent(fileStem)}`,
  );
  return response.data;
}

export async function uploadPublishScreenshot(
  fileStem: string,
  file: File,
): Promise<{ file_name: string }> {
  const formData = new FormData();
  formData.append("file", file);
  const response = await axios.post<{ file_name: string }>(
    `${COMMUNITY_BASE}/screenshots/${encodeURIComponent(fileStem)}`,
    formData,
    { headers: { "Content-Type": "multipart/form-data" } },
  );
  return response.data;
}

export async function deletePublishScreenshot(fileStem: string, fileName: string): Promise<void> {
  await axios.delete(
    `${COMMUNITY_BASE}/screenshots/${encodeURIComponent(fileStem)}/${encodeURIComponent(fileName)}`,
  );
}

/** JWT-gated screenshot bytes as an object URL (raw <img src> would 401). */
export async function fetchPublishScreenshotUrl(
  fileStem: string,
  fileName: string,
): Promise<string | null> {
  try {
    const response = await axios.get(
      `${COMMUNITY_BASE}/screenshots/${encodeURIComponent(fileStem)}/${encodeURIComponent(fileName)}`,
      { responseType: "blob" },
    );
    return URL.createObjectURL(response.data as Blob);
  } catch {
    return null;
  }
}

// ---- In-app GitHub publishing (device flow + PAT + PR) ----
// Router mounted at /community_nodes/github with NO trailing slash. GitHub-domain
// failures never return 401 (that would trip the JWT-refresh interceptor); they
// carry a typed detail.error_code we surface as PublishPrBlockedError.
const GITHUB_BASE = `${COMMUNITY_BASE}/github`;

export interface GithubConnectStatus {
  configured: boolean;
  connected: boolean;
  login: string;
}

export interface DeviceStartResponse {
  device_code: string;
  user_code: string;
  verification_uri: string;
  interval: number;
  expires_in: number;
}

export interface DevicePollResponse {
  status: "pending" | "slow_down" | "connected";
  interval?: number;
  login?: string;
}

export interface GithubPatResponse {
  login: string;
  scopes_warning: string;
}

export interface PublishPrBody {
  file_name: string;
  license: string;
  repository?: string;
  description?: string;
  category?: string;
  readme?: string;
  changelog?: string;
}

export interface OpenPublishPr {
  number: number;
  url: string;
  branch: string;
  version: string;
  title: string;
}

export interface PublishPrResponse {
  // "already_open" kept for old-core skew; current cores report "updated".
  status: "created" | "updated" | "already_open" | "fork_creating";
  pr_url: string;
  pr_number: number | null;
  branch: string;
  node_id: string;
  version: string;
  is_update: boolean;
}

/** A GitHub-domain publish failure carrying the backend's typed error_code. */
export class PublishPrBlockedError extends Error {
  constructor(
    public readonly code: string,
    message: string,
  ) {
    super(message || code);
    this.name = "PublishPrBlockedError";
  }
}

const GITHUB_BLOCKED_STATUSES = [400, 409, 410, 412, 502, 503];

// Turn a typed GitHub-domain error body into PublishPrBlockedError; other errors
// (network, unexpected) pass through unchanged for generic handling.
function asGithubError(err: unknown): unknown {
  const axiosErr = err as AxiosError;
  const status = axiosErr.response?.status;
  if (status && GITHUB_BLOCKED_STATUSES.includes(status)) {
    const detail = (
      axiosErr.response?.data as { detail?: { error_code?: string; message?: string } }
    )?.detail;
    return new PublishPrBlockedError(detail?.error_code ?? `HTTP_${status}`, detail?.message ?? "");
  }
  return err;
}

export async function getGithubStatus(): Promise<GithubConnectStatus> {
  const response = await axios.get<GithubConnectStatus>(`${GITHUB_BASE}/status`);
  return response.data;
}

export async function startGithubDevice(): Promise<DeviceStartResponse> {
  try {
    const response = await axios.post<DeviceStartResponse>(`${GITHUB_BASE}/device/start`);
    return response.data;
  } catch (err) {
    throw asGithubError(err);
  }
}

export async function pollGithubDevice(deviceCode: string): Promise<DevicePollResponse> {
  try {
    const response = await axios.post<DevicePollResponse>(`${GITHUB_BASE}/device/poll`, {
      device_code: deviceCode,
    });
    return response.data;
  } catch (err) {
    throw asGithubError(err);
  }
}

export async function saveGithubPat(token: string): Promise<GithubPatResponse> {
  try {
    const response = await axios.post<GithubPatResponse>(`${GITHUB_BASE}/pat`, { token });
    return response.data;
  } catch (err) {
    throw asGithubError(err);
  }
}

export async function disconnectGithub(): Promise<void> {
  await axios.delete(`${GITHUB_BASE}/token`);
}

export async function publishPr(body: PublishPrBody): Promise<PublishPrResponse> {
  try {
    const response = await axios.post<PublishPrResponse>(`${COMMUNITY_BASE}/publish-pr`, body);
    return response.data;
  } catch (err) {
    const axiosErr = err as AxiosError;
    if (axiosErr.response?.status === 422) {
      throw new PublishIncompleteError(await issuesFromErrorBody(axiosErr.response.data));
    }
    throw asGithubError(err);
  }
}

export async function fetchOpenPublishPrs(fileName: string): Promise<OpenPublishPr[]> {
  try {
    const response = await axios.get<{ open_prs: OpenPublishPr[] }>(
      `${COMMUNITY_BASE}/publish-pr/open`,
      { params: { file_name: fileName } },
    );
    return response.data.open_prs;
  } catch (err) {
    throw asGithubError(err);
  }
}
