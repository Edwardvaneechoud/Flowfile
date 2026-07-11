// Typed axios wrappers for the community-node registry endpoints. Uses the
// global axios instance (baseURL + auth interceptors live in
// services/axios.config.ts). Paths carry NO trailing slash — they must match the
// FastAPI routes exactly or a 307 redirect drops the body in Docker.
import axios from "axios";

const BASE = "/community_nodes";

export type InstallState =
  | "not_installed"
  | "installed"
  | "update_available"
  | "modified_locally"
  | "incompatible";

export interface CommunityAuthor {
  github: string;
  name: string;
  url: string;
}

export interface PinnedFile {
  path: string;
  sha256: string;
  size: number;
}

export interface CommunityArtifacts {
  node: PinnedFile;
  manifest: PinnedFile;
  icon: PinnedFile | null;
  readme: PinnedFile | null;
  screenshots: PinnedFile[];
}

export interface CommunityPopularity {
  thumbs_up: number;
  discussion_url: string;
}

// Mirrors CommunityIndexEntry (models.py) + the per-node popularity / install_state
// the /index route layers on top.
export interface CommunityNodeSummary {
  id: string;
  node_name: string;
  version: string;
  description: string;
  author: CommunityAuthor;
  maintainers: string[];
  license: string;
  category: string;
  palette_category: string;
  tags: string[];
  environment: "local" | "kernel";
  dependencies: string[];
  number_of_inputs: number;
  number_of_outputs: number;
  min_flowfile_version: string;
  designer_editable: boolean;
  risk_flags: string[];
  published_at: string;
  updated_at: string;
  changelog: string;
  path: string;
  artifacts: CommunityArtifacts;
  popularity: CommunityPopularity | null;
  install_state: InstallState;
}

export interface CommunityNodeDetail extends CommunityNodeSummary {
  readme_text: string | null;
  icon_file: string | null;
  screenshots: string[]; // media file names (not PinnedFile)
}

export interface CommunityIndexResponse {
  fetched_at: string;
  source: string;
  stale: boolean;
  categories: string[];
  repo_stars: number;
  nodes: CommunityNodeSummary[];
}

export interface ConsentRecord {
  acknowledged: boolean;
  at: string;
  by: string;
  capabilities: string[];
}

export interface InstallReceipt {
  node_id: string;
  node_key: string;
  file_name: string;
  version: string;
  sha256: string;
  icon_file: string | null;
  installed_at: string;
  installed_by: string;
  consent: ConsentRecord;
  source_commit: string;
  index_url: string;
  min_flowfile_version: string;
}

export interface InstalledCommunityNode {
  receipt: InstallReceipt;
  node_name: string;
  modified_locally: boolean;
  error: string | null;
}

export interface UpdateInfo {
  node_id: string;
  installed_version: string;
  latest_version: string;
  sha256_changed: boolean;
  changelog: string;
}

export interface InstallRequestBody {
  node_id: string;
  version: string;
  acknowledged_risks: boolean;
  acknowledged_capabilities: string[];
}

export interface InstallResponse {
  success: boolean;
  node: Record<string, unknown>;
  receipt: InstallReceipt;
  load_error: string | null;
}

// Error codes the router returns in `detail.error_code`.
export type CommunityErrorCode =
  | "CONSENT_REQUIRED"
  | "CONSENT_CAPABILITIES"
  | "NODE_NAME_COLLISION"
  | "BLOCKED"
  | "PIN_MISMATCH"
  | "COMMUNITY_UNAVAILABLE"
  | "SCAN_REJECTED"
  | "NODE_NOT_FOUND"
  | "MEDIA_NOT_FOUND"
  | "UNKNOWN";

/** Normalised registry error — the router returns a structured `detail` object
 *  with an `error_code`; this flattens it (plus the loose fields some codes add:
 *  holder_file, missing, rule_ids) into one typed surface for the store. */
export class CommunityApiError extends Error {
  readonly status: number;
  readonly errorCode: CommunityErrorCode;
  readonly holderFile?: string;
  readonly missing?: string[];
  readonly ruleIds?: string[];
  readonly nodeId?: string;

  constructor(
    status: number,
    errorCode: CommunityErrorCode,
    message: string,
    extra?: { holderFile?: string; missing?: string[]; ruleIds?: string[]; nodeId?: string },
  ) {
    super(message);
    this.name = "CommunityApiError";
    this.status = status;
    this.errorCode = errorCode;
    this.holderFile = extra?.holderFile;
    this.missing = extra?.missing;
    this.ruleIds = extra?.ruleIds;
    this.nodeId = extra?.nodeId;
  }
}

interface StructuredDetail {
  error_code?: CommunityErrorCode;
  message?: string;
  holder_file?: string;
  missing?: string[];
  rule_ids?: string[];
  node_id?: string;
}

export function toCommunityError(e: unknown): CommunityApiError {
  const err = e as {
    response?: { status?: number; data?: { detail?: StructuredDetail | string } };
  };
  const status = err?.response?.status ?? 0;
  const detail = err?.response?.data?.detail;
  if (detail && typeof detail === "object") {
    return new CommunityApiError(
      status,
      detail.error_code ?? "UNKNOWN",
      detail.message ?? "Community registry request failed",
      {
        holderFile: detail.holder_file,
        missing: detail.missing,
        ruleIds: detail.rule_ids,
        nodeId: detail.node_id,
      },
    );
  }
  const message = typeof detail === "string" ? detail : "Community registry request failed";
  return new CommunityApiError(status, "UNKNOWN", message);
}

async function wrap<T>(p: Promise<{ data: T }>): Promise<T> {
  try {
    return (await p).data;
  } catch (e) {
    throw toCommunityError(e);
  }
}

export function fetchIndex(refresh = false): Promise<CommunityIndexResponse> {
  return wrap(
    axios.get<CommunityIndexResponse>(`${BASE}/index`, { params: { refresh } }),
  );
}

export function fetchNode(nodeId: string): Promise<CommunityNodeDetail> {
  return wrap(axios.get<CommunityNodeDetail>(`${BASE}/node/${nodeId}`));
}

export function installNode(body: InstallRequestBody): Promise<InstallResponse> {
  return wrap(axios.post<InstallResponse>(`${BASE}/install`, body));
}

export function uninstallNode(nodeId: string): Promise<{ success: boolean; node_id: string }> {
  return wrap(axios.delete<{ success: boolean; node_id: string }>(`${BASE}/uninstall/${nodeId}`));
}

export function fetchInstalled(): Promise<InstalledCommunityNode[]> {
  return wrap(axios.get<InstalledCommunityNode[]>(`${BASE}/installed`));
}

export function fetchUpdates(): Promise<UpdateInfo[]> {
  return wrap(axios.get<UpdateInfo[]>(`${BASE}/updates`));
}

/** Basename of a repo-relative artifact path (screenshots/foo.png -> foo.png). */
export function mediaFileName(path: string): string {
  const parts = path.split("/");
  return parts[parts.length - 1] ?? path;
}
