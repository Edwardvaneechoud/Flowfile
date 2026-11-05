// Paths match the FastAPI decorators exactly: a trailing slash would answer 307.

import axios from "../services/axios.config";

const BASE = "/hub_client";

export interface HubFlow {
  flowUuid: string;
  name: string;
  /** The newest version published through the hub; null when the flow has none yet. */
  latestVersion: number | null;
  /** Who published the newest version, by name; null when the hub does not say. */
  authorName: string | null;
}

export interface HubFlowVersion {
  version: number;
  authorId: number;
  /** The author's display name; null from a hub that sends the id only. */
  authorName: string | null;
  publishedAt: string;
  message: string | null;
}

/** A node whose settings name a path on this machine, which other users of the hub will not have. */
export interface LocalPathUse {
  label: string;
  path: string;
}

export interface HubPublishInput {
  flowId: number;
  /** Read by the hub on a first publish only; a later publish never moves the flow. */
  namespaceId?: number | null;
  message?: string;
  /** The version to publish over after a stale-base refusal. */
  overwriteVersion?: number;
}

export interface HubPublishResult {
  version: number;
  name: string;
}

export interface HubOpenInput {
  flowUuid: string;
  version?: number;
  /** Overwrite the local copy of this hub flow when one exists. */
  replace?: boolean;
}

export interface HubOpenResult {
  flowPath: string;
}

/** A refusal of publish or open, branched on its `kind`. */
export type HubRefusal =
  | {
      kind: "stale_base";
      flowUuid: string;
      base: HubFlowVersion | null;
      current: HubFlowVersion | null;
    }
  | { kind: "refused"; status: number; detail: string }
  | { kind: "other_hub"; hubUrl: string }
  | { kind: "local_copy_exists"; flowPath: string };

// Core answers more on each (ids of registrations, owners, authors' uuids); the editor reads these.
interface PyFlow {
  flow_uuid: string;
  name: string;
  latest_version: number | null;
  author_name?: string | null;
}

interface PyVersion {
  version: number;
  author_id: number;
  author_name?: string | null;
  published_at: string;
  message: string | null;
}

interface PyLocalPathUse {
  label: string;
  path: string;
}

interface PyPublishOut {
  version: number;
  name: string;
}

interface PyOpenOut {
  flow_path: string;
}

const flowFromPy = (raw: PyFlow): HubFlow => ({
  flowUuid: raw.flow_uuid,
  name: raw.name,
  latestVersion: raw.latest_version,
  authorName: raw.author_name ?? null,
});

const versionFromPy = (raw: PyVersion | null | undefined): HubFlowVersion | null =>
  raw
    ? {
        version: raw.version,
        authorId: raw.author_id,
        authorName: raw.author_name ?? null,
        publishedAt: raw.published_at,
        message: raw.message,
      }
    : null;

export async function listHubFlows(namespaceId?: number | null): Promise<HubFlow[]> {
  const params = namespaceId == null ? undefined : { namespace_id: namespaceId };
  const response = await axios.get<PyFlow[]>(`${BASE}/flows`, { params });
  return response.data.map(flowFromPy);
}

export async function listHubFlowVersions(flowUuid: string): Promise<HubFlowVersion[]> {
  const response = await axios.get<PyVersion[]>(`${BASE}/flows/${flowUuid}/versions`);
  return response.data.map((raw) => versionFromPy(raw) as HubFlowVersion);
}

/** The nodes of the flow that read or write a path on this machine; a warning, never a block. */
export async function publishCheck(flowId: number): Promise<LocalPathUse[]> {
  const response = await axios.get<PyLocalPathUse[]>(`${BASE}/publish_check`, {
    params: { flow_id: flowId },
  });
  return response.data.map((raw) => ({ label: raw.label, path: raw.path }));
}

export async function publishToHub(input: HubPublishInput): Promise<HubPublishResult> {
  const response = await axios.post<PyPublishOut>(`${BASE}/publish`, {
    flow_id: input.flowId,
    namespace_id: input.namespaceId ?? null,
    message: input.message ?? null,
    overwrite_version: input.overwriteVersion ?? null,
  });
  return { version: response.data.version, name: response.data.name };
}

export async function openFromHub(input: HubOpenInput): Promise<HubOpenResult> {
  const response = await axios.post<PyOpenOut>(`${BASE}/open`, {
    flow_uuid: input.flowUuid,
    version: input.version ?? null,
    replace: input.replace ?? false,
  });
  return { flowPath: response.data.flow_path };
}

/** The structured refusal in a failed publish or open, null for any other error. */
export function refusalOf(error: unknown): HubRefusal | null {
  const detail = (error as { response?: { data?: { detail?: unknown } } })?.response?.data?.detail;
  if (!detail || typeof detail !== "object") return null;
  const raw = detail as Record<string, unknown>;
  switch (raw.kind) {
    case "stale_base":
      return {
        kind: "stale_base",
        flowUuid: String(raw.flow_uuid),
        base: versionFromPy(raw.base as PyVersion | null),
        current: versionFromPy(raw.current as PyVersion | null),
      };
    case "refused":
      return { kind: "refused", status: Number(raw.status), detail: String(raw.detail) };
    case "other_hub":
      return { kind: "other_hub", hubUrl: String(raw.hub_url) };
    case "local_copy_exists":
      return { kind: "local_copy_exists", flowPath: String(raw.flow_path) };
    default:
      return null;
  }
}
