// Paths match the FastAPI decorators exactly: a trailing slash would answer 307.

import axios from "../services/axios.config";

const BASE = "/hub_client";

export interface HubNamespace {
  id: number;
  name: string;
  parentId: number | null;
  level: number;
  description: string | null;
}

export interface HubFlow {
  registrationId: number;
  flowUuid: string;
  name: string;
  namespaceId: number | null;
  ownerId: number;
  /** The newest version published through the hub; null when the flow has none yet. */
  latestVersion: number | null;
}

export interface HubFlowVersion {
  flowUuid: string;
  version: number;
  authorId: number;
  publishedAt: string;
  message: string | null;
}

/** A node whose settings name a path on this machine, which other users of the hub will not have. */
export interface LocalPathUse {
  nodeId: number;
  nodeType: string;
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
  flowUuid: string;
  version: number;
  name: string;
  namespaceId: number | null;
}

export interface HubOpenInput {
  flowUuid: string;
  version?: number;
  /** Overwrite the local copy of this hub flow when one exists. */
  replace?: boolean;
}

export interface HubOpenResult {
  flowPath: string;
  flowUuid: string;
  version: number;
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

interface PyNamespace {
  id: number;
  name: string;
  parent_id: number | null;
  level: number;
  description: string | null;
}

interface PyFlow {
  registration_id: number;
  flow_uuid: string;
  name: string;
  namespace_id: number | null;
  owner_id: number;
  latest_version: number | null;
}

interface PyVersion {
  flow_uuid: string;
  version: number;
  author_id: number;
  published_at: string;
  message: string | null;
}

interface PyLocalPathUse {
  node_id: number;
  node_type: string;
  label: string;
  path: string;
}

interface PyPublishOut {
  flow_uuid: string;
  version: number;
  name: string;
  namespace_id: number | null;
}

interface PyOpenOut {
  flow_path: string;
  flow_uuid: string;
  version: number;
}

const namespaceFromPy = (raw: PyNamespace): HubNamespace => ({
  id: raw.id,
  name: raw.name,
  parentId: raw.parent_id,
  level: raw.level,
  description: raw.description,
});

const flowFromPy = (raw: PyFlow): HubFlow => ({
  registrationId: raw.registration_id,
  flowUuid: raw.flow_uuid,
  name: raw.name,
  namespaceId: raw.namespace_id,
  ownerId: raw.owner_id,
  latestVersion: raw.latest_version,
});

const versionFromPy = (raw: PyVersion | null | undefined): HubFlowVersion | null =>
  raw
    ? {
        flowUuid: raw.flow_uuid,
        version: raw.version,
        authorId: raw.author_id,
        publishedAt: raw.published_at,
        message: raw.message,
      }
    : null;

/** The hub's root namespaces, or the children of `parentId`: one level of the tree per call. */
export async function listHubNamespaces(parentId?: number | null): Promise<HubNamespace[]> {
  const response =
    parentId == null
      ? await axios.get<PyNamespace[]>(`${BASE}/namespaces`)
      : await axios.get<PyNamespace[]>(`${BASE}/namespaces`, { params: { parent_id: parentId } });
  return response.data.map(namespaceFromPy);
}

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
  return response.data.map((raw) => ({
    nodeId: raw.node_id,
    nodeType: raw.node_type,
    label: raw.label,
    path: raw.path,
  }));
}

export async function publishToHub(input: HubPublishInput): Promise<HubPublishResult> {
  const response = await axios.post<PyPublishOut>(`${BASE}/publish`, {
    flow_id: input.flowId,
    namespace_id: input.namespaceId ?? null,
    message: input.message ?? null,
    overwrite_version: input.overwriteVersion ?? null,
  });
  return {
    flowUuid: response.data.flow_uuid,
    version: response.data.version,
    name: response.data.name,
    namespaceId: response.data.namespace_id,
  };
}

export async function openFromHub(input: HubOpenInput): Promise<HubOpenResult> {
  const response = await axios.post<PyOpenOut>(`${BASE}/open`, {
    flow_uuid: input.flowUuid,
    version: input.version ?? null,
    replace: input.replace ?? false,
  });
  return {
    flowPath: response.data.flow_path,
    flowUuid: response.data.flow_uuid,
    version: response.data.version,
  };
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
