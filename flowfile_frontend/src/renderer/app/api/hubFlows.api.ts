// Paths match the FastAPI decorators exactly: a trailing slash would answer 307.

import axios from "../services/axios.config";

const BASE = "/hub_client";

export interface HubFlowVersion {
  version: number;
  author_id: number;
  /** The author's display name; null from a hub that sends the id only. */
  author_name: string | null;
  published_at: string;
  message: string | null;
}

/** A node whose settings name a path on this machine, which other users of the hub will not have. */
export interface LocalPathUse {
  label: string;
  path: string;
}

export interface HubPublishInput {
  flow_id: number;
  /** Read by the hub on a first publish only; a later publish never moves the flow. */
  namespace_id?: number | null;
  message?: string;
  /** The version to publish over after a stale-base refusal. */
  overwrite_version?: number;
}

export interface HubPublishResult {
  version: number;
  name: string;
}

export interface HubOpenInput {
  flow_uuid: string;
  version?: number;
  /** Overwrite the local copy of this hub flow when one exists. */
  replace?: boolean;
}

export interface HubOpenResult {
  flow_path: string;
  /** What the hub says of the content it gave, shown as given; absent when it has nothing to say. */
  note?: string | null;
}

/** A refusal of publish or open, branched on its `kind`. */
export type HubRefusal =
  | {
      kind: "stale_base";
      flow_uuid: string;
      base: HubFlowVersion | null;
      current: HubFlowVersion | null;
    }
  | { kind: "refused"; status: number; detail: string }
  | { kind: "other_hub"; hub_url: string }
  | { kind: "local_copy_exists"; flow_path: string };

const REFUSAL_KINDS: unknown[] = ["stale_base", "refused", "other_hub", "local_copy_exists"];

export async function listHubFlowVersions(flowUuid: string): Promise<HubFlowVersion[]> {
  const response = await axios.get<HubFlowVersion[]>(`${BASE}/flows/${flowUuid}/versions`);
  return response.data;
}

/** The nodes of the flow that read or write a path on this machine; a warning, never a block. */
export async function publishCheck(flowId: number): Promise<LocalPathUse[]> {
  const response = await axios.get<LocalPathUse[]>(`${BASE}/publish_check`, {
    params: { flow_id: flowId },
  });
  return response.data;
}

export async function publishToHub(input: HubPublishInput): Promise<HubPublishResult> {
  const response = await axios.post<HubPublishResult>(`${BASE}/publish`, input);
  return response.data;
}

export async function openFromHub(input: HubOpenInput): Promise<HubOpenResult> {
  const response = await axios.post<HubOpenResult>(`${BASE}/open`, input);
  return response.data;
}

/** The structured refusal in a failed publish or open, null for any other error. */
export function refusalOf(error: unknown): HubRefusal | null {
  const failure = error as { response?: { data?: { detail?: HubRefusal } } };
  const detail = failure?.response?.data?.detail;
  return REFUSAL_KINDS.includes(detail?.kind) ? (detail as HubRefusal) : null;
}
