// Paths match the FastAPI decorators exactly: a trailing slash would answer 307.

import axios from "../services/axios.config";
import setupService from "../services/setup.service";

// On a server the hub is reached through core's own routes; on the hub's own page the hub answers directly.
const base = () => (setupService.isOnHub() ? "/hub" : "/hub_client");

// Core answers more on each (ids of registrations, owners, the flow's uuid); the editor reads these.
export interface HubFlow {
  flow_uuid: string;
  name: string;
  /** The newest version published through the hub; null when the flow has none yet. */
  latest_version: number | null;
  /** Who published the newest version, by name; null when the hub does not say. */
  author_name: string | null;
}

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

export async function listHubFlows(namespaceId?: number | null): Promise<HubFlow[]> {
  const params = namespaceId == null ? undefined : { namespace_id: namespaceId };
  const response = await axios.get<HubFlow[]>(`${base()}/flows`, { params });
  return response.data;
}

export async function listHubFlowVersions(flowUuid: string): Promise<HubFlowVersion[]> {
  const response = await axios.get<HubFlowVersion[]>(`${base()}/flows/${flowUuid}/versions`);
  return response.data;
}

/** The nodes of the flow that read or write a path on this machine; a warning, never a block. */
export async function publishCheck(flowId: number): Promise<LocalPathUse[]> {
  const response = await axios.get<LocalPathUse[]>(`${base()}/publish_check`, {
    params: { flow_id: flowId },
  });
  return response.data;
}

export async function publishToHub(input: HubPublishInput): Promise<HubPublishResult> {
  const response = await axios.post<HubPublishResult>(`${base()}/publish`, input);
  return response.data;
}

export async function openFromHub(input: HubOpenInput): Promise<HubOpenResult> {
  const response = await axios.post<HubOpenResult>(`${base()}/open`, input);
  return response.data;
}

/** The structured refusal in a failed publish or open, null for any other error. */
export function refusalOf(error: unknown): HubRefusal | null {
  const failure = error as { response?: { data?: { detail?: HubRefusal } } };
  const detail = failure?.response?.data?.detail;
  return REFUSAL_KINDS.includes(detail?.kind) ? (detail as HubRefusal) : null;
}
