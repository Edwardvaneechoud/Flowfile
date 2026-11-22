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
  /** Publish a flow already on the hub as a new flow; the open flow then becomes that flow. */
  as_new?: boolean;
  /** The new flow's name, with `as_new`; the flow's own name when left out. */
  name?: string;
}

export interface HubPublishResult {
  version: number;
  name: string;
  /** The open flow's new id, answered only when the publish moved it (a publish as a new flow). */
  flow_id?: number;
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
  | { kind: "local_copy_exists"; flow_path: string }
  | { kind: "unresolved_conflicts"; count: number; message: string };

const REFUSAL_KINDS: unknown[] = [
  "stale_base",
  "refused",
  "other_hub",
  "local_copy_exists",
  "unresolved_conflicts",
];

/** One step a merge changed on the open canvas, with the values that differ. */
export interface HubMergeStep {
  kind: "added" | "removed" | "changed";
  label: string;
  details: string[];
}

/** A part both sides changed; the canvas keeps the person's version until it is settled. */
export interface HubMergeConflict {
  key: string;
  kind: string;
  label: string;
  message: string;
}

export interface HubMergeResult {
  /** The hub's version merged in. */
  version: number;
  /** False when the open flow already builds on the hub's newest version. */
  applied: boolean;
  changes: HubMergeStep[];
  conflicts: HubMergeConflict[];
  warnings: string[];
}

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

/** Merge the hub's newest version into the open flow, step by step, as one undo step. */
export async function mergeFromHub(flowId: number): Promise<HubMergeResult> {
  const response = await axios.post<HubMergeResult>(`${BASE}/merge`, { flow_id: flowId });
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

/** A merge conflict held on the open flow, with what each side did to the step, one line each. */
export interface MergeConflictRow {
  key: string;
  kind: string;
  label: string;
  message: string;
  step_id: number | null;
  ours_label: string;
  theirs_label: string;
  ours: string[];
  theirs: string[];
}

export async function listMergeConflicts(flowId: number): Promise<MergeConflictRow[]> {
  const response = await axios.get<{ conflicts: MergeConflictRow[] }>("/editor/merge/conflicts", {
    params: { flow_id: flowId },
  });
  return response.data.conflicts;
}

/** Take one side of one conflict, as one undo step; answers the conflicts left. */
export async function resolveMergeConflict(
  flowId: number,
  key: string,
  take: "ours" | "theirs",
): Promise<MergeConflictRow[]> {
  const response = await axios.post<{ conflicts: MergeConflictRow[] }>("/editor/merge/resolve/", {
    flow_id: flowId,
    key,
    take,
  });
  return response.data.conflicts;
}
