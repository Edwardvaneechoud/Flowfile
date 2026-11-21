// Project (git versioning) types — mirrors the /project REST contract.

export interface ProjectInfo {
  id: number;
  name: string;
  folder_path: string;
  track_data_artifacts: boolean; // version catalog tables, dashboards and ML models alongside flows
}

export interface ImportedCounts {
  flows: number;
  connections: number;
  schedules: number;
}

/** Returned by open / restore / reload — what was rebuilt from the project files. */
export interface ImportResult {
  imported: ImportedCounts;
  placeholder_secrets: string[];
  incomplete?: string[]; // steps that failed after the database committed, e.g. a flow file it could not write
  recovery_sha?: string | null; // autosave commit made before a forced restore/reload
}

export interface OpenProjectResult extends ImportResult {
  project: ProjectInfo;
}

export interface ActiveProjectResult {
  project: ProjectInfo | null;
  has_external_changes?: boolean; // present only when a project is active
  dirty?: boolean; // working tree has changes to save as a version
  external_edits?: boolean | null; // the folder holds edits that were never applied
}

export interface SaveVersionResult {
  sha: string | null; // null = nothing changed since the last version
}

export interface ProjectVersion {
  sha: string;
  message: string;
  committed_at: string; // ISO timestamp
}

/** One step of a changed flow, as a version's diff names it: the values that differ, one line each. */
export interface ProjectStepChange {
  kind: "added" | "removed" | "changed";
  label: string;
  details: string[];
}

export interface ProjectVersionChange {
  change: "removed" | "added" | "modified";
  kind: string; // "flow" | "database connection" | "cloud connection" | "schedule" | "settings"
  label: string;
  path: string;
  consequence?: string | null;
  /** A changed flow in a version's diff: what changed in it, step by step. */
  steps?: ProjectStepChange[];
}

export interface PlaceholderSecretInput {
  name: string;
  value: string;
}

/** One local branch of the project; `ahead`/`behind` against the remote's copy, null when it has none. */
export interface ProjectBranch {
  name: string;
  sha: string;
  current: boolean;
  ahead: number | null;
  behind: number | null;
}

export interface ProjectBranchState {
  current: string | null;
  branches: ProjectBranch[];
  remote: string | null;
  has_token: boolean;
}

/** One item a pull's merge waits on: a step both sides changed (`key`), or a whole file (`key` null). */
export interface ProjectMergeItem {
  path: string;
  key: string | null;
  kind: string;
  label: string;
  message: string;
  step_id: number | null;
  ours_label: string;
  theirs_label: string;
  ours: string[];
  theirs: string[];
}

export interface ProjectMergeState {
  branch: string;
  ours: string;
  theirs: string;
  sides: Record<string, string>;
  conflicts: ProjectMergeItem[];
  flows: string[];
  warnings: string[];
}

export type ProjectPullState = "up_to_date" | "ahead" | "fast_forward" | "merged" | "conflicted";

export interface ProjectPullResult {
  state: ProjectPullState;
  sha: string | null;
  result: ImportResult | null;
  conflicts: ProjectMergeItem[];
  warnings: string[];
}

export interface ProjectPushResult {
  branch: string;
  sha: string | null;
  unsaved: boolean;
}
