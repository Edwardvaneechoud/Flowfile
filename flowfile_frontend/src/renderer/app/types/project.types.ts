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
  prune_errors?: string[]; // resources that failed to prune during restore/reload
  recovery_sha?: string | null; // autosave commit made before a forced restore/reload
}

export interface OpenProjectResult extends ImportResult {
  project: ProjectInfo;
}

export interface ActiveProjectResult {
  project: ProjectInfo | null;
  has_external_changes?: boolean; // present only when a project is active
  dirty?: boolean; // working tree has changes to save as a version
  projection_failed?: boolean; // last DB→files sync hook errored — folder may have drifted
}

export interface SaveVersionResult {
  sha: string | null; // null = nothing changed since the last version
}

export interface ProjectVersion {
  sha: string;
  message: string;
  committed_at: string; // ISO timestamp
}

export interface ProjectVersionChange {
  change: "removed" | "added" | "modified";
  kind: string; // "flow" | "database connection" | "cloud connection" | "schedule" | "settings"
  label: string;
  path: string;
}

export interface PlaceholderSecretInput {
  name: string;
  value: string;
}
