// Project (git versioning) types — mirrors the /project REST contract.

export interface ProjectInfo {
  id: number;
  name: string;
  folder_path: string;
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
}

export interface OpenProjectResult extends ImportResult {
  project: ProjectInfo;
}

export interface ActiveProjectResult {
  project: ProjectInfo | null;
  has_external_changes?: boolean; // present only when a project is active
  dirty?: boolean; // working tree has changes to save as a version
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
