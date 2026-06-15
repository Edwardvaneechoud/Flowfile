// Workspace (git-enabled project) types — mirror flowfile_core/workspace/models.py
// and the response shapes in flowfile_core/routes/workspace.py.

export interface ProjectManifest {
  project_version: string;
  flowfile_version: string;
  project_id: string;
  name: string;
  namespace_roots: string[];
  normalization: string;
}

export interface WorkspaceProjectInfo {
  project_id: string;
  name: string;
  root_path: string;
  namespace_roots: string[];
  git_enabled: boolean;
}

export interface SecretRequirement {
  name: string;
  required_by: string[];
  resolved?: boolean | null;
  source?: string | null;
}

export interface DriftReport {
  db_ahead: string[];
  files_ahead: string[];
  conflict: string[];
  in_sync: boolean;
}

export interface WorkspaceExportResult {
  project_root: string;
  written: string[];
  unchanged: string[];
  removed: string[];
  secret_requirements: SecretRequirement[];
  warnings: string[];
  counts: Record<string, number>;
}

export interface WorkspaceApplyResult {
  project_root: string;
  counts: Record<string, number>;
  missing_secrets: SecretRequirement[];
  resolved_secrets: SecretRequirement[];
  skipped: string[];
  warnings: string[];
}

export interface WorkspaceStatus {
  project_root: string;
  manifest: ProjectManifest | null;
  git_enabled: boolean;
  drift: DriftReport;
  secret_requirements: SecretRequirement[];
}

export interface WorkspaceInitRequest {
  root_path: string;
  name: string;
  namespace_roots?: string[];
}

// Mirror the backend's env_var_name(): FLOWFILE_SECRET_<UPPER_SNAKE_NAME>.
export function workspaceSecretEnvVar(name: string): string {
  const upper = name
    .trim()
    .toUpperCase()
    .replace(/[^A-Z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "");
  return `FLOWFILE_SECRET_${upper}`;
}
