// Workspace API — git-enabled project export/import (mirrors /workspace/* on core).
import axios from "../services/axios.config";
import type {
  CommitResponse,
  DiffResponse,
  ProjectManifest,
  RestoreResponse,
  SecretRequirement,
  WorkspaceApplyResult,
  WorkspaceExportResult,
  WorkspaceGitHistory,
  WorkspaceInitRequest,
  WorkspaceProjectInfo,
  WorkspaceStatus,
} from "../types";

const BASE = "/workspace";

export class WorkspaceApi {
  static async getProjects(): Promise<WorkspaceProjectInfo[]> {
    const response = await axios.get<WorkspaceProjectInfo[]>(`${BASE}/projects`);
    return response.data;
  }

  static async init(body: WorkspaceInitRequest): Promise<ProjectManifest> {
    const response = await axios.post<ProjectManifest>(`${BASE}/init`, body);
    return response.data;
  }

  static async getStatus(rootPath?: string | null): Promise<WorkspaceStatus> {
    const response = await axios.get<WorkspaceStatus>(`${BASE}/status`, {
      params: rootPath ? { root_path: rootPath } : {},
    });
    return response.data;
  }

  static async export(rootPath?: string | null): Promise<WorkspaceExportResult> {
    const response = await axios.post<WorkspaceExportResult>(`${BASE}/export`, {
      root_path: rootPath ?? null,
    });
    return response.data;
  }

  static async apply(rootPath?: string | null): Promise<WorkspaceApplyResult> {
    const response = await axios.post<WorkspaceApplyResult>(`${BASE}/apply`, {
      root_path: rootPath ?? null,
    });
    return response.data;
  }

  static async getRequiredSecrets(rootPath?: string | null): Promise<SecretRequirement[]> {
    const response = await axios.get<SecretRequirement[]>(`${BASE}/secrets/required`, {
      params: rootPath ? { root_path: rootPath } : {},
    });
    return response.data;
  }

  // ---- Embedded git (Phase 2) ----

  static async getHistory(rootPath?: string | null, limit = 50): Promise<WorkspaceGitHistory> {
    const params: Record<string, string | number> = { limit };
    if (rootPath) params.root_path = rootPath;
    const response = await axios.get<WorkspaceGitHistory>(`${BASE}/history`, { params });
    return response.data;
  }

  static async commit(
    rootPath: string | null | undefined,
    message: string,
  ): Promise<CommitResponse> {
    const response = await axios.post<CommitResponse>(`${BASE}/commit`, {
      root_path: rootPath ?? null,
      message,
    });
    return response.data;
  }

  static async getDiff(rootPath?: string | null, sha?: string | null): Promise<DiffResponse> {
    const params: Record<string, string> = {};
    if (rootPath) params.root_path = rootPath;
    if (sha) params.sha = sha;
    const response = await axios.get<DiffResponse>(`${BASE}/diff`, { params });
    return response.data;
  }

  static async restore(
    rootPath: string | null | undefined,
    sha: string,
    apply = true,
  ): Promise<RestoreResponse> {
    const response = await axios.post<RestoreResponse>(`${BASE}/restore`, {
      root_path: rootPath ?? null,
      sha,
      apply,
    });
    return response.data;
  }
}
