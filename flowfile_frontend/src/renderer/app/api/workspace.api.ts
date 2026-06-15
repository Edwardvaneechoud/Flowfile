// Workspace API — git-enabled project export/import (mirrors /workspace/* on core).
import axios from "../services/axios.config";
import type {
  ProjectManifest,
  SecretRequirement,
  WorkspaceApplyResult,
  WorkspaceExportResult,
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
}
