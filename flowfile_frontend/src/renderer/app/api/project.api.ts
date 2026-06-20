import axios from "../services/axios.config";
import type {
  ActiveProjectResult,
  ImportResult,
  OpenProjectResult,
  PlaceholderSecretInput,
  ProjectInfo,
  ProjectVersion,
  ProjectVersionChange,
  SaveVersionResult,
} from "../types";

/** Raised when a Phase-2 route isn't deployed yet, so the store can hide its UI. */
export class ProjectFeatureUnavailable extends Error {}

const detail = (error: any, fallback: string): string =>
  error?.response?.data?.detail || error?.message || fallback;

export class ProjectApi {
  /** Create a new git-backed project in a folder and activate it. */
  static async init(folderPath: string, name?: string): Promise<ProjectInfo> {
    try {
      const res = await axios.post<{ project: ProjectInfo }>("/project/init", {
        folder_path: folderPath,
        ...(name ? { name } : {}),
      });
      return res.data.project;
    } catch (error) {
      throw new Error(detail(error, "Failed to create project"));
    }
  }

  /** Open an existing project folder (synchronous — rebuilds the environment from files). */
  static async open(folderPath: string): Promise<OpenProjectResult> {
    try {
      const res = await axios.post<OpenProjectResult>("/project/open", {
        folder_path: folderPath,
      });
      return res.data;
    } catch (error: any) {
      if (error?.response?.status === 404) {
        throw new Error("No Flowfile project was found in that folder.");
      }
      throw new Error(detail(error, "Failed to open project"));
    }
  }

  /** Current active project + sync status. Never throws on "no project". */
  static async getActive(): Promise<ActiveProjectResult> {
    const res = await axios.get<ActiveProjectResult>("/project/active");
    return res.data;
  }

  /** Update project settings. Returns the persisted track-data-artifacts value. */
  static async updateSettings(trackDataArtifacts: boolean): Promise<boolean> {
    try {
      const res = await axios.put<{ track_data_artifacts: boolean }>("/project/settings", {
        track_data_artifacts: trackDataArtifacts,
      });
      return res.data.track_data_artifacts;
    } catch (error: any) {
      if (error?.response?.status === 404) throw new ProjectFeatureUnavailable();
      throw new Error(detail(error, "Failed to update project settings"));
    }
  }

  /** Save a version (commit). sha is null when nothing changed. */
  static async saveVersion(message: string): Promise<SaveVersionResult> {
    try {
      const res = await axios.post<SaveVersionResult>("/project/versions", { message });
      return res.data;
    } catch (error) {
      throw new Error(detail(error, "Failed to save version"));
    }
  }

  /** Version history (newest first). Throws ProjectFeatureUnavailable when the route is absent. */
  static async getVersions(limit = 50): Promise<ProjectVersion[]> {
    try {
      const res = await axios.get<{ versions: ProjectVersion[] }>("/project/versions", {
        params: { limit },
      });
      return res.data.versions ?? [];
    } catch (error: any) {
      if (error?.response?.status === 404 || error?.response?.status === 405) {
        throw new ProjectFeatureUnavailable();
      }
      throw new Error(detail(error, "Failed to load version history"));
    }
  }

  /** Restore the project files to a version and rebuild the environment. */
  static async restore(sha: string, label?: string): Promise<ImportResult> {
    try {
      const res = await axios.post<ImportResult>("/project/restore", { sha, label });
      return res.data;
    } catch (error: any) {
      if (error?.response?.status === 404) throw new ProjectFeatureUnavailable();
      throw new Error(detail(error, "Failed to restore version"));
    }
  }

  /** Friendly summary of what restoring a version would change (vs the latest saved version). */
  static async getVersionChanges(sha: string): Promise<ProjectVersionChange[]> {
    try {
      const res = await axios.get<{ changes: ProjectVersionChange[] }>(
        `/project/versions/${sha}/changes`,
      );
      return res.data.changes ?? [];
    } catch (error: any) {
      if (error?.response?.status === 404) throw new ProjectFeatureUnavailable();
      throw new Error(detail(error, "Failed to load changes"));
    }
  }

  /** Friendly changelog of what a specific version changed (vs the version before it). */
  static async getVersionDiff(sha: string): Promise<ProjectVersionChange[]> {
    try {
      const res = await axios.get<{ changes: ProjectVersionChange[] }>(
        `/project/versions/${sha}/diff`,
      );
      return res.data.changes ?? [];
    } catch (error: any) {
      if (error?.response?.status === 404) throw new ProjectFeatureUnavailable();
      throw new Error(detail(error, "Failed to load version changes"));
    }
  }

  /** Friendly summary of the current unsaved working-tree changes. */
  static async getUncommittedChanges(): Promise<ProjectVersionChange[]> {
    try {
      const res = await axios.get<{ changes: ProjectVersionChange[] }>("/project/uncommitted");
      return res.data.changes ?? [];
    } catch (error: any) {
      if (error?.response?.status === 404) throw new ProjectFeatureUnavailable();
      throw new Error(detail(error, "Failed to load unsaved changes"));
    }
  }

  /** Accept external (on-disk) changes by rebuilding from files. */
  static async reload(): Promise<ImportResult> {
    try {
      const res = await axios.post<ImportResult>("/project/reload", {});
      return res.data;
    } catch (error: any) {
      if (error?.response?.status === 404) throw new ProjectFeatureUnavailable();
      throw new Error(detail(error, "Failed to reload project"));
    }
  }

  /** Stop tracking the active project (files stay on disk). */
  static async close(): Promise<void> {
    try {
      await axios.post("/project/close", {});
    } catch (error: any) {
      if (error?.response?.status === 404) throw new ProjectFeatureUnavailable();
      throw new Error(detail(error, "Failed to close project"));
    }
  }

  /** Set/overwrite standalone placeholder-secret values. */
  static async setSecrets(secrets: PlaceholderSecretInput[]): Promise<void> {
    try {
      await axios.post("/project/secrets", { secrets });
    } catch (error: any) {
      if (error?.response?.status === 404) throw new ProjectFeatureUnavailable();
      throw new Error(detail(error, "Failed to set secret values"));
    }
  }
}
