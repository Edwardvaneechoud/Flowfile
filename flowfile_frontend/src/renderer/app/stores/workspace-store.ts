// Workspace store — owns the selected project, its sync status, and the
// results of the last export/apply. The runtime DB stays the source of truth;
// export/apply are explicit user actions surfaced through this store.
import { defineStore } from "pinia";
import { WorkspaceApi } from "../api/workspace.api";
import type {
  DriftReport,
  SecretRequirement,
  WorkspaceApplyResult,
  WorkspaceExportResult,
  WorkspaceProjectInfo,
  WorkspaceStatus,
} from "../types";

const SELECTED_ROOT_KEY = "workspace-selected-root";

function errMessage(error: unknown, fallback: string): string {
  const err = error as { response?: { data?: { detail?: string } }; message?: string };
  return err?.response?.data?.detail || err?.message || fallback;
}

interface WorkspaceState {
  projects: WorkspaceProjectInfo[];
  selectedRoot: string | null;
  status: WorkspaceStatus | null;
  requiredSecrets: SecretRequirement[];
  lastExport: WorkspaceExportResult | null;
  lastApply: WorkspaceApplyResult | null;
  loadingProjects: boolean;
  loadingStatus: boolean;
  exporting: boolean;
  applying: boolean;
  error: string | null;
}

export const useWorkspaceStore = defineStore("workspace", {
  state: (): WorkspaceState => ({
    projects: [],
    selectedRoot: localStorage.getItem(SELECTED_ROOT_KEY),
    status: null,
    requiredSecrets: [],
    lastExport: null,
    lastApply: null,
    loadingProjects: false,
    loadingStatus: false,
    exporting: false,
    applying: false,
    error: null,
  }),

  getters: {
    hasProjects: (state): boolean => state.projects.length > 0,
    selectedProject: (state): WorkspaceProjectInfo | null =>
      state.projects.find((p) => p.root_path === state.selectedRoot) ?? null,
    drift: (state): DriftReport | null => state.status?.drift ?? null,
    pendingChangeCount: (state): number => {
      const d = state.status?.drift;
      return d ? d.db_ahead.length + d.files_ahead.length + d.conflict.length : 0;
    },
  },

  actions: {
    setSelectedRoot(root: string | null) {
      this.selectedRoot = root;
      if (root) {
        localStorage.setItem(SELECTED_ROOT_KEY, root);
      } else {
        localStorage.removeItem(SELECTED_ROOT_KEY);
      }
    },

    async loadProjects(): Promise<WorkspaceProjectInfo[]> {
      this.loadingProjects = true;
      this.error = null;
      try {
        this.projects = await WorkspaceApi.getProjects();
        // Keep the current selection if it still exists, else fall back to the newest.
        if (!this.selectedProject) {
          this.setSelectedRoot(this.projects[0]?.root_path ?? null);
        }
        return this.projects;
      } catch (error) {
        this.error = errMessage(error, "Failed to load projects");
        throw error;
      } finally {
        this.loadingProjects = false;
      }
    },

    async loadStatus(): Promise<void> {
      if (!this.selectedRoot) {
        this.status = null;
        this.requiredSecrets = [];
        return;
      }
      this.loadingStatus = true;
      this.error = null;
      try {
        const [status, secrets] = await Promise.all([
          WorkspaceApi.getStatus(this.selectedRoot),
          WorkspaceApi.getRequiredSecrets(this.selectedRoot),
        ]);
        this.status = status;
        this.requiredSecrets = secrets;
      } catch (error) {
        this.error = errMessage(error, "Failed to load workspace status");
        throw error;
      } finally {
        this.loadingStatus = false;
      }
    },

    async exportProject(): Promise<WorkspaceExportResult> {
      if (!this.selectedRoot) throw new Error("No project selected");
      this.exporting = true;
      this.error = null;
      try {
        this.lastExport = await WorkspaceApi.export(this.selectedRoot);
        await this.loadStatus();
        return this.lastExport;
      } catch (error) {
        this.error = errMessage(error, "Export failed");
        throw error;
      } finally {
        this.exporting = false;
      }
    },

    async applyProject(): Promise<WorkspaceApplyResult> {
      if (!this.selectedRoot) throw new Error("No project selected");
      this.applying = true;
      this.error = null;
      try {
        this.lastApply = await WorkspaceApi.apply(this.selectedRoot);
        await this.loadStatus();
        return this.lastApply;
      } catch (error) {
        this.error = errMessage(error, "Apply failed");
        throw error;
      } finally {
        this.applying = false;
      }
    },

    async initProject(name: string, rootPath: string): Promise<void> {
      this.error = null;
      await WorkspaceApi.init({ name, root_path: rootPath });
      await this.loadProjects();
      this.setSelectedRoot(rootPath);
      this.lastExport = null;
      this.lastApply = null;
      await this.loadStatus();
    },

    async selectProject(root: string): Promise<void> {
      this.setSelectedRoot(root);
      this.lastExport = null;
      this.lastApply = null;
      await this.loadStatus();
    },
  },
});
