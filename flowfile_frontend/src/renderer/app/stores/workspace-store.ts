// Workspace store — owns the selected project, its sync status, and the
// results of the last export/apply. The runtime DB stays the source of truth;
// export/apply are explicit user actions surfaced through this store.
import { defineStore } from "pinia";
import { WorkspaceApi } from "../api/workspace.api";
import type {
  CommitResponse,
  DiffResponse,
  DriftReport,
  RestoreResponse,
  SecretRequirement,
  WorkspaceApplyResult,
  WorkspaceExportResult,
  WorkspaceGitHistory,
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
  history: WorkspaceGitHistory | null;
  lastExport: WorkspaceExportResult | null;
  lastApply: WorkspaceApplyResult | null;
  loadingProjects: boolean;
  loadingStatus: boolean;
  loadingHistory: boolean;
  exporting: boolean;
  applying: boolean;
  committing: boolean;
  error: string | null;
}

export const useWorkspaceStore = defineStore("workspace", {
  state: (): WorkspaceState => ({
    projects: [],
    selectedRoot: localStorage.getItem(SELECTED_ROOT_KEY),
    status: null,
    requiredSecrets: [],
    history: null,
    lastExport: null,
    lastApply: null,
    loadingProjects: false,
    loadingStatus: false,
    loadingHistory: false,
    exporting: false,
    applying: false,
    committing: false,
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
    // Anything to checkpoint: the DB differs from the exported tree, or the tree
    // has changes not yet committed to git.
    hasChanges: (state): boolean => {
      const driftChanged = state.status?.drift ? !state.status.drift.in_sync : false;
      return driftChanged || (state.history?.dirty ?? false);
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

    async loadHistory(): Promise<void> {
      if (!this.selectedRoot) {
        this.history = null;
        return;
      }
      this.loadingHistory = true;
      try {
        this.history = await WorkspaceApi.getHistory(this.selectedRoot);
      } catch (error) {
        // History is best-effort — don't block the rest of the screen.
        this.error = errMessage(error, "Failed to load history");
      } finally {
        this.loadingHistory = false;
      }
    },

    // Reload status + history together (best-effort) after any mutating action.
    async refresh(): Promise<void> {
      await Promise.allSettled([this.loadStatus(), this.loadHistory()]);
    },

    async exportProject(): Promise<WorkspaceExportResult> {
      if (!this.selectedRoot) throw new Error("No project selected");
      this.exporting = true;
      this.error = null;
      try {
        this.lastExport = await WorkspaceApi.export(this.selectedRoot);
        await this.refresh();
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
        await this.refresh();
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
      await this.refresh();
    },

    async selectProject(root: string): Promise<void> {
      this.setSelectedRoot(root);
      this.lastExport = null;
      this.lastApply = null;
      await this.refresh();
    },

    // The one primary action: snapshot the current environment. Exports the DB
    // to files, then records a git checkpoint — surfaced to the user as a single
    // "Create checkpoint" step.
    async createCheckpoint(
      message: string,
    ): Promise<{ exported: WorkspaceExportResult; committed: CommitResponse }> {
      if (!this.selectedRoot) throw new Error("No project selected");
      this.committing = true;
      this.error = null;
      try {
        const exported = await WorkspaceApi.export(this.selectedRoot);
        this.lastExport = exported;
        const committed = await WorkspaceApi.commit(this.selectedRoot, message);
        await this.refresh();
        return { exported, committed };
      } catch (error) {
        this.error = errMessage(error, "Failed to create checkpoint");
        throw error;
      } finally {
        this.committing = false;
      }
    },

    async commit(message: string): Promise<CommitResponse> {
      if (!this.selectedRoot) throw new Error("No project selected");
      this.committing = true;
      this.error = null;
      try {
        const result = await WorkspaceApi.commit(this.selectedRoot, message);
        await this.refresh();
        return result;
      } catch (error) {
        this.error = errMessage(error, "Commit failed");
        throw error;
      } finally {
        this.committing = false;
      }
    },

    async restore(sha: string): Promise<RestoreResponse> {
      if (!this.selectedRoot) throw new Error("No project selected");
      this.applying = true;
      this.error = null;
      try {
        const result = await WorkspaceApi.restore(this.selectedRoot, sha, true);
        if (result.applied) this.lastApply = result.applied;
        await this.refresh();
        return result;
      } catch (error) {
        this.error = errMessage(error, "Restore failed");
        throw error;
      } finally {
        this.applying = false;
      }
    },

    async fetchDiff(sha?: string | null): Promise<DiffResponse> {
      return WorkspaceApi.getDiff(this.selectedRoot, sha);
    },
  },
});
