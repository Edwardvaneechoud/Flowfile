// Project store — git-versioning lifecycle and the always-on sync status.
// Fully optional: with no active project everything is a cheap no-op and the UI hides.
import { defineStore } from "pinia";
import { ProjectApi, ProjectFeatureUnavailable } from "../api/project.api";
import type { ImportResult, ProjectInfo, ProjectVersion, ProjectVersionChange } from "../types";

export type ProjectStatus = "none" | "external" | "unsaved" | "clean";

interface ProjectState {
  activeProject: ProjectInfo | null;
  hasExternalChanges: boolean; // git HEAD differs from last synced (files changed outside)
  dirty: boolean; // authoritative: working tree has uncommitted changes
  hasUnsavedChanges: boolean; // optimistic: set instantly on a local save, reconciled by refresh
  placeholderSecrets: string[];
  versions: ProjectVersion[];
  versionsAvailable: boolean; // Phase-2 capability flags — hide UI on first 404
  reloadAvailable: boolean;
  closeAvailable: boolean;
  loading: boolean;
  saving: boolean;
  loadingVersions: boolean;
  error: string | null;
}

export const useProjectStore = defineStore("project", {
  state: (): ProjectState => ({
    activeProject: null,
    hasExternalChanges: false,
    dirty: false,
    hasUnsavedChanges: false,
    placeholderSecrets: [],
    versions: [],
    versionsAvailable: true,
    reloadAvailable: true,
    closeAvailable: true,
    loading: false,
    saving: false,
    loadingVersions: false,
    error: null,
  }),

  getters: {
    isActive: (s): boolean => s.activeProject !== null,
    status(s): ProjectStatus {
      if (!s.activeProject) return "none";
      if (s.hasExternalChanges) return "external";
      if (s.dirty || s.hasUnsavedChanges) return "unsaved";
      return "clean";
    },
  },

  actions: {
    /** Cheap poll of the active project + sync flags. Safe to call on boot and on focus. */
    async refreshActive(): Promise<void> {
      this.loading = true;
      this.error = null;
      try {
        const res = await ProjectApi.getActive();
        this.activeProject = res.project;
        if (res.project) {
          this.hasExternalChanges = !!res.has_external_changes;
          this.dirty = !!res.dirty;
          if (this.dirty) this.hasUnsavedChanges = false; // authoritative wins once known
        } else {
          this.hasExternalChanges = false;
          this.dirty = false;
          this.hasUnsavedChanges = false;
          this.placeholderSecrets = [];
          this.versions = [];
        }
      } catch (e: any) {
        this.error = e?.message ?? "Failed to load project status";
      } finally {
        this.loading = false;
      }
    },

    async initProject(folderPath: string, name?: string): Promise<ProjectInfo> {
      this.loading = true;
      try {
        const project = await ProjectApi.init(folderPath, name);
        this.activeProject = project;
        this.hasExternalChanges = false;
        this.dirty = false;
        this.hasUnsavedChanges = false;
        this.placeholderSecrets = [];
        this.versions = [];
        return project;
      } finally {
        this.loading = false;
      }
    },

    async openProject(folderPath: string): Promise<ImportResult> {
      this.loading = true;
      try {
        const res = await ProjectApi.open(folderPath);
        this.activeProject = res.project;
        this.placeholderSecrets = res.placeholder_secrets ?? [];
        this.hasExternalChanges = false;
        this.dirty = false;
        this.hasUnsavedChanges = false;
        return res;
      } finally {
        this.loading = false;
      }
    },

    async saveVersion(message: string): Promise<string | null> {
      this.saving = true;
      try {
        const { sha } = await ProjectApi.saveVersion(message);
        this.dirty = false;
        this.hasUnsavedChanges = false;
        this.loadVersions().catch(() => undefined);
        return sha;
      } finally {
        this.saving = false;
      }
    },

    /** Toggle whether data artifacts (tables/dashboards/models) are versioned. */
    async updateSettings(trackDataArtifacts: boolean): Promise<void> {
      const value = await ProjectApi.updateSettings(trackDataArtifacts);
      if (this.activeProject) this.activeProject.track_data_artifacts = value;
      // Re-projection may have added/removed artifact files on disk — reconcile status.
      await this.refreshActive();
    },

    /** Decoupled signal from a local save (flow/connection/secret/schedule). */
    onSourceChanged(): void {
      if (this.activeProject) this.hasUnsavedChanges = true;
    },

    async loadVersions(): Promise<void> {
      if (!this.activeProject || !this.versionsAvailable) return;
      this.loadingVersions = true;
      try {
        this.versions = await ProjectApi.getVersions();
      } catch (e: any) {
        if (e instanceof ProjectFeatureUnavailable) this.versionsAvailable = false;
        else this.error = e?.message ?? "Failed to load version history";
      } finally {
        this.loadingVersions = false;
      }
    },

    /** Preview what restoring a version would change. [] when the feature isn't available. */
    async loadVersionChanges(sha: string): Promise<ProjectVersionChange[]> {
      try {
        return await ProjectApi.getVersionChanges(sha);
      } catch (e: any) {
        if (e instanceof ProjectFeatureUnavailable) return [];
        throw e;
      }
    },

    /** A version's own changelog (vs the version before it). [] when unavailable. */
    async loadVersionDiff(sha: string): Promise<ProjectVersionChange[]> {
      try {
        return await ProjectApi.getVersionDiff(sha);
      } catch (e: any) {
        if (e instanceof ProjectFeatureUnavailable) return [];
        throw e;
      }
    },

    /** The current unsaved working-tree changes. [] when unavailable. */
    async loadUncommittedChanges(): Promise<ProjectVersionChange[]> {
      try {
        return await ProjectApi.getUncommittedChanges();
      } catch (e: any) {
        if (e instanceof ProjectFeatureUnavailable) return [];
        throw e;
      }
    },

    async restoreVersion(sha: string, label?: string): Promise<void> {
      const res = await ProjectApi.restore(sha, label);
      this.placeholderSecrets = res.placeholder_secrets ?? this.placeholderSecrets;
      await this.refreshActive();
      this.loadVersions().catch(() => undefined);
    },

    async reloadFromDisk(): Promise<ImportResult> {
      try {
        const res = await ProjectApi.reload();
        this.placeholderSecrets = res.placeholder_secrets ?? this.placeholderSecrets;
        await this.refreshActive();
        return res;
      } catch (e: any) {
        if (e instanceof ProjectFeatureUnavailable) this.reloadAvailable = false;
        throw e;
      }
    },

    async fillSecrets(secrets: { name: string; value: string }[]): Promise<void> {
      await ProjectApi.setSecrets(secrets);
      const filled = new Set(secrets.map((s) => s.name));
      this.placeholderSecrets = this.placeholderSecrets.filter((n) => !filled.has(n));
    },

    async closeProject(): Promise<void> {
      try {
        await ProjectApi.close();
        this.$reset();
      } catch (e: any) {
        if (e instanceof ProjectFeatureUnavailable) this.closeAvailable = false;
        throw e;
      }
    },
  },
});
