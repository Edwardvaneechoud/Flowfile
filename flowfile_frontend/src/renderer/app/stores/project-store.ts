// Project store — git-versioning lifecycle and the always-on sync status.
// Fully optional: with no active project everything is a cheap no-op and the UI hides.
import { defineStore } from "pinia";
import { ProjectApi, ProjectFeatureUnavailable } from "../api/project.api";
import type {
  ImportResult,
  ProjectInfo,
  ProjectMergeState,
  ProjectPullResult,
  ProjectVersion,
  ProjectVersionChange,
} from "../types";
import { useCatalogStore } from "./catalog-store";
import { useFlowStore } from "./flow-store";

export type ProjectStatus = "none" | "external" | "unsaved" | "clean";

/** Where the editor stands with a pull's merge after it settled a flow's last conflict. */
export type EditorSettleOutcome =
  | { state: "merged" }
  | { state: "conclude" }
  | { state: "settle_rest"; left: number };

const MERGE_FLOWS_KEY = "flowfile.project.mergeFlows.v1";

const readMergeFlows = (): Record<number, string> => {
  try {
    return JSON.parse(sessionStorage.getItem(MERGE_FLOWS_KEY) ?? "{}") ?? {};
  } catch {
    return {};
  }
};

const writeMergeFlows = (flows: Record<number, string>): void => {
  try {
    sessionStorage.setItem(MERGE_FLOWS_KEY, JSON.stringify(flows));
  } catch {
    // Without storage a reload just forgets which flows came from the pull.
  }
};

/** A pull that brought an import in; `up_to_date`, `ahead` and `conflicted` applied nothing. */
export const pullApplied = (result: ProjectPullResult | null): boolean => !!result?.result;

interface ProjectState {
  activeProject: ProjectInfo | null;
  hasExternalChanges: boolean; // git HEAD differs from last synced (files changed outside)
  externalEdits: boolean; // the folder holds edits that were never applied
  dirty: boolean; // authoritative: the database differs from the last version
  hasUnsavedChanges: boolean; // optimistic: set instantly on a local save, reconciled by refresh
  placeholderSecrets: string[];
  featureUnavailable: boolean; // /project router gated off (404)
  versions: ProjectVersion[];
  versionsAvailable: boolean; // Phase-2 capability flags — hide UI on first 404
  reloadAvailable: boolean;
  closeAvailable: boolean;
  loading: boolean;
  saving: boolean;
  loadingVersions: boolean;
  error: string | null;
  mergeFlows: Record<number, string>; // flow id -> project path, opened in the editor from a pull's merge
}

export const useProjectStore = defineStore("project", {
  state: (): ProjectState => ({
    activeProject: null,
    hasExternalChanges: false,
    externalEdits: false,
    dirty: false,
    hasUnsavedChanges: false,
    placeholderSecrets: [],
    featureUnavailable: false,
    versions: [],
    versionsAvailable: true,
    reloadAvailable: true,
    closeAvailable: true,
    loading: false,
    saving: false,
    loadingVersions: false,
    error: null,
    mergeFlows: readMergeFlows(),
  }),

  getters: {
    isActive: (s): boolean => s.activeProject !== null,
    status(s): ProjectStatus {
      if (!s.activeProject) return "none";
      if (s.hasExternalChanges || s.externalEdits) return "external";
      if (s.dirty || s.hasUnsavedChanges) return "unsaved";
      return "clean";
    },
  },

  actions: {
    /** Cheap poll of the active project + sync flags. Safe to call on boot and on focus. */
    async refreshActive(): Promise<void> {
      this.loading = true;
      this.error = null;
      this.featureUnavailable = false;
      try {
        const res = await ProjectApi.getActive();
        this.activeProject = res.project;
        if (res.project) {
          this.hasExternalChanges = !!res.has_external_changes;
          this.externalEdits = !!res.external_edits;
          this.dirty = !!res.dirty;
          if (this.dirty) this.hasUnsavedChanges = false; // authoritative wins once known
        } else {
          this.hasExternalChanges = false;
          this.externalEdits = false;
          this.dirty = false;
          this.hasUnsavedChanges = false;
          this.placeholderSecrets = [];
          this.versions = [];
        }
      } catch (e: any) {
        if (e instanceof ProjectFeatureUnavailable) {
          this.featureUnavailable = true;
          this.activeProject = null;
        } else {
          this.error = e?.message ?? "Failed to load project status";
        }
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
        this.externalEdits = false;
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
        this.externalEdits = false;
        this.dirty = false;
        this.hasUnsavedChanges = false;
        this.onCatalogRewritten();
        return res;
      } finally {
        this.loading = false;
      }
    },

    async saveVersion(message: string, overwrite = false): Promise<string | null> {
      this.saving = true;
      try {
        const { sha } = await ProjectApi.saveVersion(message, overwrite);
        this.hasUnsavedChanges = false;
        await this.refreshActive();
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

    /** An open, restore, reload, branch switch or pull rewrote the catalog's flows: drop its cached overview. */
    onCatalogRewritten(): void {
      useCatalogStore()
        .invalidateOverview()
        .catch(() => undefined);
    },

    /** Optimistic dirty signal from a local flow save or delete; reconciled by refreshActive. */
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

    /** What a restore of `sha`, or a reload without one, would change, per object. [] when unavailable. */
    async loadPlan(sha?: string): Promise<ProjectVersionChange[]> {
      try {
        return await ProjectApi.plan(sha);
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

    /**
     * After an import rewrote the catalog's flows (restore, reload, branch switch, applied pull): clear the
     * active flow so a removed or replaced tab can't 404-loop, keep the secrets it left to fill, and re-read.
     */
    async afterImport(res: ImportResult | null): Promise<void> {
      if (res) this.placeholderSecrets = res.placeholder_secrets ?? this.placeholderSecrets;
      useFlowStore().setFlowId(-1);
      this.onCatalogRewritten();
      await this.refreshActive();
      this.loadVersions().catch(() => undefined);
    },

    async restoreVersion(sha: string, label?: string, force = false): Promise<ImportResult> {
      const res = await ProjectApi.restore(sha, label, force);
      await this.afterImport(res);
      return res;
    },

    async reloadFromDisk(force = false): Promise<ImportResult> {
      try {
        const res = await ProjectApi.reload(force);
        await this.afterImport(res);
        return res;
      } catch (e: any) {
        if (e instanceof ProjectFeatureUnavailable) this.reloadAvailable = false;
        throw e;
      }
    },

    async switchBranch(name: string, force = false): Promise<ImportResult> {
      const res = await ProjectApi.switchBranch(name, force);
      await this.afterImport(res);
      return res;
    },

    /** The pull, its merge settled or concluded: whichever answer applied an import runs the post-import path. */
    async afterPull(result: ProjectPullResult): Promise<ProjectPullResult> {
      if (result.state !== "conflicted") this.forgetMergeFlows();
      if (pullApplied(result)) await this.afterImport(result.result);
      return result;
    },

    async pull(force = false): Promise<ProjectPullResult> {
      return this.afterPull(await ProjectApi.pull(force));
    },

    async settleMerge(
      path: string,
      key: string | null,
      take: "ours" | "theirs",
      force = false,
    ): Promise<{ merge: ProjectMergeState | null; pull: ProjectPullResult | null }> {
      const answer = await ProjectApi.settleMerge(path, key, take, force);
      if (answer.pull) await this.afterPull(answer.pull);
      return answer;
    },

    async concludeMerge(force = false): Promise<ProjectPullResult> {
      return this.afterPull(await ProjectApi.concludeMerge(force));
    },

    async abortMerge(): Promise<void> {
      await ProjectApi.abortMerge();
      this.forgetMergeFlows();
    },

    /** Open a conflicted flow of the waiting merge in the editor, remembering it came from the pull. */
    async openMergeFlow(path: string): Promise<number> {
      const flowId = await ProjectApi.openMergeFlow(path);
      this.mergeFlows = { ...this.mergeFlows, [flowId]: path };
      writeMergeFlows(this.mergeFlows);
      return flowId;
    },

    isMergeFlow(flowId: number): boolean {
      return flowId in this.mergeFlows;
    },

    forgetMergeFlows(): void {
      if (!Object.keys(this.mergeFlows).length) return;
      this.mergeFlows = {};
      writeMergeFlows(this.mergeFlows);
    },

    /**
     * The editor settled the last conflict of a flow opened from the pull; core wrote it into the merge and
     * concluded it when nothing else was left and its guard allowed. Answers what the person does next.
     */
    async afterEditorSettle(): Promise<EditorSettleOutcome> {
      const merge = await ProjectApi.getMerge();
      if (merge === null) {
        this.forgetMergeFlows();
        this.onCatalogRewritten();
        await this.refreshActive();
        this.loadVersions().catch(() => undefined);
        return { state: "merged" };
      }
      if (!merge.conflicts.length) return { state: "conclude" };
      return { state: "settle_rest", left: merge.conflicts.length };
    },

    async fillSecrets(secrets: { name: string; value: string }[]): Promise<void> {
      await ProjectApi.setSecrets(secrets);
      const filled = new Set(secrets.map((s) => s.name));
      this.placeholderSecrets = this.placeholderSecrets.filter((n) => !filled.has(n));
    },

    async closeProject(): Promise<void> {
      try {
        await ProjectApi.close();
        this.forgetMergeFlows();
        this.$reset();
      } catch (e: any) {
        if (e instanceof ProjectFeatureUnavailable) this.closeAvailable = false;
        throw e;
      }
    },
  },
});
