// Unit tests for the project (git-versioning) store: the status getter that
// drives the header pill, and graceful Phase-2 capability degradation.
//
// The API module is mocked so tests never import axios.config (which reaches
// DOM globals at import time). The mocked ProjectFeatureUnavailable is the same
// reference the store imports, so the store's `instanceof` checks still match.

import { setActivePinia, createPinia } from "pinia";
import { beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => {
  class ProjectFeatureUnavailable extends Error {}
  return {
    ProjectFeatureUnavailable,
    getActive: vi.fn(),
    init: vi.fn(),
    open: vi.fn(),
    saveVersion: vi.fn(),
    getVersions: vi.fn(),
    updateSettings: vi.fn(),
    restore: vi.fn(),
    reload: vi.fn(),
    plan: vi.fn(),
    setSecrets: vi.fn(),
    close: vi.fn(),
    switchBranch: vi.fn(),
    pull: vi.fn(),
    settleMerge: vi.fn(),
    concludeMerge: vi.fn(),
    abortMerge: vi.fn(),
    getMerge: vi.fn(),
    openMergeFlow: vi.fn(),
    setFlowId: vi.fn(),
    invalidateOverview: vi.fn(() => Promise.resolve()),
  };
});

// Mock the flow store: the real module pulls in the `../api` barrel (axios.config),
// which reaches DOM globals at import time and would crash under the node env.
vi.mock("./flow-store", () => ({
  useFlowStore: () => ({ setFlowId: mocks.setFlowId }),
}));

vi.mock("./catalog-store", () => ({
  useCatalogStore: () => ({ invalidateOverview: mocks.invalidateOverview }),
}));

vi.mock("../api/project.api", () => ({
  ProjectFeatureUnavailable: mocks.ProjectFeatureUnavailable,
  ProjectApi: {
    getActive: mocks.getActive,
    init: mocks.init,
    open: mocks.open,
    saveVersion: mocks.saveVersion,
    getVersions: mocks.getVersions,
    updateSettings: mocks.updateSettings,
    restore: mocks.restore,
    reload: mocks.reload,
    plan: mocks.plan,
    setSecrets: mocks.setSecrets,
    close: mocks.close,
    switchBranch: mocks.switchBranch,
    pull: mocks.pull,
    settleMerge: mocks.settleMerge,
    concludeMerge: mocks.concludeMerge,
    abortMerge: mocks.abortMerge,
    getMerge: mocks.getMerge,
    openMergeFlow: mocks.openMergeFlow,
  },
}));

import { useProjectStore } from "./project-store";

const PROJECT = { id: 1, name: "Demo", folder_path: "/p", track_data_artifacts: true };

beforeEach(() => {
  setActivePinia(createPinia());
  vi.clearAllMocks();
  mocks.getVersions.mockResolvedValue([]);
});

describe("status getter", () => {
  it("is 'none' with no active project", () => {
    const store = useProjectStore();
    expect(store.isActive).toBe(false);
    expect(store.status).toBe("none");
  });

  it("is 'clean' when active with no changes", async () => {
    mocks.getActive.mockResolvedValue({
      project: PROJECT,
      has_external_changes: false,
      dirty: false,
    });
    const store = useProjectStore();
    await store.refreshActive();
    expect(store.isActive).toBe(true);
    expect(store.status).toBe("clean");
  });

  it("is 'unsaved' when the working tree is dirty", async () => {
    mocks.getActive.mockResolvedValue({
      project: PROJECT,
      has_external_changes: false,
      dirty: true,
    });
    const store = useProjectStore();
    await store.refreshActive();
    expect(store.status).toBe("unsaved");
  });

  it("prefers 'external' over 'unsaved'", async () => {
    mocks.getActive.mockResolvedValue({
      project: PROJECT,
      has_external_changes: true,
      dirty: true,
    });
    const store = useProjectStore();
    await store.refreshActive();
    expect(store.status).toBe("external");
  });
});

describe("refreshActive", () => {
  it("captures the external_edits flag", async () => {
    mocks.getActive.mockResolvedValue({ project: PROJECT, external_edits: true });
    const store = useProjectStore();
    await store.refreshActive();
    expect(store.externalEdits).toBe(true);
    expect(store.status).toBe("external");
  });

  it("clears all status when no project is active", async () => {
    mocks.getActive.mockResolvedValue({ project: null });
    const store = useProjectStore();
    store.hasExternalChanges = true;
    store.externalEdits = true;
    store.dirty = true;
    await store.refreshActive();
    expect(store.activeProject).toBeNull();
    expect(store.hasExternalChanges).toBe(false);
    expect(store.externalEdits).toBe(false);
    expect(store.dirty).toBe(false);
  });

  it("surfaces a fetch failure on the error field without throwing", async () => {
    mocks.getActive.mockRejectedValue(new Error("network down"));
    const store = useProjectStore();
    await store.refreshActive();
    expect(store.error).toBe("network down");
    expect(store.loading).toBe(false);
  });

  it("marks the feature unavailable (not an error) when the router is gated off", async () => {
    mocks.getActive.mockRejectedValue(new mocks.ProjectFeatureUnavailable());
    const store = useProjectStore();
    store.activeProject = PROJECT;
    await store.refreshActive();
    expect(store.featureUnavailable).toBe(true);
    expect(store.activeProject).toBeNull();
    expect(store.error).toBeNull();
    expect(store.loading).toBe(false);
  });
});

describe("onSourceChanged", () => {
  it("does nothing without an active project", () => {
    const store = useProjectStore();
    store.onSourceChanged();
    expect(store.hasUnsavedChanges).toBe(false);
    expect(store.status).toBe("none");
  });

  it("flags unsaved changes when a project is active", () => {
    const store = useProjectStore();
    store.activeProject = PROJECT;
    store.onSourceChanged();
    expect(store.hasUnsavedChanges).toBe(true);
    expect(store.status).toBe("unsaved");
  });
});

describe("initProject", () => {
  it("activates the new project with a clean status", async () => {
    mocks.init.mockResolvedValue(PROJECT);
    const store = useProjectStore();
    store.hasExternalChanges = true;
    store.externalEdits = true;

    const project = await store.initProject("/p", "Demo");
    expect(project).toEqual(PROJECT);
    expect(store.activeProject).toEqual(PROJECT);
    expect(store.status).toBe("clean");
    expect(store.externalEdits).toBe(false);
    expect(store.loading).toBe(false);
  });
});

describe("openProject", () => {
  it("activates the project and records placeholder secrets", async () => {
    mocks.open.mockResolvedValue({
      project: PROJECT,
      imported: { flows: 1, connections: 0, schedules: 0 },
      placeholder_secrets: ["prod_pw"],
    });
    const store = useProjectStore();

    const res = await store.openProject("/p");
    expect(res.imported.flows).toBe(1);
    expect(store.activeProject).toEqual(PROJECT);
    expect(store.placeholderSecrets).toEqual(["prod_pw"]);
    expect(store.loading).toBe(false);
  });
});

describe("restoreVersion", () => {
  it("refreshes status after a restore", async () => {
    mocks.restore.mockResolvedValue({
      imported: { flows: 0, connections: 0, schedules: 0 },
      placeholder_secrets: [],
    });
    mocks.getActive.mockResolvedValue({ project: PROJECT, dirty: false });
    const store = useProjectStore();
    store.activeProject = PROJECT;

    await store.restoreVersion("deadbeef", "rollback");
    expect(mocks.restore).toHaveBeenCalledWith("deadbeef", "rollback", false);
    expect(mocks.getActive).toHaveBeenCalled();
    expect(mocks.setFlowId).toHaveBeenCalledWith(-1); // canvas reset; no stale tab
    expect(mocks.invalidateOverview).toHaveBeenCalled();
  });

  it("sends force when the caller keeps a recovery point", async () => {
    mocks.restore.mockResolvedValue({ imported: {}, placeholder_secrets: [] });
    mocks.getActive.mockResolvedValue({ project: PROJECT, dirty: false });
    const store = useProjectStore();
    store.activeProject = PROJECT;

    await store.restoreVersion("deadbeef", "rollback", true);
    expect(mocks.restore).toHaveBeenCalledWith("deadbeef", "rollback", true);
  });
});

describe("reloadFromDisk", () => {
  it("sends force when the caller keeps a recovery point", async () => {
    mocks.reload.mockResolvedValue({ imported: {}, placeholder_secrets: [] });
    mocks.getActive.mockResolvedValue({ project: PROJECT, dirty: false });
    const store = useProjectStore();
    store.activeProject = PROJECT;

    await store.reloadFromDisk(true);
    expect(mocks.reload).toHaveBeenCalledWith(true);
  });

  it("rebuilds from disk and reconciles status", async () => {
    mocks.reload.mockResolvedValue({
      imported: { flows: 2, connections: 1, schedules: 0 },
      placeholder_secrets: ["s"],
    });
    mocks.getActive.mockResolvedValue({ project: PROJECT, dirty: false });
    const store = useProjectStore();
    store.activeProject = PROJECT;

    const res = await store.reloadFromDisk();
    expect(res.imported.connections).toBe(1);
    expect(store.placeholderSecrets).toEqual(["s"]);
    expect(mocks.getActive).toHaveBeenCalled();
    expect(mocks.setFlowId).toHaveBeenCalledWith(-1); // canvas reset; no stale tab
    expect(mocks.invalidateOverview).toHaveBeenCalled();
  });

  it("hides the reload action when the route is unavailable", async () => {
    mocks.reload.mockRejectedValue(new mocks.ProjectFeatureUnavailable());
    const store = useProjectStore();
    store.activeProject = PROJECT;

    await expect(store.reloadFromDisk()).rejects.toBeInstanceOf(mocks.ProjectFeatureUnavailable);
    expect(store.reloadAvailable).toBe(false);
  });
});

describe("loadPlan", () => {
  it("asks for the plan of the sha and returns its rows", async () => {
    const rows = [{ change: "removed", kind: "flow", label: "a", path: "flows/a.flow.yaml" }];
    mocks.plan.mockResolvedValue(rows);
    expect(await useProjectStore().loadPlan("abc123")).toEqual(rows);
    expect(mocks.plan).toHaveBeenCalledWith("abc123");
  });
});

describe("fillSecrets", () => {
  it("removes filled names from the placeholder list", async () => {
    mocks.setSecrets.mockResolvedValue(undefined);
    const store = useProjectStore();
    store.placeholderSecrets = ["a", "b", "c"];

    await store.fillSecrets([
      { name: "a", value: "1" },
      { name: "c", value: "3" },
    ]);
    expect(store.placeholderSecrets).toEqual(["b"]);
  });
});

describe("closeProject", () => {
  it("resets the store to its initial state", async () => {
    mocks.close.mockResolvedValue(undefined);
    const store = useProjectStore();
    store.activeProject = PROJECT;
    store.dirty = true;
    store.placeholderSecrets = ["x"];

    await store.closeProject();
    expect(store.activeProject).toBeNull();
    expect(store.dirty).toBe(false);
    expect(store.placeholderSecrets).toEqual([]);
  });

  it("hides the close action when the route is unavailable", async () => {
    mocks.close.mockRejectedValue(new mocks.ProjectFeatureUnavailable());
    const store = useProjectStore();
    store.activeProject = PROJECT;

    await expect(store.closeProject()).rejects.toBeInstanceOf(mocks.ProjectFeatureUnavailable);
    expect(store.closeAvailable).toBe(false);
  });
});

describe("saveVersion", () => {
  it("re-reads status after the save and refreshes history", async () => {
    mocks.saveVersion.mockResolvedValue({ sha: "abc123" });
    mocks.getActive.mockResolvedValue({ project: PROJECT, dirty: false });
    const store = useProjectStore();
    store.activeProject = PROJECT;
    store.dirty = true;
    store.hasUnsavedChanges = true;

    const sha = await store.saveVersion("note");
    expect(sha).toBe("abc123");
    expect(mocks.getActive).toHaveBeenCalledTimes(1);
    expect(mocks.saveVersion.mock.invocationCallOrder[0]).toBeLessThan(
      mocks.getActive.mock.invocationCallOrder[0],
    );
    expect(store.dirty).toBe(false);
    expect(store.hasUnsavedChanges).toBe(false);
    expect(mocks.getVersions).toHaveBeenCalled();
  });

  it("passes overwrite to the api, false unless asked", async () => {
    mocks.saveVersion.mockResolvedValue({ sha: "abc123" });
    mocks.getActive.mockResolvedValue({ project: PROJECT, dirty: false });
    const store = useProjectStore();
    store.activeProject = PROJECT;

    await store.saveVersion("m", true);
    expect(mocks.saveVersion).toHaveBeenLastCalledWith("m", true);
    await store.saveVersion("m");
    expect(mocks.saveVersion).toHaveBeenLastCalledWith("m", false);
  });

  it("keeps the server's dirty answer when nothing was committed", async () => {
    mocks.saveVersion.mockResolvedValue({ sha: null });
    mocks.getActive.mockResolvedValue({ project: PROJECT, dirty: true });
    const store = useProjectStore();
    store.activeProject = PROJECT;
    store.dirty = true;

    expect(await store.saveVersion("note")).toBeNull();
    expect(store.dirty).toBe(true);
    expect(store.status).toBe("unsaved");
  });
});

describe("updateSettings", () => {
  it("persists the toggle and reconciles status from disk", async () => {
    mocks.updateSettings.mockResolvedValue(false);
    mocks.getActive.mockResolvedValue({
      project: { ...PROJECT, track_data_artifacts: false },
      has_external_changes: false,
      dirty: true, // re-projection dropped artifact files → working tree dirty
    });
    const store = useProjectStore();
    store.activeProject = { ...PROJECT };

    await store.updateSettings(false);
    expect(mocks.updateSettings).toHaveBeenCalledWith(false);
    expect(store.activeProject?.track_data_artifacts).toBe(false);
    expect(store.status).toBe("unsaved");
  });
});

describe("loadVersions capability degradation", () => {
  it("hides history (and does not throw) when the route is unavailable", async () => {
    mocks.getVersions.mockRejectedValue(new mocks.ProjectFeatureUnavailable());
    const store = useProjectStore();
    store.activeProject = PROJECT;

    await store.loadVersions();
    expect(store.versionsAvailable).toBe(false);
    expect(store.error).toBeNull();
  });

  it("surfaces a non-capability failure on the error field", async () => {
    mocks.getVersions.mockRejectedValue(new Error("boom"));
    const store = useProjectStore();
    store.activeProject = PROJECT;

    await store.loadVersions();
    expect(store.versionsAvailable).toBe(true);
    expect(store.error).toBe("boom");
  });
});

const imported = (secrets: string[]) => ({
  imported: { flows: 1, connections: 0, schedules: 0 },
  placeholder_secrets: secrets,
});

const pullResult = (state: string, result: unknown = null) => ({
  state,
  sha: "abc",
  result,
  conflicts: [],
  warnings: [],
});

describe("branch switch, pull and merge", () => {
  const activeStore = () => {
    mocks.getActive.mockResolvedValue({ project: PROJECT, dirty: false });
    const store = useProjectStore();
    store.activeProject = PROJECT;
    return store;
  };

  const expectPostImport = () => {
    expect(mocks.setFlowId).toHaveBeenCalledWith(-1);
    expect(mocks.invalidateOverview).toHaveBeenCalled();
    expect(mocks.getActive).toHaveBeenCalled();
    expect(mocks.getVersions).toHaveBeenCalled();
  };

  it("a switch clears the open flow, keeps its secrets to fill and re-reads the project", async () => {
    mocks.switchBranch.mockResolvedValue(imported(["prod_pw"]));
    const store = activeStore();
    await store.switchBranch("feature", true);
    expect(mocks.switchBranch).toHaveBeenCalledWith("feature", true);
    expect(store.placeholderSecrets).toEqual(["prod_pw"]);
    expectPostImport();
  });

  it("a pull that applied an import runs the same path", async () => {
    mocks.pull.mockResolvedValue(pullResult("fast_forward", imported(["api_key"])));
    const store = activeStore();
    const result = await store.pull();
    expect(result.state).toBe("fast_forward");
    expect(store.placeholderSecrets).toEqual(["api_key"]);
    expectPostImport();
  });

  it("a pull stopped on conflicts, or with nothing to pull, leaves the open flow alone", async () => {
    const store = activeStore();
    store.placeholderSecrets = ["kept"];
    for (const state of ["conflicted", "up_to_date", "ahead"]) {
      mocks.pull.mockResolvedValueOnce(pullResult(state));
      await store.pull();
    }
    expect(mocks.setFlowId).not.toHaveBeenCalled();
    expect(mocks.invalidateOverview).not.toHaveBeenCalled();
    expect(store.placeholderSecrets).toEqual(["kept"]);
  });

  it("settling the last item, or concluding, applies the merge like a pull", async () => {
    const store = activeStore();
    mocks.settleMerge.mockResolvedValueOnce({ merge: { conflicts: [] }, pull: null });
    await store.settleMerge("flows/a.flow.yaml", "step:2", "ours");
    expect(mocks.setFlowId).not.toHaveBeenCalled();

    mocks.settleMerge.mockResolvedValueOnce({
      merge: null,
      pull: pullResult("merged", imported(["s1"])),
    });
    await store.settleMerge("flows/a.flow.yaml", "step:3", "theirs", true);
    expect(mocks.settleMerge).toHaveBeenLastCalledWith(
      "flows/a.flow.yaml",
      "step:3",
      "theirs",
      true,
    );
    expect(store.placeholderSecrets).toEqual(["s1"]);
    expectPostImport();

    vi.clearAllMocks();
    mocks.getVersions.mockResolvedValue([]);
    mocks.concludeMerge.mockResolvedValue(pullResult("merged", imported(["s2"])));
    await store.concludeMerge(true);
    expect(mocks.concludeMerge).toHaveBeenCalledWith(true);
    expect(store.placeholderSecrets).toEqual(["s2"]);
    expectPostImport();
  });

  it("remembers a flow opened from the merge until the merge ends", async () => {
    const store = activeStore();
    mocks.openMergeFlow.mockResolvedValue(42);
    expect(await store.openMergeFlow("flows/a.flow.yaml")).toBe(42);
    expect(store.isMergeFlow(42)).toBe(true);
    expect(store.isMergeFlow(7)).toBe(false);

    mocks.concludeMerge.mockResolvedValue(pullResult("merged", imported([])));
    await store.concludeMerge();
    expect(store.isMergeFlow(42)).toBe(false);
  });

  it("after the editor settled a flow, answers what is left of the pull", async () => {
    const store = activeStore();
    mocks.getMerge.mockResolvedValueOnce({ conflicts: [{}, {}] });
    expect(await store.afterEditorSettle()).toEqual({ state: "settle_rest", left: 2 });
    mocks.getMerge.mockResolvedValueOnce({ conflicts: [] });
    expect(await store.afterEditorSettle()).toEqual({ state: "conclude" });
    mocks.getMerge.mockResolvedValueOnce(null);
    expect(await store.afterEditorSettle()).toEqual({ state: "merged" });
    expect(mocks.invalidateOverview).toHaveBeenCalled();
  });
});
