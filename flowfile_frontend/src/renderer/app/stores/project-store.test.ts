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
    setSecrets: vi.fn(),
    close: vi.fn(),
    warning: vi.fn(),
    setFlowId: vi.fn(),
  };
});

vi.mock("element-plus", () => ({
  ElMessage: { warning: mocks.warning },
}));

// Mock the flow store: the real module pulls in the `../api` barrel (axios.config),
// which reaches DOM globals at import time and would crash under the node env.
vi.mock("./flow-store", () => ({
  useFlowStore: () => ({ setFlowId: mocks.setFlowId }),
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
    setSecrets: mocks.setSecrets,
    close: mocks.close,
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
  it("captures the projection_failed flag", async () => {
    mocks.getActive.mockResolvedValue({ project: PROJECT, projection_failed: true });
    const store = useProjectStore();
    await store.refreshActive();
    expect(store.projectionFailed).toBe(true);
  });

  it("clears all status when no project is active", async () => {
    mocks.getActive.mockResolvedValue({ project: null });
    const store = useProjectStore();
    store.hasExternalChanges = true;
    store.projectionFailed = true;
    store.dirty = true;
    await store.refreshActive();
    expect(store.activeProject).toBeNull();
    expect(store.hasExternalChanges).toBe(false);
    expect(store.projectionFailed).toBe(false);
    expect(store.dirty).toBe(false);
  });

  it("surfaces a fetch failure on the error field without throwing", async () => {
    mocks.getActive.mockRejectedValue(new Error("network down"));
    const store = useProjectStore();
    await store.refreshActive();
    expect(store.error).toBe("network down");
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
    store.projectionFailed = true;

    const project = await store.initProject("/p", "Demo");
    expect(project).toEqual(PROJECT);
    expect(store.activeProject).toEqual(PROJECT);
    expect(store.status).toBe("clean");
    expect(store.projectionFailed).toBe(false);
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
  it("warns on prune errors and refreshes status", async () => {
    mocks.restore.mockResolvedValue({
      imported: { flows: 0, connections: 0, schedules: 0 },
      placeholder_secrets: [],
      prune_errors: ["table x"],
    });
    mocks.getActive.mockResolvedValue({ project: PROJECT, dirty: false });
    const store = useProjectStore();
    store.activeProject = PROJECT;

    await store.restoreVersion("deadbeef", "rollback");
    expect(mocks.restore).toHaveBeenCalledWith("deadbeef", "rollback");
    expect(mocks.warning).toHaveBeenCalled();
    expect(mocks.getActive).toHaveBeenCalled();
    expect(mocks.setFlowId).toHaveBeenCalledWith(-1); // canvas reset; no stale tab
  });
});

describe("reloadFromDisk", () => {
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
  });

  it("hides the reload action when the route is unavailable", async () => {
    mocks.reload.mockRejectedValue(new mocks.ProjectFeatureUnavailable());
    const store = useProjectStore();
    store.activeProject = PROJECT;

    await expect(store.reloadFromDisk()).rejects.toBeInstanceOf(mocks.ProjectFeatureUnavailable);
    expect(store.reloadAvailable).toBe(false);
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
  it("clears unsaved state and refreshes history", async () => {
    mocks.saveVersion.mockResolvedValue({ sha: "abc123" });
    const store = useProjectStore();
    store.activeProject = PROJECT;
    store.dirty = true;
    store.hasUnsavedChanges = true;

    const sha = await store.saveVersion("note");
    expect(sha).toBe("abc123");
    expect(store.dirty).toBe(false);
    expect(store.hasUnsavedChanges).toBe(false);
    expect(mocks.getVersions).toHaveBeenCalled();
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
