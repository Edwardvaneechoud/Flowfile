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
    saveVersion: vi.fn(),
    getVersions: vi.fn(),
    updateSettings: vi.fn(),
  };
});

vi.mock("../api/project.api", () => ({
  ProjectFeatureUnavailable: mocks.ProjectFeatureUnavailable,
  ProjectApi: {
    getActive: mocks.getActive,
    saveVersion: mocks.saveVersion,
    getVersions: mocks.getVersions,
    updateSettings: mocks.updateSettings,
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
    mocks.getActive.mockResolvedValue({ project: PROJECT, has_external_changes: false, dirty: false });
    const store = useProjectStore();
    await store.refreshActive();
    expect(store.isActive).toBe(true);
    expect(store.status).toBe("clean");
  });

  it("is 'unsaved' when the working tree is dirty", async () => {
    mocks.getActive.mockResolvedValue({ project: PROJECT, has_external_changes: false, dirty: true });
    const store = useProjectStore();
    await store.refreshActive();
    expect(store.status).toBe("unsaved");
  });

  it("prefers 'external' over 'unsaved'", async () => {
    mocks.getActive.mockResolvedValue({ project: PROJECT, has_external_changes: true, dirty: true });
    const store = useProjectStore();
    await store.refreshActive();
    expect(store.status).toBe("external");
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
});
