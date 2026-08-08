import { beforeEach, describe, expect, it, vi } from "vitest";
import { createPinia, setActivePinia } from "pinia";
import type { ArtifactVersionInfo, GlobalArtifact, NamespaceTree } from "../types";

const getArtifactVersions = vi.fn();
const deleteArtifactVersion = vi.fn();
const promoteArtifactVersion = vi.fn();
const pruneArtifactVersions = vi.fn();
const getNamespaceTree = vi.fn();
const getArtifactById = vi.fn();

vi.mock("../api/catalog.api", () => ({
  CatalogApi: {
    getArtifactById: (...args: unknown[]) => getArtifactById(...args),
    getArtifactVersions: (...args: unknown[]) => getArtifactVersions(...args),
    deleteArtifactVersion: (...args: unknown[]) => deleteArtifactVersion(...args),
    promoteArtifactVersion: (...args: unknown[]) => promoteArtifactVersion(...args),
    pruneArtifactVersions: (...args: unknown[]) => pruneArtifactVersions(...args),
    getNamespaceTree: (...args: unknown[]) => getNamespaceTree(...args),
  },
}));

import { useCatalogStore } from "./catalog-store";

const version = (v: number): ArtifactVersionInfo => ({
  id: 1000 + v,
  version: v,
  created_at: "2026-08-01T00:00:00",
  size_bytes: 10,
  sha256: `sha-${v}`,
  python_type: "xgboost.core.Booster",
  blob_exists: true,
});

const artifact = (id: number, v: number): GlobalArtifact =>
  ({
    id,
    name: "churn",
    version: v,
    namespace_id: 4,
    status: "active",
    blob_exists: true,
  }) as unknown as GlobalArtifact;

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

const treeWith = (a: GlobalArtifact): NamespaceTree[] =>
  [
    {
      id: 1,
      name: "General",
      children: [{ id: 4, name: "models", children: [], artifacts: [a] }],
      artifacts: [],
    },
  ] as unknown as NamespaceTree[];

describe("catalog-store artifact versions", () => {
  beforeEach(() => {
    setActivePinia(createPinia());
    vi.clearAllMocks();
  });

  it("loadArtifactVersions populates state and remembers the ref", async () => {
    getArtifactVersions.mockResolvedValue({
      all_versions: [version(42), version(41)],
      total_versions: 42,
    });
    const store = useCatalogStore();

    await store.loadArtifactVersions("General.models::churn", 4);

    expect(getArtifactVersions).toHaveBeenCalledWith("General.models::churn", 4, 25, 0);
    expect(store.artifactVersions.map((v) => v.version)).toEqual([42, 41]);
    expect(store.artifactVersionsTotal).toBe(42);
    expect(store.artifactVersionsRef).toBe("General.models::churn");
  });

  it("drops a superseded response so rows cannot land under another artifact", async () => {
    const slow = deferred<unknown>();
    getArtifactVersions
      .mockReturnValueOnce(slow.promise)
      .mockResolvedValueOnce({ all_versions: [version(2)], total_versions: 2 });
    const store = useCatalogStore();

    const first = store.loadArtifactVersions("General.models::alpha", 4);
    await store.loadArtifactVersions("General.models::beta", 5);
    slow.resolve({ all_versions: [version(99)], total_versions: 99 });
    await first;

    expect(store.artifactVersionsRef).toBe("General.models::beta");
    expect(store.artifactVersions.map((v) => v.version)).toEqual([2]);
    expect(store.artifactVersionsTotal).toBe(2);
    expect(store.loadingArtifactVersions).toBe(false);
  });

  it("ignores a superseded failure instead of blanking the current list", async () => {
    const slow = deferred<unknown>();
    getArtifactVersions
      .mockReturnValueOnce(slow.promise)
      .mockResolvedValueOnce({ all_versions: [version(2)], total_versions: 2 });
    const store = useCatalogStore();

    const first = store.loadArtifactVersions("General.models::alpha", 4);
    await store.loadArtifactVersions("General.models::beta", 5);
    slow.reject({ response: { data: { detail: "gone" } } });
    await first;

    expect(store.artifactVersions.map((v) => v.version)).toEqual([2]);
    expect(store.error).toBeNull();
  });

  it("requests the right offset for a later page", async () => {
    getArtifactVersions.mockResolvedValue({ all_versions: [], total_versions: 42 });
    const store = useCatalogStore();

    await store.loadArtifactVersions("churn", null, 2);

    expect(getArtifactVersions).toHaveBeenCalledWith("churn", null, 25, 25);
    expect(store.artifactVersionsPage).toBe(2);
  });

  it("clears the list and records the error when the fetch fails", async () => {
    getArtifactVersions.mockRejectedValue({ response: { data: { detail: "nope" } } });
    const store = useCatalogStore();
    store.artifactVersions = [version(1)];
    store.artifactVersionsTotal = 1;

    await store.loadArtifactVersions("churn", null);

    expect(store.artifactVersions).toEqual([]);
    expect(store.artifactVersionsTotal).toBe(0);
    expect(store.error).toBe("nope");
    expect(store.loadingArtifactVersions).toBe(false);
  });

  it("reloadArtifactVersions re-fetches the remembered ref", async () => {
    getArtifactVersions.mockResolvedValue({ all_versions: [], total_versions: 0 });
    const store = useCatalogStore();
    await store.loadArtifactVersions("General.models::churn", 4, 3);
    getArtifactVersions.mockClear();

    await store.reloadArtifactVersions();

    expect(getArtifactVersions).toHaveBeenCalledWith("General.models::churn", 4, 25, 50);
  });

  it("reloadArtifactVersions is a no-op when nothing is loaded", async () => {
    const store = useCatalogStore();

    await store.reloadArtifactVersions();

    expect(getArtifactVersions).not.toHaveBeenCalled();
  });

  it("clearArtifactSelection also drops the version list", async () => {
    getArtifactVersions.mockResolvedValue({ all_versions: [version(1)], total_versions: 1 });
    const store = useCatalogStore();
    await store.loadArtifactVersions("churn", null);

    store.clearArtifactSelection();

    expect(store.artifactVersions).toEqual([]);
    expect(store.artifactVersionsRef).toBeNull();
    expect(store.artifactVersionsPage).toBe(1);
  });

  it("deleteArtifactVersion forwards the force flag", async () => {
    deleteArtifactVersion.mockResolvedValue(undefined);
    const store = useCatalogStore();

    await store.deleteArtifactVersion(1003, true);

    expect(deleteArtifactVersion).toHaveBeenCalledWith(1003, true);
  });

  it("promoteArtifactVersion forwards the row id and returns the new row", async () => {
    promoteArtifactVersion.mockResolvedValue({ id: 1099, version: 43 });
    const store = useCatalogStore();

    const promoted = await store.promoteArtifactVersion(1003);

    expect(promoteArtifactVersion).toHaveBeenCalledWith(1003);
    expect(promoted).toEqual({ id: 1099, version: 43 });
  });

  it("pruneArtifactVersions uses the loaded ref and namespace", async () => {
    getArtifactVersions.mockResolvedValue({ all_versions: [], total_versions: 0 });
    pruneArtifactVersions.mockResolvedValue({ would_delete: [], freed_bytes: 0, deleted: 0 });
    const store = useCatalogStore();
    await store.loadArtifactVersions("General.models::churn", 4);

    await store.pruneArtifactVersions(5, true);

    expect(pruneArtifactVersions).toHaveBeenCalledWith("General.models::churn", 4, 5, true);
  });

  it("pruneArtifactVersions rejects when no artifact is loaded", async () => {
    const store = useCatalogStore();

    await expect(store.pruneArtifactVersions(5, true)).rejects.toThrow("No artifact selected");
  });

  it("selectArtifact resolves a non-latest row id onto its family", async () => {
    const latest = artifact(999, 43);
    getArtifactById.mockResolvedValue({ id: 500, name: "churn", namespace_id: 4 });
    const store = useCatalogStore();
    store.tree = treeWith(latest);

    await store.selectArtifact(500);

    expect(getArtifactById).toHaveBeenCalledWith(500);
    expect(store.selectedArtifactId).toBe(999);
    expect(store.selectedArtifact?.version).toBe(43);
  });

  it("selectArtifact skips the by-id lookup when the tree already holds the row", async () => {
    const store = useCatalogStore();
    store.tree = treeWith(artifact(999, 43));

    await store.selectArtifact(999);

    expect(getArtifactById).not.toHaveBeenCalled();
    expect(store.selectedArtifact?.id).toBe(999);
  });

  it("refreshSelectedArtifact re-resolves the family's latest row after a promote", async () => {
    const promoted = artifact(999, 43);
    getNamespaceTree.mockResolvedValue(treeWith(promoted));
    const store = useCatalogStore();
    store.selectedArtifact = artifact(500, 42);
    store.selectedArtifactId = 500;

    await store.refreshSelectedArtifact();

    expect(store.selectedArtifactId).toBe(999);
    expect(store.selectedArtifact?.version).toBe(43);
  });

  it("refreshSelectedArtifact clears the selection when the family is gone", async () => {
    getNamespaceTree.mockResolvedValue([]);
    const store = useCatalogStore();
    store.selectedArtifact = artifact(500, 42);
    store.selectedArtifactId = 500;

    await store.refreshSelectedArtifact();

    expect(store.selectedArtifact).toBeNull();
    expect(store.selectedArtifactId).toBeNull();
  });
});
