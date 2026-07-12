import { beforeEach, describe, expect, it, vi } from "vitest";
import { createPinia, setActivePinia } from "pinia";

import type { CommunityNodeSummary, InstalledAlert } from "../api/communityNodes";

// Stub the network wrappers but keep the real CommunityApiError / toCommunityError.
vi.mock("../api/communityNodes", async (importActual) => {
  const actual = await importActual<typeof import("../api/communityNodes")>();
  return {
    ...actual,
    fetchIndex: vi.fn(),
    fetchNode: vi.fn(),
    installNode: vi.fn(),
    uninstallNode: vi.fn(),
    fetchInstalled: vi.fn(),
    fetchUpdates: vi.fn(),
  };
});

// The palette-cache module pulls import.meta.glob + axios; stub the one symbol we call.
vi.mock("../composables/useNodes", () => ({
  clearNodeTemplatesCache: vi.fn(),
}));

import { CommunityApiError, fetchIndex, installNode, uninstallNode } from "../api/communityNodes";
import { clearNodeTemplatesCache } from "../composables/useNodes";
import { describeError, useCommunityNodesStore } from "./community-nodes-store";

const fetchIndexMock = vi.mocked(fetchIndex);
const installNodeMock = vi.mocked(installNode);
const uninstallNodeMock = vi.mocked(uninstallNode);
const clearCacheMock = vi.mocked(clearNodeTemplatesCache);

function makeNode(over: Partial<CommunityNodeSummary> = {}): CommunityNodeSummary {
  return {
    id: over.id ?? "node_a",
    node_name: over.node_name ?? "Node A",
    version: over.version ?? "1.0.0",
    description: over.description ?? "",
    author: over.author ?? { github: "alice", name: "", url: "" },
    maintainers: [],
    license: "MIT",
    category: over.category ?? "Transform",
    palette_category: "Custom",
    tags: over.tags ?? [],
    environment: over.environment ?? "local",
    dependencies: [],
    number_of_inputs: 1,
    number_of_outputs: 1,
    min_flowfile_version: over.min_flowfile_version ?? "",
    designer_editable: true,
    risk_flags: over.risk_flags ?? [],
    published_at: "",
    updated_at: over.updated_at ?? "",
    changelog: "",
    path: "",
    artifacts: {
      node: { path: "node.py", sha256: "abc", size: 1 },
      manifest: { path: "manifest.json", sha256: "def", size: 1 },
      icon: null,
      readme: null,
      screenshots: [],
    },
    popularity: over.popularity ?? null,
    install_state: over.install_state ?? "not_installed",
  };
}

function indexResponse(nodes: CommunityNodeSummary[], alerts: InstalledAlert[] = []) {
  return {
    fetched_at: "2026-01-01T00:00:00Z",
    source: "fixture",
    stale: false,
    categories: ["Transform", "Input"],
    repo_stars: 42,
    nodes,
    alerts,
  };
}

describe("community-nodes-store", () => {
  beforeEach(() => {
    setActivePinia(createPinia());
    vi.clearAllMocks();
  });

  describe("filtering", () => {
    it("searches over name, description and tags", () => {
      const store = useCommunityNodesStore();
      store.nodes = [
        makeNode({ id: "a", node_name: "Emoji Mapper", description: "adds emoji" }),
        makeNode({ id: "b", node_name: "CSV Reader", description: "reads csv", tags: ["io"] }),
        makeNode({ id: "c", node_name: "Joiner", description: "joins", tags: ["merge"] }),
      ];

      store.filters.search = "emoji";
      expect(store.filteredNodes.map((n) => n.id)).toEqual(["a"]);

      store.filters.search = "io";
      expect(store.filteredNodes.map((n) => n.id)).toEqual(["b"]);

      store.filters.search = "merge";
      expect(store.filteredNodes.map((n) => n.id)).toEqual(["c"]);
    });

    it("filters by category and by tags", () => {
      const store = useCommunityNodesStore();
      store.nodes = [
        makeNode({ id: "a", category: "Transform", tags: ["x"] }),
        makeNode({ id: "b", category: "Input", tags: ["x", "y"] }),
      ];

      store.filters.category = "Input";
      expect(store.filteredNodes.map((n) => n.id)).toEqual(["b"]);

      store.filters.category = "";
      store.filters.tags = ["y"];
      expect(store.filteredNodes.map((n) => n.id)).toEqual(["b"]);
    });
  });

  describe("sorting", () => {
    it("sorts by popularity desc with null-popularity last, name tiebreak", () => {
      const store = useCommunityNodesStore();
      store.nodes = [
        makeNode({ id: "none", node_name: "Zeta", popularity: null }),
        makeNode({
          id: "low",
          node_name: "Beta",
          popularity: { upvotes: 3, discussion_url: "" },
        }),
        makeNode({
          id: "hi",
          node_name: "Alpha",
          popularity: { upvotes: 9, discussion_url: "" },
        }),
      ];
      store.filters.sort = "popularity";
      expect(store.filteredNodes.map((n) => n.id)).toEqual(["hi", "low", "none"]);
    });

    it("sorts by name", () => {
      const store = useCommunityNodesStore();
      store.nodes = [
        makeNode({ id: "z", node_name: "Zed" }),
        makeNode({ id: "a", node_name: "Ace" }),
      ];
      store.filters.sort = "name";
      expect(store.filteredNodes.map((n) => n.id)).toEqual(["a", "z"]);
    });

    it("sorts by updated (newest first)", () => {
      const store = useCommunityNodesStore();
      store.nodes = [
        makeNode({ id: "old", updated_at: "2025-01-01" }),
        makeNode({ id: "new", updated_at: "2026-06-01" }),
      ];
      store.filters.sort = "updated";
      expect(store.filteredNodes.map((n) => n.id)).toEqual(["new", "old"]);
    });

    it("falls back to name when popularity is requested but absent everywhere", () => {
      const store = useCommunityNodesStore();
      store.nodes = [
        makeNode({ id: "z", node_name: "Zed" }),
        makeNode({ id: "a", node_name: "Ace" }),
      ];
      store.filters.sort = "popularity";
      expect(store.hasPopularity).toBe(false);
      expect(store.filteredNodes.map((n) => n.id)).toEqual(["a", "z"]);
    });
  });

  describe("hasPopularity gating", () => {
    it("is false when no node carries popularity", () => {
      const store = useCommunityNodesStore();
      store.nodes = [makeNode({ popularity: null })];
      expect(store.hasPopularity).toBe(false);
    });

    it("is true when any node carries popularity", () => {
      const store = useCommunityNodesStore();
      store.nodes = [
        makeNode({ id: "a", popularity: null }),
        makeNode({ id: "b", popularity: { upvotes: 1, discussion_url: "" } }),
      ];
      expect(store.hasPopularity).toBe(true);
    });
  });

  describe("installStateFor", () => {
    it("returns the state for a known node and null otherwise", () => {
      const store = useCommunityNodesStore();
      store.nodes = [makeNode({ id: "a", install_state: "installed" })];
      expect(store.installStateFor("a")).toBe("installed");
      expect(store.installStateFor("missing")).toBeNull();
    });
  });

  describe("install", () => {
    it("sends acknowledged_risks + acknowledged_capabilities, reloads, invalidates palette", async () => {
      const store = useCommunityNodesStore();
      const node = makeNode({ id: "a", version: "2.1.0", risk_flags: ["network"] });
      installNodeMock.mockResolvedValue({
        success: true,
        node: {},
        receipt: {} as never,
        load_error: null,
      });
      fetchIndexMock.mockResolvedValue(
        indexResponse([makeNode({ id: "a", install_state: "installed" })]),
      );

      await store.install(node, ["network"]);

      expect(installNodeMock).toHaveBeenCalledWith({
        node_id: "a",
        version: "2.1.0",
        acknowledged_risks: true,
        acknowledged_capabilities: ["network"],
      });
      // reload index (refresh=true) + palette cache invalidation are the post-install seam.
      expect(fetchIndexMock).toHaveBeenCalledWith(true);
      expect(clearCacheMock).toHaveBeenCalledTimes(1);
      expect(store.isInstalling("a")).toBe(false);
    });

    it("clears the installing flag even when install fails", async () => {
      const store = useCommunityNodesStore();
      installNodeMock.mockRejectedValue(new CommunityApiError(409, "NODE_NAME_COLLISION", "x"));
      await expect(store.install(makeNode({ id: "a" }), [])).rejects.toBeInstanceOf(
        CommunityApiError,
      );
      expect(store.isInstalling("a")).toBe(false);
    });
  });

  describe("uninstall", () => {
    it("calls the api, reloads and invalidates the palette", async () => {
      const store = useCommunityNodesStore();
      uninstallNodeMock.mockResolvedValue({ success: true, node_id: "a" });
      fetchIndexMock.mockResolvedValue(indexResponse([]));
      await store.uninstall("a");
      expect(uninstallNodeMock).toHaveBeenCalledWith("a");
      expect(fetchIndexMock).toHaveBeenCalledWith(false);
      expect(clearCacheMock).toHaveBeenCalledTimes(1);
    });
  });

  describe("loadIndex", () => {
    it("captures a CommunityApiError into store.error instead of throwing", async () => {
      const store = useCommunityNodesStore();
      fetchIndexMock.mockRejectedValue(new CommunityApiError(503, "COMMUNITY_UNAVAILABLE", "down"));
      await store.loadIndex(false);
      expect(store.error?.errorCode).toBe("COMMUNITY_UNAVAILABLE");
      expect(store.loading).toBe(false);
    });

    it("stores installed-node alerts from the index response", async () => {
      const store = useCommunityNodesStore();
      const alert: InstalledAlert = {
        node_id: "bad_node",
        kind: "blocked",
        reason: "malicious",
        severity: "",
        advisory_url: "https://example.com/adv",
      };
      fetchIndexMock.mockResolvedValue(indexResponse([], [alert]));
      await store.loadIndex(false);
      expect(store.alerts).toEqual([alert]);
    });
  });

  describe("describeError", () => {
    it("maps 503 COMMUNITY_UNAVAILABLE", () => {
      const msg = describeError(new CommunityApiError(503, "COMMUNITY_UNAVAILABLE", "x"));
      expect(msg).toMatch(/unavailable/i);
    });

    it("maps 409 collision and includes the holder file", () => {
      const msg = describeError(
        new CommunityApiError(409, "NODE_NAME_COLLISION", "x", { holderFile: "my_node.py" }),
      );
      expect(msg).toMatch(/already installed/i);
      expect(msg).toContain("my_node.py");
    });

    it("maps CONSENT_CAPABILITIES with the missing list", () => {
      const msg = describeError(
        new CommunityApiError(403, "CONSENT_CAPABILITIES", "x", {
          missing: ["network", "fs_write"],
        }),
      );
      expect(msg).toContain("network");
      expect(msg).toContain("fs_write");
    });

    it("maps YANKED and INCOMPATIBLE_VERSION", () => {
      expect(describeError(new CommunityApiError(410, "YANKED", "x"))).toMatch(/withdrawn/i);
      expect(
        describeError(
          new CommunityApiError(409, "INCOMPATIBLE_VERSION", "'n' requires Flowfile >= 9.9.9"),
        ),
      ).toContain("9.9.9");
    });

    it("falls back for non-registry errors", () => {
      expect(describeError(new Error("boom"))).toMatch(/failed/i);
    });
  });
});
