// Browse/install state for the community-node registry. The install action is
// consent-gated: it is only ever called after CommunityConsentDialog resolves,
// and it forwards the acknowledged capability set the server re-verifies.
import { computed, reactive, ref } from "vue";
import { defineStore } from "pinia";
import {
  CommunityApiError,
  fetchIndex,
  fetchNode,
  installNode,
  uninstallNode,
  type CommunityNodeDetail,
  type CommunityNodeSummary,
  type InstalledAlert,
  type InstallResponse,
  type InstallState,
} from "../api/communityNodes";
import { clearNodeTemplatesCache } from "../composables/useNodes";

export type CommunitySort = "popularity" | "name" | "updated";

export interface CommunityFilters {
  search: string;
  category: string;
  tags: string[];
  sort: CommunitySort;
}

/** Human-readable message for any registry failure, keyed on the router's
 *  error_code so the panel/dialog can surface something actionable. */
export function describeError(e: unknown): string {
  if (e instanceof CommunityApiError) {
    switch (e.errorCode) {
      case "COMMUNITY_UNAVAILABLE":
        return "The community registry is unavailable and no cached copy is stored yet.";
      case "CONSENT_REQUIRED":
        return "You must acknowledge the risks before installing this node.";
      case "CONSENT_CAPABILITIES":
        return `Install needs acknowledgement of: ${(e.missing ?? []).join(", ")}.`;
      case "NODE_NAME_COLLISION":
        return `A node with the same name is already installed${
          e.holderFile ? ` (${e.holderFile})` : ""
        }.`;
      case "BLOCKED":
        return "This node has been blocked by the registry and cannot be installed.";
      case "YANKED":
        return "This version has been withdrawn by the registry and cannot be installed.";
      case "INCOMPATIBLE_VERSION":
        return e.message || "This node requires a newer version of Flowfile.";
      case "PIN_MISMATCH":
        return "Downloaded content did not match its pinned checksum; install aborted.";
      case "SCAN_REJECTED":
        return `The security scan rejected this node${
          e.ruleIds?.length ? ` (${e.ruleIds.join(", ")})` : ""
        }.`;
      case "NODE_NOT_FOUND":
        return "This node is no longer in the registry.";
      default:
        return e.message || "Community registry request failed.";
    }
  }
  return "Community registry request failed.";
}

export const useCommunityNodesStore = defineStore("communityNodes", () => {
  const nodes = ref<CommunityNodeSummary[]>([]);
  const categories = ref<string[]>([]);
  const alerts = ref<InstalledAlert[]>([]);
  const repoStars = ref(0);
  const fetchedAt = ref("");
  const stale = ref(false);
  const loading = ref(false);
  const error = ref<CommunityApiError | null>(null);
  const installing = reactive(new Set<string>());
  const loaded = ref(false);

  const filters = reactive<CommunityFilters>({
    search: "",
    category: "",
    tags: [],
    sort: "popularity",
  });

  const hasPopularity = computed(() => nodes.value.some((n) => n.popularity !== null));

  const allTags = computed(() => {
    const set = new Set<string>();
    for (const n of nodes.value) for (const t of n.tags) set.add(t);
    return [...set].sort();
  });

  function matchesSearch(node: CommunityNodeSummary, q: string): boolean {
    if (!q) return true;
    const haystack = [node.node_name, node.description, ...node.tags].join(" ").toLowerCase();
    return haystack.includes(q);
  }

  const filteredNodes = computed<CommunityNodeSummary[]>(() => {
    const q = filters.search.trim().toLowerCase();
    let list = nodes.value.filter((n) => matchesSearch(n, q));
    if (filters.category) list = list.filter((n) => n.category === filters.category);
    if (filters.tags.length) {
      list = list.filter((n) => filters.tags.every((t) => n.tags.includes(t)));
    }

    // popularity sort degrades to name when nothing carries popularity data.
    const sort = filters.sort === "popularity" && !hasPopularity.value ? "name" : filters.sort;
    const sorted = [...list];
    if (sort === "name") {
      sorted.sort((a, b) => a.node_name.localeCompare(b.node_name));
    } else if (sort === "updated") {
      sorted.sort((a, b) => (b.updated_at || "").localeCompare(a.updated_at || ""));
    } else {
      // popularity desc, null popularity last, name as the tiebreaker.
      sorted.sort((a, b) => {
        const ta = a.popularity?.upvotes ?? -1;
        const tb = b.popularity?.upvotes ?? -1;
        if (ta !== tb) return tb - ta;
        return a.node_name.localeCompare(b.node_name);
      });
    }
    return sorted;
  });

  function installStateFor(nodeId: string): InstallState | null {
    return nodes.value.find((n) => n.id === nodeId)?.install_state ?? null;
  }

  function isInstalling(nodeId: string): boolean {
    return installing.has(nodeId);
  }

  async function loadIndex(refresh = false): Promise<void> {
    loading.value = true;
    error.value = null;
    try {
      const res = await fetchIndex(refresh);
      nodes.value = res.nodes;
      categories.value = res.categories;
      alerts.value = res.alerts ?? [];
      repoStars.value = res.repo_stars;
      fetchedAt.value = res.fetched_at;
      stale.value = res.stale;
      loaded.value = true;
    } catch (e) {
      error.value = e instanceof CommunityApiError ? e : null;
      if (!(e instanceof CommunityApiError)) throw e;
    } finally {
      loading.value = false;
    }
  }

  function loadDetail(nodeId: string): Promise<CommunityNodeDetail> {
    return fetchNode(nodeId);
  }

  // Consent-gated: acknowledgedCapabilities is the node's risk_flags the user
  // confirmed in the dialog; the server re-scans and rejects any it did not.
  async function install(
    node: CommunityNodeSummary,
    acknowledgedCapabilities: string[],
  ): Promise<InstallResponse> {
    installing.add(node.id);
    try {
      const res = await installNode({
        node_id: node.id,
        version: node.version,
        acknowledged_risks: true,
        acknowledged_capabilities: acknowledgedCapabilities,
      });
      await loadIndex(true);
      // New node type is now on disk; drop the palette cache so the flow editor
      // picks it up on its next fetch (clean seam in composables/useNodes.ts).
      clearNodeTemplatesCache();
      return res;
    } finally {
      installing.delete(node.id);
    }
  }

  async function uninstall(nodeId: string): Promise<void> {
    installing.add(nodeId);
    try {
      await uninstallNode(nodeId);
      await loadIndex(false);
      clearNodeTemplatesCache();
    } finally {
      installing.delete(nodeId);
    }
  }

  function clearFilters(): void {
    filters.search = "";
    filters.category = "";
    filters.tags = [];
  }

  return {
    nodes,
    categories,
    alerts,
    repoStars,
    fetchedAt,
    stale,
    loading,
    error,
    installing,
    loaded,
    filters,
    hasPopularity,
    allTags,
    filteredNodes,
    installStateFor,
    isInstalling,
    loadIndex,
    loadDetail,
    install,
    uninstall,
    clearFilters,
  };
});
