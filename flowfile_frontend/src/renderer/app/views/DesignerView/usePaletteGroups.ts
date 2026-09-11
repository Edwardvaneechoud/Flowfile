// Palette grouping: built-in groups first in a fixed order, then dynamic
// custom-category groups (sorted alphabetically) driven by NodeTemplate.node_group
// + node_group_label. Grouping key is always node_group; a custom node whose slug
// matches a built-in group lands there. Extracted from NodeList.vue so the
// hardcoded CategoryKey union dies and the logic is unit-testable.
import { ref, computed, type Ref, type ComputedRef } from "vue";
import type { NodeTemplate } from "../../types";

const OPEN_STATE_STORAGE_KEY = "nodeList.groupOpenState";
const FAVORITES_STORAGE_KEY = "nodeList.favorites";
const HIDDEN_STORAGE_KEY = "nodeList.hiddenNodes";

// Synthetic groups wrapping the regular palette: favorites on top, hidden at the
// bottom. Namespaced so a custom node's free-form node_group slug can't collide.
export const FAVORITES_GROUP_KEY = "__favorites__";
export const HIDDEN_GROUP_KEY = "__hidden__";

// Fixed built-in order + today's labels (identical to the old CategoryKey map).
export const BUILTIN_GROUPS: { key: string; label: string }[] = [
  { key: "input", label: "Input Sources" },
  { key: "transform", label: "Transformations" },
  { key: "combine", label: "Combine Operations" },
  { key: "aggregate", label: "Aggregations" },
  { key: "ml", label: "Machine Learning" },
  { key: "output", label: "Output Operations" },
  { key: "custom", label: "User Defined Operations" },
];

const BUILTIN_KEYS = new Set(BUILTIN_GROUPS.map((g) => g.key));

export interface PaletteGroup {
  key: string;
  label: string;
  nodes: NodeTemplate[];
  // Dynamic groups are user-defined custom categories (not one of the fixed built-ins).
  isDynamic: boolean;
}

function titleCaseSlug(slug: string): string {
  return slug
    .split(/[_-]+/)
    .filter(Boolean)
    .map((word) => word.charAt(0).toUpperCase() + word.slice(1))
    .join(" ");
}

// Prefer the backend-supplied display label, else title-case the slug.
function labelFor(node: NodeTemplate | undefined, key: string): string {
  const label = node?.node_group_label;
  if (label && label.trim()) return label.trim();
  return titleCaseSlug(key);
}

export interface PaletteUserPrefs {
  favorites?: ReadonlySet<string>;
  hidden?: ReadonlySet<string>;
}

/**
 * Build the ordered palette groups from a flat node list.
 * Built-ins come first (fixed order, today's labels, only if non-empty), then
 * dynamic custom groups sorted alphabetically by display label.
 *
 * With user prefs, favorited nodes are additionally listed in a leading
 * "Favorites" group (they stay in their own group too) and hidden nodes are
 * pulled out of every regular group into a trailing "Hidden nodes" group.
 * Both synthetic groups only appear when non-empty; prefs are keyed by
 * `NodeTemplate.item`.
 */
export function buildPaletteGroups(
  nodes: NodeTemplate[],
  prefs: PaletteUserPrefs = {},
): PaletteGroup[] {
  const hiddenNodes = prefs.hidden?.size ? nodes.filter((n) => prefs.hidden!.has(n.item)) : [];
  const visible = hiddenNodes.length ? nodes.filter((n) => !prefs.hidden!.has(n.item)) : nodes;

  const byKey = new Map<string, NodeTemplate[]>();
  for (const node of visible) {
    const key = node.node_group || "custom";
    const bucket = byKey.get(key);
    if (bucket) bucket.push(node);
    else byKey.set(key, [node]);
  }

  const groups: PaletteGroup[] = [];

  for (const { key, label } of BUILTIN_GROUPS) {
    const groupNodes = byKey.get(key);
    if (groupNodes && groupNodes.length) {
      groups.push({ key, label, nodes: groupNodes, isDynamic: false });
    }
  }

  const dynamicKeys = [...byKey.keys()].filter((key) => !BUILTIN_KEYS.has(key));
  const dynamic = dynamicKeys.map((key) => {
    const groupNodes = byKey.get(key)!;
    return {
      key,
      label: labelFor(groupNodes[0], key),
      nodes: groupNodes,
      isDynamic: true,
    };
  });
  dynamic.sort((a, b) => a.label.localeCompare(b.label));

  const regular = [...groups, ...dynamic];
  const result: PaletteGroup[] = [];

  // Favorites keep palette order so the group reads the same as the list below it.
  const favorites = prefs.favorites?.size
    ? regular.flatMap((g) => g.nodes).filter((n) => prefs.favorites!.has(n.item))
    : [];
  if (favorites.length) {
    result.push({
      key: FAVORITES_GROUP_KEY,
      label: "Favorites",
      nodes: favorites,
      isDynamic: false,
    });
  }
  result.push(...regular);
  if (hiddenNodes.length) {
    result.push({
      key: HIDDEN_GROUP_KEY,
      label: "Hidden nodes",
      nodes: hiddenNodes,
      isDynamic: false,
    });
  }
  return result;
}

function filterNode(node: NodeTemplate, query: string): boolean {
  return (
    node.name.toLowerCase().includes(query) ||
    (node.tags ?? []).some((tag) => tag.toLowerCase().includes(query))
  );
}

function loadOpenState(): Record<string, boolean> {
  try {
    const raw = localStorage.getItem(OPEN_STATE_STORAGE_KEY);
    if (!raw) return {};
    const parsed = JSON.parse(raw);
    return parsed && typeof parsed === "object" ? parsed : {};
  } catch {
    return {};
  }
}

function persistOpenState(state: Record<string, boolean>): void {
  try {
    localStorage.setItem(OPEN_STATE_STORAGE_KEY, JSON.stringify(state));
  } catch {
    // localStorage may be unavailable (private mode); open-state is best-effort.
  }
}

function loadItemSet(key: string): Set<string> {
  try {
    const raw = localStorage.getItem(key);
    const parsed = raw ? JSON.parse(raw) : [];
    return new Set(Array.isArray(parsed) ? parsed.filter((v) => typeof v === "string") : []);
  } catch {
    return new Set();
  }
}

function persistItemSet(key: string, items: ReadonlySet<string>): void {
  try {
    localStorage.setItem(key, JSON.stringify([...items]));
  } catch {
    // Best-effort, same as open-state.
  }
}

export function usePaletteGroups(nodes: Ref<NodeTemplate[]>) {
  const searchQuery = ref("");
  // Per-group open/closed, keyed by group key. New groups default open.
  const openState = ref<Record<string, boolean>>(loadOpenState());
  // Per-node favorite / hidden flags keyed by NodeTemplate.item, persisted locally.
  const favorites = ref<Set<string>>(loadItemSet(FAVORITES_STORAGE_KEY));
  const hidden = ref<Set<string>>(loadItemSet(HIDDEN_STORAGE_KEY));

  const allGroups = computed(() =>
    buildPaletteGroups(nodes.value, { favorites: favorites.value, hidden: hidden.value }),
  );

  // Search filters nodes within each group; empty groups drop out.
  const filteredGroups: ComputedRef<PaletteGroup[]> = computed(() => {
    const query = searchQuery.value.trim().toLowerCase();
    if (!query) return allGroups.value;
    return allGroups.value
      .map((group) => ({ ...group, nodes: group.nodes.filter((n) => filterNode(n, query)) }))
      .filter((group) => group.nodes.length > 0);
  });

  // Every group defaults open except the hidden bucket, which stays out of the way.
  const defaultOpen = (key: string): boolean => key !== HIDDEN_GROUP_KEY;

  const isGroupOpen = (key: string): boolean => {
    // While searching, matching groups are force-expanded.
    if (searchQuery.value.trim()) return true;
    return openState.value[key] ?? defaultOpen(key);
  };

  const toggleGroup = (key: string): void => {
    if (searchQuery.value.trim()) return;
    const next = { ...openState.value, [key]: !(openState.value[key] ?? defaultOpen(key)) };
    openState.value = next;
    persistOpenState(next);
  };

  const isFavorite = (item: string): boolean => favorites.value.has(item);
  const isHidden = (item: string): boolean => hidden.value.has(item);

  const setFavorites = (next: Set<string>): void => {
    favorites.value = next;
    persistItemSet(FAVORITES_STORAGE_KEY, next);
  };

  const setHidden = (next: Set<string>): void => {
    hidden.value = next;
    persistItemSet(HIDDEN_STORAGE_KEY, next);
  };

  // Favoriting a hidden node unhides it; hiding a favorite drops the favorite.
  const toggleFavorite = (item: string): void => {
    const next = new Set(favorites.value);
    if (next.has(item)) next.delete(item);
    else {
      next.add(item);
      if (hidden.value.has(item)) {
        const nextHidden = new Set(hidden.value);
        nextHidden.delete(item);
        setHidden(nextHidden);
      }
    }
    setFavorites(next);
  };

  const toggleHidden = (item: string): void => {
    const next = new Set(hidden.value);
    if (next.has(item)) next.delete(item);
    else {
      next.add(item);
      if (favorites.value.has(item)) {
        const nextFavorites = new Set(favorites.value);
        nextFavorites.delete(item);
        setFavorites(nextFavorites);
      }
    }
    setHidden(next);
  };

  return {
    searchQuery,
    filteredGroups,
    isGroupOpen,
    toggleGroup,
    isFavorite,
    isHidden,
    toggleFavorite,
    toggleHidden,
  };
}
