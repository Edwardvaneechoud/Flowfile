// Palette grouping: built-in groups first in a fixed order, then dynamic
// custom-category groups (sorted alphabetically) driven by NodeTemplate.node_group
// + node_group_label. Grouping key is always node_group; a custom node whose slug
// matches a built-in group lands there. Extracted from NodeList.vue so the
// hardcoded CategoryKey union dies and the logic is unit-testable.
import { ref, computed, type Ref, type ComputedRef } from "vue";
import type { NodeTemplate } from "../../types";

const OPEN_STATE_STORAGE_KEY = "nodeList.groupOpenState";

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

/**
 * Build the ordered palette groups from a flat node list.
 * Built-ins come first (fixed order, today's labels, only if non-empty), then
 * dynamic custom groups sorted alphabetically by display label.
 */
export function buildPaletteGroups(nodes: NodeTemplate[]): PaletteGroup[] {
  const byKey = new Map<string, NodeTemplate[]>();
  for (const node of nodes) {
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

  return [...groups, ...dynamic];
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

export function usePaletteGroups(nodes: Ref<NodeTemplate[]>) {
  const searchQuery = ref("");
  // Per-group open/closed, keyed by group key. New groups default open.
  const openState = ref<Record<string, boolean>>(loadOpenState());

  const allGroups = computed(() => buildPaletteGroups(nodes.value));

  // Search filters nodes within each group; empty groups drop out.
  const filteredGroups: ComputedRef<PaletteGroup[]> = computed(() => {
    const query = searchQuery.value.trim().toLowerCase();
    if (!query) return allGroups.value;
    return allGroups.value
      .map((group) => ({ ...group, nodes: group.nodes.filter((n) => filterNode(n, query)) }))
      .filter((group) => group.nodes.length > 0);
  });

  const isGroupOpen = (key: string): boolean => {
    // While searching, matching groups are force-expanded.
    if (searchQuery.value.trim()) return true;
    return openState.value[key] ?? true;
  };

  const toggleGroup = (key: string): void => {
    if (searchQuery.value.trim()) return;
    const next = { ...openState.value, [key]: !(openState.value[key] ?? true) };
    openState.value = next;
    persistOpenState(next);
  };

  return {
    searchQuery,
    allGroups,
    filteredGroups,
    isGroupOpen,
    toggleGroup,
  };
}
