// The hub's namespace tree as the publish and open dialogs browse it: one flat list of rows indented by depth,
// fetched one level per expand. The steps over the rows are pure so the dialogs' logic is unit-tested without
// mounting; the composable binds them to the api.
import { computed, ref } from "vue";
import type { Ref } from "vue";
import { listHubNamespaces, type HubNamespace } from "../../api/hubFlows.api";
import { detailOf } from "../../utils/apiDetail";

export interface TreeRow extends HubNamespace {
  depth: number;
  expanded: boolean;
  /** Whether this row's children were fetched once; a later expand only toggles. */
  loaded: boolean;
  /** Unknown until a fetch said: then whether it returned rows. A row keeps its chevron while unknown. */
  hasChildren: boolean | null;
}

const rowOf = (namespace: HubNamespace, depth: number): TreeRow => ({
  ...namespace,
  depth,
  expanded: false,
  loaded: false,
  hasChildren: null,
});

/** The hub's top level, as rows at depth 0. */
export const rootRows = (namespaces: HubNamespace[]): TreeRow[] =>
  namespaces.map((namespace) => rowOf(namespace, 0));

/** The fetched children placed under their parent, which is now loaded and expanded. */
export function withChildren(
  rows: TreeRow[],
  parentId: number,
  children: HubNamespace[],
): TreeRow[] {
  const at = rows.findIndex((row) => row.id === parentId);
  if (at === -1) return rows;
  const parent = rows[at];
  const loaded = { ...parent, loaded: true, expanded: true, hasChildren: children.length > 0 };
  const below = children.map((child) => rowOf(child, parent.depth + 1));
  return [...rows.slice(0, at), loaded, ...below, ...rows.slice(at + 1)];
}

/** The row flipped between expanded and collapsed; its rows stay, `visibleRows` hides or shows them. */
export const toggled = (rows: TreeRow[], id: number): TreeRow[] =>
  rows.map((row) => (row.id === id ? { ...row, expanded: !row.expanded } : row));

/** The rows whose ancestors are all expanded, in the order they are listed. */
export function visibleRows(rows: TreeRow[]): TreeRow[] {
  const shown: TreeRow[] = [];
  let hiddenBelow: number | null = null;
  for (const row of rows) {
    if (hiddenBelow !== null && row.depth > hiddenBelow) continue;
    shown.push(row);
    hiddenBelow = row.expanded ? null : row.depth;
  }
  return shown;
}

export const chevronLabel = (row: TreeRow): string =>
  row.expanded ? `Hide namespaces in ${row.name}` : `Show namespaces in ${row.name}`;

/**
 * The tree one dialog browses: `load()` lists the hub's roots, `expand(id)` fetches a row's children the first
 * time and toggles it after, `select(id)` marks the chosen namespace (`null` for none). One fetch per expand,
 * nothing prefetched, nothing polled; a click on a row whose fetch is still pending does nothing.
 */
export function useHubNamespaceTree() {
  const rows: Ref<TreeRow[]> = ref([]);
  const visible = computed(() => visibleRows(rows.value));
  const selectedId = ref<number | null>(null);
  const loading = ref(false);
  const error = ref("");
  const pending = new Set<number>();

  const load = async () => {
    loading.value = true;
    error.value = "";
    try {
      rows.value = rootRows(await listHubNamespaces());
    } catch (err) {
      rows.value = [];
      error.value = detailOf(err, "Couldn't reach the hub.");
    } finally {
      loading.value = false;
    }
  };

  const expand = async (id: number) => {
    const row = rows.value.find((candidate) => candidate.id === id);
    if (!row) return;
    if (row.loaded) {
      rows.value = toggled(rows.value, id);
      return;
    }
    if (pending.has(id)) return;
    pending.add(id);
    error.value = "";
    try {
      rows.value = withChildren(rows.value, id, await listHubNamespaces(id));
    } catch (err) {
      error.value = detailOf(err, "Couldn't reach the hub.");
    } finally {
      pending.delete(id);
    }
  };

  const select = (id: number | null) => {
    selectedId.value = id;
  };

  return { rows, visible, selectedId, loading, error, load, expand, select };
}
