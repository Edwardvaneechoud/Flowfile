import { computed } from "vue";
import { useCatalogStore } from "../stores/catalog-store";
import { filterSelectableNamespaces } from "../types/catalog.types";
import type { NamespaceTree } from "../types/catalog.types";
import type { AccessInfo } from "../types/sharing.types";
import { byPlace, isHubRow, isOffered, unavailableReason } from "../views/CatalogView/hubRows";
import { useResourceSharing } from "./useResourceSharing";

interface HasAccess {
  access?: AccessInfo | null;
  source?: string | null;
}

export interface SchemaOption {
  id: number;
  label: string;
}

/** A schema a notebook, a chart or a dashboard can be saved in: this machine's own, or a hub's. */
export interface SaveTargetOption extends SchemaOption {
  hub: boolean;
  // Where the schema lives, as core words it; null when no hub's catalogs are listed beside this machine's.
  place: string | null;
}

export interface SaveTargetGroup {
  label: string | null;
  options: SaveTargetOption[];
}

// The backend stamps `access` only on directly-granted namespaces: a grant on
// the level-0 catalog leaves its child schemas unstamped, yet the grant's level
// governs their writability. Inherit the parent's stamp for unstamped children.
const withInheritedAccess = (schema: HasAccess, parent: HasAccess): HasAccess => ({
  access: schema.access ?? parent.access,
  source: parent.source,
});

// Pure core (Pinia-free, unit-testable): flatten "catalog / schema" save-target
// options from a namespace tree, stripping system schemas and keeping only
// schemas the predicate deems writable. In unrestricted mode every node's
// `access` is null, so the predicate keeps everything and the list is unchanged.
// This machine's schemas only, and none core marks not offered (a server that joined a hub).
export function collectWritableSchemaOptions(
  tree: NamespaceTree[],
  isWritable: (node: HasAccess) => boolean,
): SchemaOption[] {
  const items: SchemaOption[] = [];
  for (const cat of filterSelectableNamespaces(tree)) {
    if (!isOffered(cat)) continue;
    for (const schema of cat.children) {
      if (isWritable(withInheritedAccess(schema, cat))) {
        items.push({ id: schema.id, label: `${cat.name} / ${schema.name}` });
      }
    }
  }
  return items;
}

/**
 * Where a notebook, a chart or a dashboard can be saved: this machine's schemas first, then the schemas of a hub's
 * catalog the person may write there (the hub's Manage), marked as the hub's. Flows, tables and query views are not
 * saved into a hub schema this way, so their lists stay `collectWritableSchemaOptions`.
 */
export function collectSaveTargetOptions(
  tree: NamespaceTree[],
  isWritable: (node: HasAccess) => boolean,
): SaveTargetOption[] {
  const placeOf = (id: number) => tree.find((cat) => cat.children.some((s) => s.id === id))?.place;
  const items: SaveTargetOption[] = collectWritableSchemaOptions(tree, isWritable).map((o) => ({
    ...o,
    hub: false,
    place: placeOf(o.id) ?? null,
  }));
  for (const cat of tree) {
    if (!isHubRow(cat) || !isOffered(cat) || unavailableReason(cat) !== null) continue;
    for (const schema of cat.children) {
      if (!isWritable(withInheritedAccess(schema, cat))) continue;
      items.push({
        id: schema.id,
        label: `${cat.name} / ${schema.name}`,
        hub: true,
        place: cat.place ?? null,
      });
    }
  }
  return items;
}

/** Save targets under a heading per place; the hub's are always headed as the hub's, this machine's only beside them. */
export function groupSaveTargets(options: SaveTargetOption[]): SaveTargetGroup[] {
  const besideHub = options.some((o) => o.hub);
  return byPlace(options).map((group) => {
    const hub = group.rows[0]?.hub ?? false;
    const label = hub ? (group.place ? `${group.place} (hub)` : "Hub") : group.place;
    return { label: hub || besideHub ? label : null, options: group.rows };
  });
}

// Pure core: shallow copy of the tree with non-writable schemas pruned, for
// pickers that render their own hierarchy from a self-fetched tree.
export function pruneNonWritableSchemas(
  tree: NamespaceTree[],
  isWritable: (node: HasAccess) => boolean,
): NamespaceTree[] {
  return tree.map((node) => ({
    ...node,
    children: node.children.filter((c) => isWritable(withInheritedAccess(c, node))),
  }));
}

// Save-target writability for catalog namespaces: a `use`-level grant is
// read-only (the backend refuses creates with 403), so those schemas must not
// be offered as save targets. Owner/manage/public/unrestricted stay writable.
export function useWritableNamespaces() {
  const catalogStore = useCatalogStore();
  const { canManage } = useResourceSharing();

  const isWritableNamespace = (node: HasAccess) => canManage(node);

  const writableSchemaNamespaces = computed<SchemaOption[]>(() =>
    collectWritableSchemaOptions(catalogStore.tree, isWritableNamespace),
  );

  const writableSchemaOptions = (tree: NamespaceTree[]) =>
    collectWritableSchemaOptions(tree, isWritableNamespace);

  const saveTargetNamespaces = computed<SaveTargetOption[]>(() =>
    collectSaveTargetOptions(catalogStore.tree, isWritableNamespace),
  );

  const pruneNonWritable = (tree: NamespaceTree[]) =>
    pruneNonWritableSchemas(tree, isWritableNamespace);

  return {
    isWritableNamespace,
    writableSchemaNamespaces,
    writableSchemaOptions,
    saveTargetNamespaces,
    pruneNonWritable,
  };
}
