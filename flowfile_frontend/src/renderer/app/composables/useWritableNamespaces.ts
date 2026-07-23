import { computed } from "vue";
import { useCatalogStore } from "../stores/catalog-store";
import { filterSelectableNamespaces } from "../types/catalog.types";
import type { NamespaceTree } from "../types/catalog.types";
import type { AccessInfo } from "../types/sharing.types";
import { useResourceSharing } from "./useResourceSharing";

interface HasAccess {
  access?: AccessInfo | null;
}

export interface SchemaOption {
  id: number;
  label: string;
}

// The backend stamps `access` only on directly-granted namespaces: a grant on
// the level-0 catalog leaves its child schemas unstamped, yet the grant's level
// governs their writability. Inherit the parent's stamp for unstamped children.
const withInheritedAccess = (schema: HasAccess, parent: HasAccess): HasAccess => ({
  access: schema.access ?? parent.access,
});

// Pure core (Pinia-free, unit-testable): flatten "catalog / schema" save-target
// options from a namespace tree, stripping system schemas and keeping only
// schemas the predicate deems writable. In unrestricted mode every node's
// `access` is null, so the predicate keeps everything and the list is unchanged.
export function collectWritableSchemaOptions(
  tree: NamespaceTree[],
  isWritable: (node: HasAccess) => boolean,
): SchemaOption[] {
  const items: SchemaOption[] = [];
  for (const cat of filterSelectableNamespaces(tree)) {
    for (const schema of cat.children) {
      if (isWritable(withInheritedAccess(schema, cat))) {
        items.push({ id: schema.id, label: `${cat.name} / ${schema.name}` });
      }
    }
  }
  return items;
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

  const pruneNonWritable = (tree: NamespaceTree[]) =>
    pruneNonWritableSchemas(tree, isWritableNamespace);

  return {
    isWritableNamespace,
    writableSchemaNamespaces,
    writableSchemaOptions,
    pruneNonWritable,
  };
}
