// What the catalog reader and writer offer from the catalog tree, and what a choice writes into their settings.
import { SYSTEM_NAMESPACE_NAMES } from "../../../../types/catalog.types";
import type { CatalogTable, NamespaceTree } from "../../../../types/catalog.types";
import type { CatalogWriteSettings, NodeCatalogReader } from "../../../../types/node.types";
import { isHubRow } from "../../../../views/CatalogView/hubRows";

/** One schema in a picker. ``id`` is the tree's id (negative for a hub schema), ``hubId`` the hub's own. */
export interface NamespaceOption {
  id: number;
  label: string;
  full: string;
  source: "local" | "hub";
  hubId: number | null;
}

export type ReaderTarget = Pick<
  NodeCatalogReader,
  "catalog_table_id" | "catalog_namespace_id" | "hub_table_id" | "hub_namespace_id"
>;

export type WriterTarget = Pick<
  CatalogWriteSettings,
  "namespace_id" | "namespace_full_name" | "hub_namespace_id"
>;

type TableWrites = Pick<
  NodeCatalogReader,
  | "catalog_table_id"
  | "catalog_namespace_id"
  | "catalog_table_name"
  | "catalog_full_table_name"
  | "hub_table_id"
  | "hub_namespace_id"
>;

const isWriter = (settings: ReaderTarget | WriterTarget): settings is WriterTarget =>
  "namespace_id" in settings;

/**
 * One option per schema, the local catalogs first and the hub's after them. The system schemas under the local
 * ``General`` are left out unless ``includeSystemSchemas`` (the reader reads tables kept there).
 */
export function namespaceOptions(
  tree: NamespaceTree[],
  includeSystemSchemas = false,
): NamespaceOption[] {
  const options: NamespaceOption[] = [];
  const ordered = [...tree.filter((c) => !isHubRow(c)), ...tree.filter(isHubRow)];
  for (const catalog of ordered) {
    const hub = isHubRow(catalog);
    for (const schema of catalog.children ?? []) {
      if (
        !hub &&
        !includeSystemSchemas &&
        catalog.name === "General" &&
        SYSTEM_NAMESPACE_NAMES.has(schema.name)
      ) {
        continue;
      }
      options.push({
        id: schema.id,
        label: `${catalog.name} / ${schema.name}`,
        full: `${catalog.name}.${schema.name}`,
        source: hub ? "hub" : "local",
        hubId: hub ? (schema.hub_id ?? null) : null,
      });
    }
  }
  return options;
}

/** The reader's fields for a picked table: a hub table by the hub's ids only, a local one as before. */
export function tableChoice(table: CatalogTable | null): Partial<TableWrites> {
  if (!table) {
    return {
      catalog_table_id: null,
      catalog_table_name: null,
      catalog_full_table_name: null,
      hub_table_id: null,
    };
  }
  const names = {
    catalog_table_name: table.name,
    catalog_full_table_name: table.full_table_name ?? null,
  };
  if (isHubRow(table)) {
    return {
      ...names,
      hub_table_id: table.hub_id ?? null,
      hub_namespace_id: table.hub_namespace_id ?? null,
      catalog_table_id: null,
      catalog_namespace_id: null,
    };
  }
  return {
    ...names,
    catalog_table_id: table.id,
    catalog_namespace_id: table.namespace_id,
    hub_table_id: null,
    hub_namespace_id: null,
  };
}

/** The reader's namespace fields for a picked schema (the table list's filter). */
export function readerNamespaceChoice(
  option: NamespaceOption | null,
): Pick<ReaderTarget, "catalog_namespace_id" | "hub_namespace_id"> {
  if (option?.source === "hub")
    return { catalog_namespace_id: null, hub_namespace_id: option.hubId };
  return { catalog_namespace_id: option?.id ?? null, hub_namespace_id: null };
}

/** The writer's fields for a picked schema: a hub schema by the hub's id only, a local one as before. */
export function namespaceChoice(option: NamespaceOption | null): WriterTarget {
  if (option?.source === "hub") {
    return { namespace_id: null, namespace_full_name: null, hub_namespace_id: option.hubId };
  }
  return {
    namespace_id: option?.id ?? null,
    namespace_full_name: option?.full ?? null,
    hub_namespace_id: null,
  };
}

/** The option a saved reader or writer points at: by the hub's id, else the local id, else the full name. */
export function selectedNamespaceRow(
  settings: ReaderTarget | WriterTarget,
  options: NamespaceOption[],
): NamespaceOption | null {
  if (settings.hub_namespace_id != null) {
    return options.find((o) => o.source === "hub" && o.hubId === settings.hub_namespace_id) ?? null;
  }
  const localId = isWriter(settings) ? settings.namespace_id : settings.catalog_namespace_id;
  const local = options.filter((o) => o.source === "local");
  if (localId != null) {
    const byId = local.find((o) => o.id === localId);
    if (byId) return byId;
  }
  const full = isWriter(settings) ? settings.namespace_full_name : null;
  if (full) return local.find((o) => o.full === full) ?? null;
  return null;
}

/** The table a saved reader points at: by the hub's id, else the local id. */
export function selectedTableRow(
  settings: ReaderTarget,
  tables: CatalogTable[],
): CatalogTable | null {
  if (settings.hub_table_id != null) {
    return tables.find((t) => isHubRow(t) && t.hub_id === settings.hub_table_id) ?? null;
  }
  if (settings.catalog_table_id == null) return null;
  return tables.find((t) => !isHubRow(t) && t.id === settings.catalog_table_id) ?? null;
}

/** Whether a reader reads, or a writer writes, a table on the hub. */
export function isHubTarget(settings: ReaderTarget | WriterTarget): boolean {
  return isWriter(settings) ? settings.hub_namespace_id != null : settings.hub_table_id != null;
}

/** The value a picker's select binds for a row; negative for a hub row, never written to the settings. */
export const pickerRowId = (row: { id: number }): number => row.id;
