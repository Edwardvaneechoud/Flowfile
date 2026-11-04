// What the catalog reader and writer offer from the catalog tree, and what a choice writes into their settings.
import { SYSTEM_NAMESPACE_NAMES } from "../../../../types/catalog.types";
import type { CatalogTable, NamespaceTree } from "../../../../types/catalog.types";
import type { CatalogWriteSettings, NodeCatalogReader } from "../../../../types/node.types";
import type { AccessInfo } from "../../../../types/sharing.types";
import {
  byPlace,
  isHubRow,
  isOffered,
  unavailableReason,
  type PlaceGroup,
} from "../../../../views/CatalogView/hubRows";

/** The schema a hub's organisation writes to by default. */
export const HUB_DEFAULT_SCHEMA = "main.default";

/** Why the writer offers a schema the person may only read, greyed. */
export const READ_ONLY = "You can read this but not write to it";

/** One schema in a picker. ``id`` is the tree's id (negative for a hub schema), ``hubId`` the hub's own. */
export interface NamespaceOption {
  id: number;
  label: string;
  full: string;
  source: "local" | "hub";
  hubId: number | null;
  /** Where the schema's catalog lives, as core words it; null when core does not say (no hub beside it). */
  place: string | null;
  /** Why this Flowfile cannot use the schema's tables, as core words it; null when it can. */
  unavailableReason: string | null;
  /**
   * Whether the access the listing answers lets this person write here: owner or manage, or for a local schema no
   * access at all (unrestricted). A hub schema the hub stamped nothing on is not counted: the hub decides at run.
   */
  writable: boolean;
  /** Whether the listing says this person may read here and not write: a ``use`` grant. */
  readOnly: boolean;
}

const writableBy = (access: AccessInfo | null | undefined, hub: boolean): boolean =>
  access ? access.is_owner || access.access_level === "manage" : !hub;

const readOnlyBy = (access: AccessInfo | null | undefined): boolean =>
  !!access && !access.is_owner && access.access_level === "use";

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
 * ``General`` are left out unless ``includeSystemSchemas`` (the reader reads tables kept there), and so is every
 * catalog core does not offer (a joined server's own). A schema this machine cannot use is listed, marked with why,
 * so a node saved on it still finds its row; ``pickable`` decides what a picker shows.
 */
export function namespaceOptions(
  tree: NamespaceTree[],
  includeSystemSchemas = false,
): NamespaceOption[] {
  const options: NamespaceOption[] = [];
  const offered = tree.filter(isOffered);
  const ordered = [...offered.filter((c) => !isHubRow(c)), ...offered.filter(isHubRow)];
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
        place: catalog.place ?? null,
        unavailableReason: unavailableReason(schema) ?? unavailableReason(catalog),
        writable: writableBy(schema.access ?? catalog.access, hub),
        readOnly: readOnlyBy(schema.access ?? catalog.access),
      });
    }
  }
  return options;
}

/** What a picker shows: the schemas this machine can use, and the one a saved node already points at. */
export function pickable(options: NamespaceOption[], selectedId: number | null): NamespaceOption[] {
  return options.filter((o) => o.unavailableReason === null || o.id === selectedId);
}

/** Why the writer cannot write into a schema it lists, greyed: out of this machine's reach, or read only; else null. */
export function writeBlock(option: NamespaceOption): string | null {
  return option.unavailableReason ?? (option.readOnly ? READ_ONLY : null);
}

/** A picker's rows under the place each lives in, in the order given. */
export function placeGroups<T>(rows: T[], placeOf: (row: T) => string | null): PlaceGroup<T>[] {
  return byPlace(rows.map((row) => ({ row, place: placeOf(row) }))).map((group) => ({
    place: group.place,
    rows: group.rows.map((entry) => entry.row),
  }));
}

/** A table's place: that of the options of its source, since one machine and one hub each have one. */
export function tablePlace(table: CatalogTable, options: NamespaceOption[]): string | null {
  const source = isHubRow(table) ? "hub" : "local";
  return options.find((o) => o.source === source)?.place ?? null;
}

/** The tree ids of the namespaces whose tables this machine cannot use, catalogs and schemas alike. */
export function unusableNamespaceIds(tree: NamespaceTree[]): Set<number> {
  const ids = new Set<number>();
  for (const catalog of tree) {
    const catalogReason = unavailableReason(catalog);
    for (const ns of [catalog, ...(catalog.children ?? [])]) {
      if (catalogReason !== null || unavailableReason(ns) !== null) ids.add(ns.id);
    }
  }
  return ids;
}

/**
 * The hub's default schema: ``main.default`` when this machine can use it and the person may write it, else the
 * first hub schema that holds for, else none.
 */
export function hubDefaultSchema(options: NamespaceOption[]): NamespaceOption | null {
  const usable = options.filter(
    (o) => o.source === "hub" && o.unavailableReason === null && o.writable,
  );
  return usable.find((o) => o.full === HUB_DEFAULT_SCHEMA) ?? usable[0] ?? null;
}

/**
 * What a new writer preselects. Nothing while this machine's own schemas are offered (a desktop, as before); where
 * only a hub's are (a server joined to it), the hub's default schema.
 */
export function writerDefault(options: NamespaceOption[]): NamespaceOption | null {
  if (options.some((o) => o.source === "local")) return null;
  return hubDefaultSchema(options);
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
