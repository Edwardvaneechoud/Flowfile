// Core's rules for changing a catalog's storage and deleting it; core checks both again.

import type { NamespaceTree } from "../../types/catalog.types";
import { CLOUD_URI_SCHEMES } from "../../utils/storagePath";

/** Why a storage URI and connection pair cannot be saved; null when it can (both blank means local). */
export function catalogStorageError(uri: string, connectionName: string | null): string | null {
  const trimmed = uri.trim();
  if (trimmed && !CLOUD_URI_SCHEMES.some((s) => trimmed.startsWith(s))) {
    return "Storage URI must start with s3://, gs://, abfss://, …";
  }
  if (trimmed && !connectionName) return "Select a storage connection for this URI";
  if (connectionName && !trimmed)
    return "Enter a storage URI (e.g. s3://bucket/path) for this connection";
  return null;
}

/** The physical tables stored in the catalog and its schemas; virtual tables store nothing. */
export function storedTableCount(catalog: NamespaceTree): number {
  return [catalog, ...catalog.children].reduce(
    (count, ns) => count + ns.tables.filter((t) => t.table_type === "physical").length,
    0,
  );
}

const plural = (n: number, word: string) => `${n} ${word}${n === 1 ? "" : "s"}`;

/** Why the catalog's storage cannot change any more; null while it holds no stored table. */
export function storageLockedReason(catalog: NamespaceTree): string | null {
  const tables = storedTableCount(catalog);
  if (tables === 0) return null;
  return `Its storage can't change: it holds ${plural(tables, "table")}. Create a new catalog to use other storage.`;
}

const CONTENT_KINDS = [
  ["flows", "flow"],
  ["tables", "table"],
  ["notebooks", "notebook"],
  ["visualizations", "visualization"],
  ["artifacts", "model"],
] as const;

/** What the namespaces hold, as "2 tables, 1 flow"; empty when they hold nothing but schemas. */
function heldBy(namespaces: NamespaceTree[]): string[] {
  return CONTENT_KINDS.map(
    ([key, word]) => [namespaces.reduce((n, ns) => n + ns[key].length, 0), word] as const,
  )
    .filter(([n]) => n > 0)
    .map(([n, word]) => plural(n, word));
}

/** Whether a schema holds nothing, so core deletes it and nothing that points at it is left behind. */
export function isEmptySchema(schema: NamespaceTree): boolean {
  return schema.children.length === 0 && heldBy([schema]).length === 0;
}

/**
 * Why the catalog cannot be deleted; null when neither it nor its schemas hold anything.
 * The page deletes its empty schemas first, since core deletes only an empty namespace.
 */
export function deleteBlockedReason(catalog: NamespaceTree): string | null {
  if (isSeededCatalog(catalog)) return SEEDED_CATALOG_REASON;
  const held = heldBy([catalog, ...catalog.children]);
  if (held.length === 0) return null;
  return `Only an empty catalog can be deleted; this one holds ${held.join(", ")}.`;
}

const SEEDED_CATALOG_NAME = "General";
const SEEDED_SCHEMA_NAMES = new Set([
  "default",
  "Unnamed Flows",
  "Local Flows",
  "Python Editor",
  "sync",
]);
const SEEDED_CATALOG_REASON = "The General catalog is created by Flowfile and cannot be deleted.";

/** Whether the catalog is the root catalog Flowfile seeds and saves into; core refuses to delete it. */
export function isSeededCatalog(catalog: NamespaceTree): boolean {
  return catalog.parent_id === null && catalog.name === SEEDED_CATALOG_NAME;
}

/** Whether the schema is one Flowfile keeps under the seeded root catalog; core refuses to delete it. */
export function isSeededSchema(schema: NamespaceTree, roots: NamespaceTree[]): boolean {
  if (!SEEDED_SCHEMA_NAMES.has(schema.name)) return false;
  return roots.some((root) => isSeededCatalog(root) && root.id === schema.parent_id);
}
