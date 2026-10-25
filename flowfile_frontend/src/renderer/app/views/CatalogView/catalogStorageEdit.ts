// Core's rules for changing a catalog's storage and deleting it; core checks both again.

import type { NamespaceTree } from "../../types/catalog.types";
import { CLOUD_URI_SCHEMES } from "../../utils/storagePath";

/** Why a storage URI and connection pair cannot be saved; null when it can (both blank is local storage). */
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

/** Why the catalog cannot be deleted; null when it is empty. */
export function deleteBlockedReason(catalog: NamespaceTree): string | null {
  const contents = [
    [catalog.children.length, "schema"],
    [catalog.flows.length, "flow"],
    [catalog.tables.length, "table"],
    [catalog.notebooks.length, "notebook"],
  ] as const;
  const held = contents.filter(([n]) => n > 0).map(([n, word]) => plural(n, word));
  if (held.length === 0) return null;
  return `Only an empty catalog can be deleted; this one holds ${held.join(", ")}.`;
}
