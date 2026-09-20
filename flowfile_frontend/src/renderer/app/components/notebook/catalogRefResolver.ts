// Metadata only (never a read, never a collect), memoized per ref key so lookups stay synchronous.
import { CatalogApi } from "@/api/catalog.api";
import { useCatalogStore } from "@/stores/catalog-store";
import type { CatalogTable, NamespaceTree } from "@/types/catalog.types";
import {
  catalogRefKey,
  type CatalogRefLiteral,
  type SchemaColumn,
} from "../nodes/node-types/elements/pythonScript/dataframeSchemaTypes";

interface CacheEntry {
  columns: SchemaColumn[] | null;
  at: number;
}

// A table created after a failed lookup should become visible without a reload.
const NEGATIVE_TTL_MS = 30_000;

const cache = new Map<string, CacheEntry>();
const inFlight = new Map<string, Promise<void>>();

/** `undefined` = never asked (or an expired negative); `null` = unresolved; array = resolved. */
export function getCatalogRefColumns(key: string): SchemaColumn[] | null | undefined {
  const entry = cache.get(key);
  if (!entry) return undefined;
  if (entry.columns === null && Date.now() - entry.at >= NEGATIVE_TTL_MS) {
    cache.delete(key);
    return undefined;
  }
  return entry.columns;
}

export async function resolveCatalogRefs(refs: CatalogRefLiteral[]): Promise<void> {
  const pending: Promise<void>[] = [];
  for (const ref of refs) {
    const key = catalogRefKey(ref);
    if (getCatalogRefColumns(key) !== undefined) continue;
    let task = inFlight.get(key);
    if (!task) {
      task = resolveOne(key, ref).finally(() => {
        if (inFlight.get(key) === task) inFlight.delete(key);
      });
      inFlight.set(key, task);
    }
    pending.push(task);
  }
  await Promise.all(pending);
}

/** Test seam. */
export function resetCatalogRefCache(): void {
  cache.clear();
  inFlight.clear();
}

async function resolveOne(key: string, ref: CatalogRefLiteral): Promise<void> {
  let table: CatalogTable | null = null;
  if (ref.namespaceId != null) {
    table = await CatalogApi.resolveTableStrict(ref.table, ref.namespaceId);
  } else if (ref.catalog && ref.schema) {
    const namespaceId = findSchemaNamespaceId(ref.catalog, ref.schema);
    table =
      namespaceId != null
        ? await CatalogApi.resolveTableStrict(ref.table, namespaceId)
        : await CatalogApi.resolveTableStrict(`${ref.schema}.${ref.table}`);
  } else if (ref.schema) {
    table = await CatalogApi.resolveTableStrict(`${ref.schema}.${ref.table}`);
  } else {
    table = await CatalogApi.resolveTableStrict(ref.table);
  }
  const columns = table
    ? (table.schema_columns ?? []).map((c) => ({ name: c.name, dtype: c.dtype }))
    : null;
  cache.set(key, { columns, at: Date.now() });
}

function findSchemaNamespaceId(catalog: string, schema: string): number | null {
  let tree: NamespaceTree[] = [];
  try {
    tree = useCatalogStore().tree ?? [];
  } catch {
    // No active Pinia (completions can run outside a component) — fall back to the 2-part form.
    return null;
  }
  const catalogNode = tree.find((n) => n.name === catalog);
  const schemaNode = catalogNode?.children?.find((c) => c.name === schema);
  return schemaNode?.id ?? null;
}
