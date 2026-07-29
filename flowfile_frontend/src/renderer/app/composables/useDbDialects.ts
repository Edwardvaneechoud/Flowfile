import { ref } from "vue";

import { DbDialectInfo, FALLBACK_DIALECTS, getDbDialects } from "../api/dbDialects";

const dialects = ref<DbDialectInfo[]>(FALLBACK_DIALECTS);
let fetchStarted = false;

/**
 * Shared, backend-driven database dialect catalog (GET /db_dialects).
 * Renders from the fallback list until the fetch resolves, so dialect
 * dropdowns work even against an older core or while offline.
 */
export function useDbDialects() {
  if (!fetchStarted) {
    fetchStarted = true;
    getDbDialects().then((fetched) => {
      dialects.value = fetched;
    });
  }

  const findDialect = (databaseType?: string): DbDialectInfo | undefined =>
    dialects.value.find((dialect) => dialect.name === (databaseType || "").toLowerCase());

  const isFileBased = (databaseType?: string): boolean =>
    findDialect(databaseType)?.file_based ?? false;

  const defaultPort = (databaseType?: string): number | undefined =>
    findDialect(databaseType)?.default_port ?? undefined;

  return { dialects, findDialect, isFileBased, defaultPort };
}
