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

  const authMethods = (databaseType?: string): string[] =>
    findDialect(databaseType)?.auth_methods ?? [];

  const isSavedConnectionOnly = (databaseType?: string): boolean =>
    findDialect(databaseType)?.saved_connection_only ?? false;

  const browsesDatabases = (databaseType?: string): boolean =>
    findDialect(databaseType)?.browses_databases === true;

  const quotedIdentifiers = (databaseType?: string): boolean =>
    findDialect(databaseType)?.quoted_identifiers === true;

  return {
    dialects,
    findDialect,
    isFileBased,
    defaultPort,
    authMethods,
    isSavedConnectionOnly,
    browsesDatabases,
    quotedIdentifiers,
  };
}
