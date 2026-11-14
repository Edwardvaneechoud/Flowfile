import { computed, reactive, ref, watch, type Ref } from "vue";
import { ElMessage } from "element-plus";

import type { DatabaseSettings, DatabaseWriteSettings } from "../../../baseNode/nodeInput";
import type { FullDatabaseConnectionInterface } from "../../../../../views/DatabaseView/databaseConnectionTypes";
import { useDbDialects } from "../../../../../composables/useDbDialects";
import { fetchDbDatabases, fetchDbSchemas, fetchDbTables } from "./api";
import {
  connectionOptionLabel,
  databaseChanged,
  databasePlaceholder,
  missingSchemaError,
  missingSchemaNotice,
  shouldLoadTables,
  splitTableReference,
  tablesEmptyText,
} from "./databaseChoice";
import { createLatestLoader, type ListState } from "./latestLoader";
import { validateSqlIdentifier } from "../databaseWriter/validation";

/**
 * The Database, Schema and Table lists of the database reader and writer drawers, written once for both.
 *
 * The Database select shows in saved-connection mode for a dialect that browses databases (or
 * whenever the node already names a database, so it can be cleared). The node stores the pick in
 * `database_name`; empty means the connection's default database. Each list is filled only by its
 * latest load, so a slow answer for an earlier connection, database or schema never lands.
 */
export function useDatabaseChoice(options: {
  settings: () => DatabaseSettings | DatabaseWriteSettings | undefined;
  connections: Ref<FullDatabaseConnectionInterface[]>;
  // Drawer-side reset run when the database changes, before the lists reload.
  onDatabaseChange?: () => void;
}) {
  const { browsesDatabases, quotedIdentifiers } = useDbDialects();
  const databases = reactive<ListState<string>>({ options: [], loading: false });
  const schemas = reactive<ListState<string>>({ options: [], loading: false });
  const tables = reactive<ListState<string>>({ options: [], loading: false });
  const databaseLoader = createLatestLoader(databases);
  const schemaLoader = createLatestLoader(schemas);
  const tableLoader = createLatestLoader(tables);
  let previousDatabase: string | null | undefined;
  // Set once the user applied or validated with a database but no schema; reset by a new node or database.
  const attempted = ref(false);

  const selectedConnection = computed(() => {
    const name = options.settings()?.database_connection_name;
    return options.connections.value.find((c) => c.connectionName === name);
  });

  const connectionBrowses = computed(() =>
    browsesDatabases(selectedConnection.value?.databaseType),
  );

  // Databases are browsed only through a saved connection.
  const browsing = computed(
    () => options.settings()?.connection_mode === "reference" && connectionBrowses.value,
  );

  const showDatabase = computed(() => {
    const settings = options.settings();
    if (!settings || settings.connection_mode !== "reference") return false;
    return connectionBrowses.value || !!settings.database_name;
  });

  const placeholder = computed(() =>
    databases.loading ? "Loading..." : databasePlaceholder(selectedConnection.value?.database),
  );

  const schemaError = computed(() => {
    const settings = options.settings();
    return missingSchemaError(settings?.database_name, settings?.schema_name);
  });

  const schemaNotice = computed(() => {
    const settings = options.settings();
    return missingSchemaNotice(settings?.database_name, settings?.schema_name, attempted.value);
  });

  /** An apply or validate was tried: a missing schema now reads as an error, not a hint. */
  const markAttempted = () => {
    attempted.value = true;
  };

  // Names may hold $ or be double-quoted only on a saved connection whose dialect takes them (Snowflake).
  const quoted = computed(
    () =>
      options.settings()?.connection_mode === "reference" &&
      quotedIdentifiers(selectedConnection.value?.databaseType),
  );

  /** A database, schema or table name checked with the selected connection's dialect rule. */
  const identifierError = (value?: string | null): string | null =>
    validateSqlIdentifier(value, { quoted: quoted.value });

  const tablesNoDataText = computed(() =>
    tablesEmptyText(browsing.value, options.settings()?.schema_name),
  );

  const reportError = (silent: boolean, fallback: string) =>
    silent ? undefined : (error: any) => ElMessage.error(error?.response?.data?.detail || fallback);

  const fetchDatabases = async (silent = false) => {
    const settings = options.settings();
    if (!settings || !connectionBrowses.value) {
      databaseLoader.invalidate();
      return;
    }
    await databaseLoader.load(
      () => fetchDbDatabases(settings),
      reportError(silent, "Failed to fetch databases"),
    );
  };

  const loadSchemas = async (silent = false) => {
    const settings = options.settings();
    if (!settings) return;
    await schemaLoader.load(
      () => fetchDbSchemas(settings),
      reportError(silent, "Failed to fetch schemas"),
    );
  };

  const loadTables = async (silent = false) => {
    const settings = options.settings();
    if (!settings) return;
    if (!shouldLoadTables(browsing.value, settings.schema_name)) {
      tableLoader.invalidate();
      return;
    }
    await tableLoader.load(
      () => fetchDbTables(settings),
      reportError(silent, "Failed to fetch tables"),
    );
  };

  /** Empty the schema and table lists and drop any answer still on its way. */
  const clearLists = () => {
    schemaLoader.invalidate();
    tableLoader.invalidate();
  };

  /** Remember what the loaded node names, so the next change knows what it replaced. */
  const syncDatabase = () => {
    previousDatabase = options.settings()?.database_name;
    attempted.value = false;
  };

  const handleDatabaseChange = () => {
    const settings = options.settings();
    if (!settings) return;
    if (!settings.database_name) settings.database_name = null;
    if (databaseChanged(previousDatabase, settings.database_name)) {
      settings.schema_name = undefined;
      settings.table_name = undefined;
    }
    previousDatabase = settings.database_name;
    attempted.value = false;
    clearLists();
    options.onDatabaseChange?.();
    loadSchemas(true);
    loadTables(true);
  };

  /** A new schema makes any table list still loading for the old one stale. */
  const handleSchemaChange = () => {
    loadTables(true);
  };

  /**
   * Split a qualified table name into the node's fields; false when it is left as typed.
   * A `database.schema.table` sets the database too, for a dialect that browses databases.
   */
  const applyTableReference = (value: string): boolean => {
    const settings = options.settings();
    if (!settings || settings.schema_name) return false;
    const parts = splitTableReference(value, browsing.value);
    if (!parts) return false;
    if (parts.database) {
      settings.database_name = parts.database;
      previousDatabase = parts.database;
    }
    settings.schema_name = parts.schema;
    settings.table_name = parts.table;
    return true;
  };

  const optionLabel = (connection: FullDatabaseConnectionInterface): string =>
    connectionOptionLabel(connection, browsesDatabases(connection.databaseType));

  watch(
    () => [options.settings()?.database_connection_name, connectionBrowses.value] as const,
    ([name], previous) => {
      const settings = options.settings();
      const previousName = previous?.[0];
      // A database belongs to the connection it was picked on.
      if (settings && previousName && name !== previousName && settings.database_name) {
        settings.database_name = null;
        previousDatabase = null;
      }
      databaseLoader.invalidate();
      clearLists();
      if (!name) return;
      fetchDatabases(true);
      loadSchemas(true);
      loadTables(true);
    },
  );

  return {
    databases,
    schemas,
    tables,
    showDatabase,
    placeholder,
    schemaError,
    schemaNotice,
    markAttempted,
    identifierError,
    tablesNoDataText,
    fetchDatabases,
    loadSchemas,
    loadTables,
    clearLists,
    syncDatabase,
    handleDatabaseChange,
    handleSchemaChange,
    applyTableReference,
    optionLabel,
  };
}
