import { computed, ref, watch, type Ref } from "vue";
import { ElMessage } from "element-plus";

import type { DatabaseSettings, DatabaseWriteSettings } from "../../../baseNode/nodeInput";
import type { FullDatabaseConnectionInterface } from "../../../../../views/DatabaseView/databaseConnectionTypes";
import { useDbDialects } from "../../../../../composables/useDbDialects";
import { fetchDbDatabases } from "./api";
import {
  connectionOptionLabel,
  databaseChanged,
  databasePlaceholder,
  missingSchemaError,
  splitTableReference,
} from "./databaseChoice";

/**
 * The Database select of the database reader and writer drawers, written once for both.
 *
 * Shown in saved-connection mode for a dialect that browses databases (or whenever the node already
 * names a database, so it can be cleared). The node stores the pick in `database_name`; empty means
 * the connection's default database.
 */
export function useDatabaseChoice(options: {
  settings: () => DatabaseSettings | DatabaseWriteSettings | undefined;
  connections: Ref<FullDatabaseConnectionInterface[]>;
  // Reloads the drawer's schema and table lists after the database changed.
  reload: () => void;
}) {
  const { browsesDatabases } = useDbDialects();
  const availableDatabases = ref<string[]>([]);
  const databasesAreLoading = ref(false);
  let previousDatabase: string | null | undefined;
  let fetchSeq = 0;

  const selectedConnection = computed(() => {
    const name = options.settings()?.database_connection_name;
    return options.connections.value.find((c) => c.connectionName === name);
  });

  const connectionBrowses = computed(() =>
    browsesDatabases(selectedConnection.value?.databaseType),
  );

  const showDatabase = computed(() => {
    const settings = options.settings();
    if (!settings || settings.connection_mode !== "reference") return false;
    return connectionBrowses.value || !!settings.database_name;
  });

  const placeholder = computed(() =>
    databasesAreLoading.value
      ? "Loading..."
      : databasePlaceholder(selectedConnection.value?.database),
  );

  const schemaError = computed(() => {
    const settings = options.settings();
    return missingSchemaError(settings?.database_name, settings?.schema_name);
  });

  const fetchDatabases = async (silent = false) => {
    const settings = options.settings();
    if (!settings || !connectionBrowses.value) {
      availableDatabases.value = [];
      return;
    }
    const seq = ++fetchSeq;
    databasesAreLoading.value = true;
    try {
      const databases = await fetchDbDatabases(settings);
      if (seq === fetchSeq) availableDatabases.value = databases;
    } catch (error: any) {
      if (!silent && seq === fetchSeq) {
        ElMessage.error(error.response?.data?.detail || "Failed to fetch databases");
      }
    } finally {
      if (seq === fetchSeq) databasesAreLoading.value = false;
    }
  };

  /** Remember what the loaded node names, so the next change knows what it replaced. */
  const syncDatabase = () => {
    previousDatabase = options.settings()?.database_name;
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
    options.reload();
  };

  /**
   * Split a qualified table name into the node's fields; false when it is left as typed.
   * A `database.schema.table` sets the database too, for a dialect that browses databases.
   */
  const applyTableReference = (value: string): boolean => {
    const settings = options.settings();
    if (!settings || settings.schema_name) return false;
    const parts = splitTableReference(value, showDatabase.value && connectionBrowses.value);
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
      availableDatabases.value = [];
      if (name) fetchDatabases(true);
    },
  );

  return {
    availableDatabases,
    databasesAreLoading,
    showDatabase,
    placeholder,
    schemaError,
    fetchDatabases,
    syncDatabase,
    handleDatabaseChange,
    applyTableReference,
    optionLabel,
  };
}
