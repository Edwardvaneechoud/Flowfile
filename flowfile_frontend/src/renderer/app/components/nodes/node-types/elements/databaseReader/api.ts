import axios from "axios";
import type { DatabaseSettings, DatabaseWriteSettings } from "../../../baseNode/nodeInput";

/**
 * Build a DatabaseSettings-shaped payload for /db_databases, /db_schemas and /db_tables.
 * Works with both DatabaseSettings (reader) and DatabaseWriteSettings (writer).
 */
function buildBrowsePayload(
  settings: DatabaseSettings | DatabaseWriteSettings,
): Record<string, unknown> {
  const payload: Record<string, unknown> = {
    connection_mode: settings.connection_mode,
    database_connection: settings.database_connection,
    database_connection_name: settings.database_connection_name,
    database_name: settings.database_name || undefined,
    schema_name: settings.schema_name,
    query_mode: "table",
  };
  if (payload.connection_mode === "reference") {
    payload.database_connection = undefined;
  } else {
    payload.database_connection_name = undefined;
  }
  return payload;
}

/**
 * Fetch the databases a node may pick on a connection; empty for a dialect with one database.
 */
export const fetchDbDatabases = async (
  settings: DatabaseSettings | DatabaseWriteSettings,
): Promise<string[]> => {
  const payload = {
    ...buildBrowsePayload(settings),
    database_name: undefined,
    schema_name: undefined,
  };
  const response = await axios.post<string[]>("/db_databases", payload);
  return response.data;
};

/**
 * Fetch available schema names for a database connection (of its database_name when set).
 */
export const fetchDbSchemas = async (
  settings: DatabaseSettings | DatabaseWriteSettings,
): Promise<string[]> => {
  const response = await axios.post<string[]>("/db_schemas", buildBrowsePayload(settings));
  return response.data;
};

/**
 * Fetch available table names for a database connection and optional schema.
 */
export const fetchDbTables = async (
  settings: DatabaseSettings | DatabaseWriteSettings,
): Promise<string[]> => {
  const response = await axios.post<string[]>("/db_tables", buildBrowsePayload(settings));
  return response.data;
};
