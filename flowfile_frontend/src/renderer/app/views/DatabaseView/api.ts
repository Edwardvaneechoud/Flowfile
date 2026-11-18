//flowfile_frontend/src/renderer/app/views/DatabaseView/api.ts

import axios from "axios";
import { detailOf } from "../../utils/apiDetail";
import type {
  DatabaseConnectionTestResult,
  FullDatabaseConnection,
  FullDatabaseConnectionInterface,
  PythonFullDatabaseConnection,
  PythonFullDatabaseConnectionInterface,
} from "./databaseConnectionTypes";

const API_BASE_URL = "/db_connection_lib";

/**
 * Converts JavaScript camelCase to Python snake_case for API requests
 */
const toPythonFormat = (connection: FullDatabaseConnection): PythonFullDatabaseConnection => {
  return {
    connection_name: connection.connectionName,
    database_type: connection.databaseType,
    username: connection.username,
    password: connection.password,
    host: connection.host,
    port: connection.port,
    database: connection.database,
    ssl_enabled: connection.sslEnabled,
    url: connection.url,
    auth_method: connection.authMethod ?? null,
    role: connection.role ?? null,
    warehouse: connection.warehouse ?? null,
    private_key_passphrase: connection.privateKeyPassphrase ?? null,
    oauth_flow_id: connection.oauthFlowId ?? null,
  };
};

/**
 * Fetches the list of database connections from the API.
 * @returns A promise that resolves to an array of FullDatabaseConnectionInterface objects.
 */
export const fetchDatabaseConnectionsInterfaces = async (): Promise<
  FullDatabaseConnectionInterface[]
> => {
  try {
    const response = await axios.get<PythonFullDatabaseConnectionInterface[]>(API_BASE_URL);
    return response.data.map(convertConnectionInterfacePytoTs);
  } catch (error) {
    console.error("API Error: Failed to load database connections:", error);
    throw error;
  }
};

export const convertConnectionInterfacePytoTs = (
  pythonConnectionInterface: PythonFullDatabaseConnectionInterface,
): FullDatabaseConnectionInterface => {
  return {
    username: pythonConnectionInterface.username,
    connectionName: pythonConnectionInterface.connection_name,
    databaseType: pythonConnectionInterface.database_type,
    host: pythonConnectionInterface.host,
    port: pythonConnectionInterface.port,
    sslEnabled: pythonConnectionInterface.ssl_enabled,
    url: pythonConnectionInterface.url,
    database: pythonConnectionInterface.database,
    id: pythonConnectionInterface.id,
    access: pythonConnectionInterface.access,
    source: pythonConnectionInterface.source ?? "local",
    authMethod: pythonConnectionInterface.auth_method ?? null,
    role: pythonConnectionInterface.role ?? null,
    warehouse: pythonConnectionInterface.warehouse ?? null,
  };
};

export const convertConnectionInterfaceTstoPy = (
  dbConnectionInterface: FullDatabaseConnectionInterface,
): PythonFullDatabaseConnectionInterface => {
  return {
    username: dbConnectionInterface.username,
    connection_name: dbConnectionInterface.connectionName,
    database_type: dbConnectionInterface.databaseType,
    host: dbConnectionInterface.host,
    port: dbConnectionInterface.port,
    ssl_enabled: dbConnectionInterface.sslEnabled,
    url: dbConnectionInterface.url,
    auth_method: dbConnectionInterface.authMethod ?? null,
    role: dbConnectionInterface.role ?? null,
    warehouse: dbConnectionInterface.warehouse ?? null,
  };
};

/**
 * Creates a new database connection via the API.
 * @param connectionData - The database connection configuration to add.
 * @returns A promise that resolves when the connection is created.
 */
export const createDatabaseConnectionApi = async (
  connectionData: FullDatabaseConnection,
): Promise<void> => {
  try {
    const pythonFormattedData = toPythonFormat(connectionData);
    await axios.post(API_BASE_URL, pythonFormattedData);
  } catch (error) {
    console.error("API Error: Failed to create database connection:", error);
    throw new Error(detailOf(error, "Failed to create database connection"));
  }
};

/**
 * Updates an existing database connection via the API.
 * @param connectionData - The database connection configuration to update.
 * @returns A promise that resolves when the connection is updated.
 */
export const updateDatabaseConnectionApi = async (
  connectionData: FullDatabaseConnection,
): Promise<void> => {
  try {
    const pythonFormattedData = toPythonFormat(connectionData);
    await axios.put(API_BASE_URL, pythonFormattedData);
  } catch (error) {
    console.error("API Error: Failed to update database connection:", error);
    throw new Error(detailOf(error, "Failed to update database connection"));
  }
};

/**
 * Deletes a database connection via the API.
 * @param connectionName - The name of the connection to delete.
 * @returns A promise that resolves when the connection is deleted.
 */
export const deleteDatabaseConnectionApi = async (connectionName: string): Promise<void> => {
  try {
    await axios.delete(`${API_BASE_URL}?connection_name=${encodeURIComponent(connectionName)}`);
  } catch (error) {
    console.error("API Error: Failed to delete database connection:", error);
    throw error;
  }
};

/**
 * Tests a saved connection. A failed test is still HTTP 200 with `success: false`;
 * a request error (unknown connection, server down) is reported the same way.
 */
export const testDatabaseConnectionApi = async (
  connectionName: string,
): Promise<DatabaseConnectionTestResult> => {
  try {
    const response = await axios.post<DatabaseConnectionTestResult>(`${API_BASE_URL}/test`, {
      connection_name: connectionName,
    });
    return { ...response.data, details: response.data.details ?? null };
  } catch (error) {
    console.error("API Error: Failed to test database connection:", error);
    return { success: false, message: detailOf(error, "Connection test failed"), details: null };
  }
};
