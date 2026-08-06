//flowfile_frontend/src/renderer/app/pages/databaseManager/api.ts

import axios from "axios";
import type {
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
    database_type: connection.databaseType as "postgresql",
    username: connection.username,
    password: connection.password,
    host: connection.host,
    port: connection.port,
    database: connection.database,
    ssl_enabled: connection.sslEnabled,
    url: connection.url,
    extra_params: connection.extraParams,
    auth_method: connection.authMethod,
    // undefined (dropped from JSON) rather than "": an empty string would create
    // empty key-secret rows server-side; blank means "no key" / "keep existing".
    private_key: connection.privateKey || undefined,
    private_key_passphrase: connection.privateKeyPassphrase || undefined,
    oauth_client_id: connection.oauthClientId || undefined,
    oauth_client_secret: connection.oauthClientSecret || undefined,
    oauth_authorize_endpoint: connection.oauthAuthorizeEndpoint || undefined,
    oauth_token_endpoint: connection.oauthTokenEndpoint || undefined,
    oauth_redirect_uri: connection.oauthRedirectUri || undefined,
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
    extraParams: pythonConnectionInterface.extra_params,
    authMethod: pythonConnectionInterface.auth_method ?? undefined,
    oauthClientId: pythonConnectionInterface.oauth_client_id ?? undefined,
    oauthAuthorizeEndpoint: pythonConnectionInterface.oauth_authorize_endpoint ?? undefined,
    oauthTokenEndpoint: pythonConnectionInterface.oauth_token_endpoint ?? undefined,
    oauthRedirectUri: pythonConnectionInterface.oauth_redirect_uri ?? undefined,
    oauthConnected: pythonConnectionInterface.oauth_connected ?? false,
    id: pythonConnectionInterface.id,
    access: pythonConnectionInterface.access,
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
    database: dbConnectionInterface.database,
    ssl_enabled: dbConnectionInterface.sslEnabled,
    url: dbConnectionInterface.url,
    extra_params: dbConnectionInterface.extraParams,
    auth_method: dbConnectionInterface.authMethod,
    oauth_client_id: dbConnectionInterface.oauthClientId,
    oauth_authorize_endpoint: dbConnectionInterface.oauthAuthorizeEndpoint,
    oauth_token_endpoint: dbConnectionInterface.oauthTokenEndpoint,
    oauth_redirect_uri: dbConnectionInterface.oauthRedirectUri,
    oauth_connected: dbConnectionInterface.oauthConnected,
  };
};

/**
 * Starts the OAuth sign-in flow for a stored connection; returns the IdP
 * authorize URL to open in a popup / the system browser.
 */
export const startDbOauthApi = async (connectionName: string): Promise<string> => {
  const response = await axios.get<{ auth_url: string }>(`${API_BASE_URL}/oauth/start`, {
    params: { connection_name: connectionName },
  });
  return response.data.auth_url;
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
    const errorMsg =
      (error as any).response?.data?.detail || "Failed to create database connection";
    throw new Error(errorMsg);
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
    const errorMsg =
      (error as any).response?.data?.detail || "Failed to update database connection";
    throw new Error(errorMsg);
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
 * Tests a database connection via the API.
 * Note: Add this function if your backend provides a connection testing endpoint
 * @param connectionData - The database connection configuration to test.
 * @returns A promise that resolves to a success/failure message.
 */
export const testDatabaseConnectionApi = async (
  connectionData: FullDatabaseConnection,
): Promise<{ success: boolean; message: string }> => {
  try {
    const pythonFormattedData = toPythonFormat(connectionData);
    const response = await axios.post<{ success: boolean; message: string }>(
      `${API_BASE_URL}/test`,
      pythonFormattedData,
    );
    return response.data;
  } catch (error) {
    console.error("API Error: Failed to test database connection:", error);
    const errorMsg = (error as any).response?.data?.detail || "Connection test failed";
    return { success: false, message: errorMsg };
  }
};
