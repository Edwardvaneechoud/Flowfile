//flowfile_frontend/src/renderer/app/pages/databaseManager/api.ts

import axios from 'axios';
import type { 
  FullDatabaseConnection, 
  FullDatabaseConnectionInterface,
  PythonFullDatabaseConnection,
  PythonFullDatabaseConnectionInterface
} from './databaseConnectionTypes';

const API_BASE_URL = '/db_connection_lib';

/**
 * Converts JavaScript camelCase to Python snake_case for API requests
 */
const toPythonFormat = (connection: FullDatabaseConnection): PythonFullDatabaseConnection => {
  return {
    connection_name: connection.connectionName,
    database_type: connection.databaseType as 'postgresql',
    username: connection.username,
    password: connection.password,
    host: connection.host,
    port: connection.port,
    database: connection.database,
    ssl_enabled: connection.sslEnabled,
    url: connection.url
  };
};

/**
 * Fetches the list of database connections from the API.
 * @returns A promise that resolves to an array of FullDatabaseConnectionInterface objects.
 */
export const fetchDatabaseConnectionsInterfaces = async (): Promise<FullDatabaseConnectionInterface[]> => {
  try {
    const response = await axios.get<PythonFullDatabaseConnectionInterface[]>(API_BASE_URL);
    return response.data.map(convertConnectionInterfacePytoTs);
  } catch (error) {
    console.error('API Error: Failed to load database connections:', error);
    throw error;
  }
};

export const convertConnectionInterfacePytoTs = ( pythonConnectionInterface: PythonFullDatabaseConnectionInterface ): FullDatabaseConnectionInterface => {
    return {
        username: pythonConnectionInterface.username,
        connectionName: pythonConnectionInterface.connection_name,
        databaseType: pythonConnectionInterface.database_type,
        host: pythonConnectionInterface.host,
        port: pythonConnectionInterface.port,
        sslEnabled: pythonConnectionInterface.ssl_enabled,
        url: pythonConnectionInterface.url,        
        database: pythonConnectionInterface.database
    }
}

export const convertConnectionInterfaceTstoPy = ( dbConnectionInterface: FullDatabaseConnectionInterface ): PythonFullDatabaseConnectionInterface => {
    return {
        username: dbConnectionInterface.username,
        connection_name: dbConnectionInterface.connectionName,
        database_type: dbConnectionInterface.databaseType,
        host: dbConnectionInterface.host,
        port: dbConnectionInterface.port,
        ssl_enabled: dbConnectionInterface.sslEnabled,
        url: dbConnectionInterface.url,
    }
}


/**
 * Creates a new database connection via the API.
 * @param connectionData - The database connection configuration to add.
 * @returns A promise that resolves when the connection is created.
 */
export const createDatabaseConnectionApi = async (connectionData: FullDatabaseConnection): Promise<void> => {
  try {
    const pythonFormattedData = toPythonFormat(connectionData);
    await axios.post(API_BASE_URL, pythonFormattedData);
  } catch (error) {
    console.error('API Error: Failed to create database connection:', error);
    const errorMsg = (error as any).response?.data?.detail || 'Failed to create database connection';
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
    console.error('API Error: Failed to delete database connection:', error);
    throw error;
  }
};

/**
 * Tests a database connection via the API.
 * Note: Add this function if your backend provides a connection testing endpoint
 * @param connectionData - The database connection configuration to test.
 * @returns A promise that resolves to a success/failure message.
 */
export const testDatabaseConnectionApi = async (connectionData: FullDatabaseConnection): Promise<{ success: boolean; message: string }> => {
  try {
    const pythonFormattedData = toPythonFormat(connectionData);
    const response = await axios.post<{ success: boolean; message: string }>(`${API_BASE_URL}/test`, pythonFormattedData);
    return response.data;
  } catch (error) {
    console.error('API Error: Failed to test database connection:', error);
    const errorMsg = (error as any).response?.data?.detail || 'Connection test failed';
    return { success: false, message: errorMsg };
  }
};
