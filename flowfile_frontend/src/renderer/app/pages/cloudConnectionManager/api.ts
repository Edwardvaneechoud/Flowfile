//flowfile_frontend/src/renderer/app/pages/cloudConnectionManager/api.ts

import axios from 'axios';
import type { 
  FullCloudStorageConnection, 
  FullCloudStorageConnectionInterface,
  PythonFullCloudStorageConnection,
  PythonFullCloudStorageConnectionInterface
} from './CloudConnectionTypes';

const API_BASE_URL = '/cloud_connections';

/**
 * Converts JavaScript camelCase to Python snake_case for API requests
 */
const toPythonFormat = (connection: FullCloudStorageConnection): PythonFullCloudStorageConnection => {
  return {
    storage_type: connection.storageType,
    auth_method: connection.authMethod,
    connection_name: connection.connectionName,
    
    // AWS S3
    aws_region: connection.awsRegion,
    aws_access_key_id: connection.awsAccessKeyId,
    aws_secret_access_key: connection.awsSecretAccessKey,
    aws_role_arn: connection.awsRoleArn,
    aws_allow_unsafe_html: connection.awsAllowUnsafeHtml,
    
    // Azure ADLS
    azure_account_name: connection.azureAccountName,
    azure_account_key: connection.azureAccountKey,
    azure_tenant_id: connection.azureTenantId,
    azure_client_id: connection.azureClientId,
    azure_client_secret: connection.azureClientSecret,
    
    // Common
    endpoint_url: connection.endpointUrl,
    verify_ssl: connection.verifySsl
  };
};

/**
 * Fetches the list of cloud storage connections from the API.
 * @returns A promise that resolves to an array of FullCloudStorageConnectionInterface objects.
 */
export const fetchCloudStorageConnectionsInterfaces = async (): Promise<FullCloudStorageConnectionInterface[]> => {
  try {
    const response = await axios.get<PythonFullCloudStorageConnectionInterface[]>(API_BASE_URL+"/cloud_connections");
    return response.data.map(convertConnectionInterfacePytoTs);
  } catch (error) {
    console.error('API Error: Failed to load cloud storage connections:', error);
    throw error;
  }
};

export const convertConnectionInterfacePytoTs = (
  pythonConnectionInterface: PythonFullCloudStorageConnectionInterface
): FullCloudStorageConnectionInterface => {
  return {
    storageType: pythonConnectionInterface.storage_type,
    authMethod: pythonConnectionInterface.auth_method,
    connectionName: pythonConnectionInterface.connection_name,
    
    // AWS S3
    awsRegion: pythonConnectionInterface.aws_region,
    awsAccessKeyId: pythonConnectionInterface.aws_access_key_id,
    awsRoleArn: pythonConnectionInterface.aws_role_arn,
    
    // Azure ADLS
    azureAccountName: pythonConnectionInterface.azure_account_name,
    azureTenantId: pythonConnectionInterface.azure_tenant_id,
    azureClientId: pythonConnectionInterface.azure_client_id,
    
    // Common
    endpointUrl: pythonConnectionInterface.endpoint_url,
    verifySsl: pythonConnectionInterface.verify_ssl
  };
};

export const convertConnectionInterfaceTstoPy = (
  cloudConnectionInterface: FullCloudStorageConnectionInterface
): PythonFullCloudStorageConnectionInterface => {
  return {
    storage_type: cloudConnectionInterface.storageType,
    auth_method: cloudConnectionInterface.authMethod,
    connection_name: cloudConnectionInterface.connectionName,
    
    // AWS S3
    aws_region: cloudConnectionInterface.awsRegion,
    aws_access_key_id: cloudConnectionInterface.awsAccessKeyId,
    aws_role_arn: cloudConnectionInterface.awsRoleArn,
    
    // Azure ADLS
    azure_account_name: cloudConnectionInterface.azureAccountName,
    azure_tenant_id: cloudConnectionInterface.azureTenantId,
    azure_client_id: cloudConnectionInterface.azureClientId,
    
    // Common
    endpoint_url: cloudConnectionInterface.endpointUrl,
    verify_ssl: cloudConnectionInterface.verifySsl
  };
};

/**
 * Creates a new cloud storage connection via the API.
 * @param connectionData - The cloud storage connection configuration to add.
 * @returns A promise that resolves when the connection is created.
 */
export const createCloudStorageConnectionApi = async (connectionData: FullCloudStorageConnection): Promise<void> => {
  try {
    const pythonFormattedData = toPythonFormat(connectionData);
    await axios.post(API_BASE_URL+"/cloud_connection", pythonFormattedData);
  } catch (error) {
    console.error('API Error: Failed to create cloud storage connection:', error);
    const errorMsg = (error as any).response?.data?.detail || 'Failed to create cloud storage connection';
    throw new Error(errorMsg);
  }
};

/**
 * Deletes a cloud storage connection via the API.
 * @param connectionName - The name of the connection to delete.
 * @returns A promise that resolves when the connection is deleted.
 */
export const deleteCloudStorageConnectionApi = async (connectionName: string): Promise<void> => {
  try {
    await axios.delete(`${API_BASE_URL}/cloud_connection?connection_name=${encodeURIComponent(connectionName)}`);
  } catch (error) {
    console.error('API Error: Failed to delete cloud storage connection:', error);
    throw error;
  }
};

/**
 * Tests a cloud storage connection via the API.
 * @param connectionData - The cloud storage connection configuration to test.
 * @returns A promise that resolves to a success/failure message.
 */
export const testCloudStorageConnectionApi = async (connectionData: FullCloudStorageConnection): Promise<{ success: boolean; message: string }> => {
  try {
    const pythonFormattedData = toPythonFormat(connectionData);
    const response = await axios.post<{ success: boolean; message: string }>(`${API_BASE_URL}/test`, pythonFormattedData);
    return response.data;
  } catch (error) {
    console.error('API Error: Failed to test cloud storage connection:', error);
    const errorMsg = (error as any).response?.data?.detail || 'Connection test failed';
    return { success: false, message: errorMsg };
  }
};