// Cloud storage connection types and interfaces for S3, ADLS, and other cloud providers

export type CloudStorageType = "s3" | "adls";

export const cloudStorageTypes: CloudStorageType[] = ["adls", "s3"]

export type AuthMethod = "access_key" | "iam_role" | "service_principal" | "managed_identity" | "sas_token" | "aws-cli" | "auto";

export interface PythonAuthSettingsInput {
    storage_type: CloudStorageType;
    auth_method: AuthMethod;
    connection_name?: string;
}

export interface AuthSettingsInput {
    storageType: CloudStorageType;
    authMethod: AuthMethod;
    connectionName?: string;
}

export interface PythonFullCloudStorageConnection extends PythonAuthSettingsInput {
    // AWS S3
    aws_region?: string;
    aws_access_key_id?: string;
    aws_secret_access_key?: string;
    aws_role_arn?: string;
    aws_allow_unsafe_html?: boolean;

    // Azure ADLS
    azure_account_name?: string;
    azure_account_key?: string;
    azure_tenant_id?: string;
    azure_client_id?: string;
    azure_client_secret?: string;

    // Common
    endpoint_url?: string;
    verify_ssl: boolean;
}

export interface FullCloudStorageConnection extends AuthSettingsInput {
    // AWS S3
    awsRegion?: string;
    awsAccessKeyId?: string;
    awsSecretAccessKey?: string;
    awsRoleArn?: string;
    awsAllowUnsafeHtml?: boolean;

    // Azure ADLS
    azureAccountName?: string;
    azureAccountKey?: string;
    azureTenantId?: string;
    azureClientId?: string;
    azureClientSecret?: string;

    // Common
    endpointUrl?: string;
    verifySsl: boolean;
}

export interface PythonFullCloudStorageConnectionInterface extends PythonAuthSettingsInput {
    // Public fields only
    aws_region?: string;
    aws_access_key_id?: string;
    aws_role_arn?: string;
    azure_account_name?: string;
    azure_tenant_id?: string;
    azure_client_id?: string;
    endpoint_url?: string;
    verify_ssl: boolean;
}

export interface FullCloudStorageConnectionInterface extends AuthSettingsInput {
    // Public fields only
    awsRegion?: string;
    awsAccessKeyId?: string;
    awsRoleArn?: string;
    azureAccountName?: string;
    azureTenantId?: string;
    azureClientId?: string;
    endpointUrl?: string;
    verifySsl: boolean;
}