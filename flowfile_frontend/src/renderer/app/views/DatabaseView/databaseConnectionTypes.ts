//flowfile_frontend/src/renderer/app/pages/databaseManager/databaseConnectionTypes.ts

import type { AccessInfo } from "../../types/sharing.types";

// Dialect names come from the backend registry (GET /db_dialects, shared/db_dialects);
// use composables/useDbDialects for metadata (file_based, default ports, display names).
export type DatabaseType = string;

export interface PythonFullDatabaseConnection {
  connection_name: string;
  database_type: DatabaseType;
  username: string;
  password: string;
  host?: string;
  port?: number;
  database?: string;
  ssl_enabled: boolean;
  url?: string;
  extra_params?: Record<string, string> | null;
  auth_method?: string | null;
  private_key?: string;
  private_key_passphrase?: string;
  oauth_client_id?: string | null;
  oauth_client_secret?: string;
  oauth_authorize_endpoint?: string | null;
  oauth_token_endpoint?: string | null;
  oauth_redirect_uri?: string | null;
}

export interface FullDatabaseConnection {
  connectionName: string;
  databaseType: DatabaseType;
  username: string;
  password: string;
  host?: string;
  port?: number;
  database?: string;
  sslEnabled: boolean;
  url?: string;
  extraParams?: Record<string, string> | null;
  authMethod?: string;
  privateKey?: string;
  privateKeyPassphrase?: string;
  oauthClientId?: string;
  oauthClientSecret?: string;
  oauthAuthorizeEndpoint?: string;
  oauthTokenEndpoint?: string;
  oauthRedirectUri?: string;
}

export interface PythonFullDatabaseConnectionInterface {
  connection_name: string;
  database_type: DatabaseType;
  username: string;
  host?: string;
  port?: number;
  database?: string;
  ssl_enabled: boolean;
  url?: string;
  extra_params?: Record<string, string> | null;
  auth_method?: string | null;
  oauth_client_id?: string | null;
  oauth_authorize_endpoint?: string | null;
  oauth_token_endpoint?: string | null;
  oauth_redirect_uri?: string | null;
  oauth_connected?: boolean;
  id?: number;
  access?: AccessInfo | null;
}

export interface FullDatabaseConnectionInterface {
  connectionName: string;
  databaseType: DatabaseType;
  username: string;
  host?: string;
  port?: number;
  database?: string;
  sslEnabled: boolean;
  url?: string;
  extraParams?: Record<string, string> | null;
  authMethod?: string;
  oauthClientId?: string;
  oauthAuthorizeEndpoint?: string;
  oauthTokenEndpoint?: string;
  oauthRedirectUri?: string;
  oauthConnected?: boolean;
  id?: number;
  access?: AccessInfo | null;
}
