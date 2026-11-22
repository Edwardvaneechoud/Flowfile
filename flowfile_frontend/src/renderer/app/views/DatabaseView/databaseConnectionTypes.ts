//flowfile_frontend/src/renderer/app/views/DatabaseView/databaseConnectionTypes.ts

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
  auth_method?: string | null;
  role?: string | null;
  warehouse?: string | null;
  private_key_passphrase?: string | null;
  oauth_flow_id?: string | null;
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
  authMethod?: string | null;
  role?: string | null;
  warehouse?: string | null;
  // Write-only: decrypts an uploaded private key once and is never stored.
  privateKeyPassphrase?: string | null;
  oauthFlowId?: string | null;
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
  id?: number;
  access?: AccessInfo | null;
  source?: "local" | "hub";
  auth_method?: string | null;
  role?: string | null;
  warehouse?: string | null;
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
  id?: number;
  access?: AccessInfo | null;
  // "hub" when listed by the person's hub: read-only here.
  source?: "local" | "hub";
  authMethod?: string | null;
  role?: string | null;
  warehouse?: string | null;
}

export interface DatabaseConnectionTestDetails {
  user?: string;
  role?: string;
  warehouse?: string;
  database?: string;
}

export interface DatabaseConnectionTestResult {
  success: boolean;
  message: string;
  details: DatabaseConnectionTestDetails | null;
}
