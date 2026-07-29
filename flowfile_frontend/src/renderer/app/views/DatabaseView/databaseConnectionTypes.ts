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
}
