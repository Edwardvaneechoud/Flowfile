//flowfile_frontend/src/renderer/app/pages/databaseManager/databaseConnectionTypes.ts

// Supported database types
export type DatabaseType = "postgresql" | "mysql" | "sqlite" | "mssql" | "oracle" | "duckdb";

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
}
