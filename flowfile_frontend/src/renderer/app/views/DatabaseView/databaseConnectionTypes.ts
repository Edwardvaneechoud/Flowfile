//flowfile_frontend/src/renderer/app/pages/databaseManager/databaseConnectionTypes.ts

import type { AccessInfo } from "../../types/sharing.types";

export type DatabaseType = "postgresql" | "mysql" | "sqlite";

export const defaultPorts: Partial<Record<DatabaseType, number>> = {
  postgresql: 5432,
  mysql: 3306,
};

export const isFileBased = (dbType: DatabaseType): boolean => dbType === "sqlite";

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
