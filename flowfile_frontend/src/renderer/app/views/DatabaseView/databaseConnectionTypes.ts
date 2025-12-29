//flowfile_frontend/src/renderer/app/pages/databaseManager/databaseConnectionTypes.ts

export interface PythonFullDatabaseConnection {
    connection_name: string;
    database_type: 'postgresql';
    username: string
    password: string;
    host?: string;
    port?: number;
    database?: string;
    ssl_enabled: boolean;
    url?: string;
}


export interface FullDatabaseConnection {
    connectionName: string;
    databaseType: string;
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
    database_type: "postgresql";
    username: string;
    host?: string;
    port?: number;
    database?: string
    ssl_enabled: boolean
    url?: string
}

export interface FullDatabaseConnectionInterface {
    connectionName: string;
    databaseType: "postgresql";
    username: string;
    host?: string;
    port?: number;
    database?: string
    sslEnabled: boolean
    url?: string
}
