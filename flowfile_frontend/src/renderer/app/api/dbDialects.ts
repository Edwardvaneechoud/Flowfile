import axios from "axios";

export interface DbDialectInfo {
  name: string;
  display_name: string;
  file_based: boolean;
  default_port: number | null;
  supports_ssl: boolean;
  available: boolean;
  // Sign-in methods, in the order the form offers them; empty for every dialect but Snowflake.
  auth_methods: string[];
  // Usable through a saved connection only, never an inline node connection.
  saved_connection_only: boolean;
  // One login reaches many databases, so a node may pick one (Snowflake); absent from an older core.
  browses_databases?: boolean;
  // How to install a missing driver; absent from an older core.
  install_hint?: string | null;
}

// Rendered when the catalog request fails (offline, older core). Must mirror the
// backend registry's built-in dialects (shared/db_dialects).
export const FALLBACK_DIALECTS: DbDialectInfo[] = [
  {
    name: "postgresql",
    display_name: "PostgreSQL",
    file_based: false,
    default_port: 5432,
    supports_ssl: true,
    available: true,
    auth_methods: [],
    saved_connection_only: false,
  },
  {
    name: "mysql",
    display_name: "MySQL",
    file_based: false,
    default_port: 3306,
    supports_ssl: false,
    available: true,
    auth_methods: [],
    saved_connection_only: false,
  },
  {
    name: "sqlite",
    display_name: "SQLite",
    file_based: true,
    default_port: null,
    supports_ssl: false,
    available: true,
    auth_methods: [],
    saved_connection_only: false,
  },
  {
    name: "duckdb",
    display_name: "DuckDB",
    file_based: true,
    default_port: null,
    supports_ssl: false,
    available: true,
    auth_methods: [],
    saved_connection_only: false,
  },
  {
    name: "mssql",
    display_name: "SQL Server",
    file_based: false,
    default_port: 1433,
    supports_ssl: false,
    available: true,
    auth_methods: [],
    saved_connection_only: false,
  },
  {
    name: "denodo",
    display_name: "Denodo",
    file_based: false,
    default_port: 9996,
    supports_ssl: true,
    available: true,
    auth_methods: [],
    saved_connection_only: false,
  },
  {
    name: "snowflake",
    display_name: "Snowflake",
    file_based: false,
    default_port: 443,
    supports_ssl: false,
    available: true,
    auth_methods: ["oauth", "access_token", "key_pair"],
    saved_connection_only: true,
    browses_databases: true,
  },
];

// An older core omits the newer fields.
const withDefaults = (dialect: DbDialectInfo): DbDialectInfo => ({
  ...dialect,
  auth_methods: Array.isArray(dialect.auth_methods) ? dialect.auth_methods : [],
  saved_connection_only: dialect.saved_connection_only === true,
  browses_databases: dialect.browses_databases === true,
});

let catalogPromise: Promise<DbDialectInfo[]> | null = null;

// Every dialect core knows, installed or not; empty when the request fails.
const getDialectCatalog = (): Promise<DbDialectInfo[]> => {
  if (!catalogPromise) {
    catalogPromise = axios
      .get<DbDialectInfo[]>("/db_dialects")
      .then((response) => (response.data || []).map(withDefaults))
      .catch(() => {
        catalogPromise = null; // allow a retry on the next call
        return [];
      });
  }
  return catalogPromise;
};

export const getDbDialects = async (): Promise<DbDialectInfo[]> => {
  const dialects = (await getDialectCatalog()).filter((dialect) => dialect.available);
  return dialects.length > 0 ? dialects : FALLBACK_DIALECTS;
};

// Dialects core reports whose driver is not installed; only the connection form lists them.
export const getUnavailableDbDialects = async (): Promise<DbDialectInfo[]> =>
  (await getDialectCatalog()).filter((dialect) => !dialect.available);

export const notInstalledLabel = (dialect: DbDialectInfo): string =>
  `${dialect.display_name} (not installed)`;

export const installNote = (dialect: DbDialectInfo): string | null =>
  dialect.install_hint
    ? `${dialect.display_name} is not installed. Run: ${dialect.install_hint}`
    : null;
