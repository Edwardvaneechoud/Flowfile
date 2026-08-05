import axios from "axios";

export interface DbDialectFieldInfo {
  name: string;
  label: string;
  required: boolean;
}

export interface DbDialectInfo {
  name: string;
  display_name: string;
  file_based: boolean;
  default_port: number | null;
  supports_ssl: boolean;
  available: boolean;
  // Dialect-specific connection fields (stored in extra_params) and standard
  // form fields the dialect hides; absent on older cores, so keep optional.
  extra_fields?: DbDialectFieldInfo[];
  hidden_fields?: string[];
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
    extra_fields: [],
    hidden_fields: [],
  },
  {
    name: "mysql",
    display_name: "MySQL",
    file_based: false,
    default_port: 3306,
    supports_ssl: false,
    available: true,
    extra_fields: [],
    hidden_fields: [],
  },
  {
    name: "sqlite",
    display_name: "SQLite",
    file_based: true,
    default_port: null,
    supports_ssl: false,
    available: true,
    extra_fields: [],
    hidden_fields: [],
  },
  {
    name: "duckdb",
    display_name: "DuckDB",
    file_based: true,
    default_port: null,
    supports_ssl: false,
    available: true,
    extra_fields: [],
    hidden_fields: [],
  },
  {
    name: "mssql",
    display_name: "SQL Server",
    file_based: false,
    default_port: 1433,
    supports_ssl: false,
    available: true,
    extra_fields: [],
    hidden_fields: [],
  },
  {
    name: "snowflake",
    display_name: "Snowflake",
    file_based: false,
    default_port: 443,
    supports_ssl: false,
    available: true,
    extra_fields: [
      { name: "account", label: "Account", required: true },
      { name: "warehouse", label: "Warehouse", required: false },
      { name: "role", label: "Role", required: false },
    ],
    hidden_fields: ["host", "port", "ssl"],
  },
];

let dialectsPromise: Promise<DbDialectInfo[]> | null = null;

export const getDbDialects = (): Promise<DbDialectInfo[]> => {
  if (!dialectsPromise) {
    dialectsPromise = axios
      .get<DbDialectInfo[]>("/db_dialects")
      .then((response) => {
        const dialects = (response.data || []).filter((dialect) => dialect.available);
        return dialects.length > 0 ? dialects : FALLBACK_DIALECTS;
      })
      .catch(() => {
        dialectsPromise = null; // allow a retry on the next call
        return FALLBACK_DIALECTS;
      });
  }
  return dialectsPromise;
};
