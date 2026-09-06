import axios from "axios";

export interface DbDialectInfo {
  name: string;
  display_name: string;
  file_based: boolean;
  default_port: number | null;
  supports_ssl: boolean;
  available: boolean;
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
  },
  {
    name: "mysql",
    display_name: "MySQL",
    file_based: false,
    default_port: 3306,
    supports_ssl: false,
    available: true,
  },
  {
    name: "sqlite",
    display_name: "SQLite",
    file_based: true,
    default_port: null,
    supports_ssl: false,
    available: true,
  },
  {
    name: "duckdb",
    display_name: "DuckDB",
    file_based: true,
    default_port: null,
    supports_ssl: false,
    available: true,
  },
  {
    name: "mssql",
    display_name: "SQL Server",
    file_based: false,
    default_port: 1433,
    supports_ssl: false,
    available: true,
  },
  {
    name: "denodo",
    display_name: "Denodo",
    file_based: false,
    default_port: 9996,
    supports_ssl: true,
    available: true,
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
