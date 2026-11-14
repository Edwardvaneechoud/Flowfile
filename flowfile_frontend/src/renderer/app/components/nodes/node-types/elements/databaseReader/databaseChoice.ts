// Framework-free rules for the database a reader or writer node picks (database_name), shared by
// both drawers through useDatabaseChoice.ts.

export const MISSING_SCHEMA_MESSAGE = "Choose a schema when a database is set.";

/** A blank, null or absent database all mean the connection's default database. */
export function normalizeDatabase(value?: string | null): string {
  return (value ?? "").trim();
}

/** The Database select's placeholder: what an empty choice reads from. */
export function databasePlaceholder(defaultDatabase?: string | null): string {
  const fallback = normalizeDatabase(defaultDatabase);
  return fallback ? `Default: ${fallback}` : "All databases";
}

/** Whether a different database was picked, so a schema and table chosen for the old one no longer apply. */
export function databaseChanged(previous?: string | null, next?: string | null): boolean {
  return normalizeDatabase(previous) !== normalizeDatabase(next);
}

/** A database set without a schema is refused when the node runs, so the drawer says so first. */
export function missingSchemaError(
  database?: string | null,
  schema?: string | null,
): string | null {
  return normalizeDatabase(database) && !(schema ?? "").trim() ? MISSING_SCHEMA_MESSAGE : null;
}

export interface FieldNotice {
  text: string;
  tone: "hint" | "error";
}

/**
 * The missing-schema line under Schema: a neutral hint while the user is still choosing, an error once they
 * tried to apply or validate with the schema still missing.
 */
export function missingSchemaNotice(
  database: string | null | undefined,
  schema: string | null | undefined,
  attempted: boolean,
): FieldNotice | null {
  const text = missingSchemaError(database, schema);
  return text ? { text, tone: attempted ? "error" : "hint" } : null;
}

export const CHOOSE_SCHEMA_FIRST = "Choose a schema first";

/**
 * Whether the Table select asks core for its list.
 *
 * A dialect that browses databases lists every table of the database as `schema.table` when no
 * schema is given, which is slow and is not what the drawer offers, so it waits for a schema.
 */
export function shouldLoadTables(browsesDatabases: boolean, schema?: string | null): boolean {
  return !browsesDatabases || !!(schema ?? "").trim();
}

/** What the Table select's empty list says. */
export function tablesEmptyText(browsesDatabases: boolean, schema?: string | null): string {
  return shouldLoadTables(browsesDatabases, schema) ? "No data" : CHOOSE_SCHEMA_FIRST;
}

const IDENTIFIER_PART = String.raw`(?:[A-Za-z_][A-Za-z0-9_$]*|"(?:[^"]|"")+")`;
const IDENTIFIER_PATH = new RegExp(`^${IDENTIFIER_PART}(?:\\.${IDENTIFIER_PART})*$`);

/** The parts of a dotted name, split on the dots outside double quotes; null when it is not one. */
export function identifierParts(value: string): string[] | null {
  if (!IDENTIFIER_PATH.test(value)) return null;
  return value.match(new RegExp(IDENTIFIER_PART, "g"));
}

export interface TableReference {
  database?: string;
  schema: string;
  table: string;
}

/**
 * Split a qualified name typed or picked in the Table field while no schema is chosen.
 *
 * Every dialect splits `schema.table` at its first dot, as the drawers always did. A dialect that
 * browses databases also takes `database.schema.table`, split on the dots outside double quotes, so
 * `"my.db"."My Schema".T` keeps its quoted parts. Anything else is left as typed (null).
 */
export function splitTableReference(
  value: string | null | undefined,
  browsesDatabases: boolean,
): TableReference | null {
  if (!value || !value.includes(".")) return null;
  if (browsesDatabases) {
    const parts = identifierParts(value);
    if (!parts) return null;
    if (parts.length === 3) return { database: parts[0], schema: parts[1], table: parts[2] };
    if (parts.length === 2) return { schema: parts[0], table: parts[1] };
    return null;
  }
  const dotIndex = value.indexOf(".");
  return { schema: value.substring(0, dotIndex), table: value.substring(dotIndex + 1) };
}

/** A saved connection as the connection dropdown shows it. */
export function connectionOptionLabel(
  connection: { connectionName: string; databaseType: string; database?: string | null },
  browsesDatabases: boolean,
): string {
  const database = normalizeDatabase(connection.database);
  const shown = database || (browsesDatabases ? "all databases" : "");
  return `${connection.connectionName} (${connection.databaseType} - ${shown})`;
}
