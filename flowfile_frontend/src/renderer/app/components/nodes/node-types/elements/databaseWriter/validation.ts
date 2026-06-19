// Mirrors the backend rule (flowfile_core schemas/input_schema.py validate_sql_identifier):
// each dot-separated part must match ^[a-zA-Z_][a-zA-Z0-9_]*$.
const SQL_IDENTIFIER_PART = /^[a-zA-Z_][a-zA-Z0-9_]*$/;

/**
 * Validate a SQL identifier (table or schema name), allowing dotted
 * "schema.table" notation. Returns an error message string, or null when valid
 * (empty/unset is treated as valid here — required-ness is handled separately).
 */
export function validateSqlIdentifier(value: string | null | undefined): string | null {
  if (value === null || value === undefined || value === "") {
    return null;
  }
  for (const part of value.split(".")) {
    if (!part || !SQL_IDENTIFIER_PART.test(part)) {
      return `Invalid SQL identifier: '${value}'. Only letters, numbers, and underscores are allowed.`;
    }
  }
  return null;
}
