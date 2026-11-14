// Mirrors the backend rule (shared/db_dialects: DbDialect.checked_identifier_path). Every dialect takes dotted
// parts of letters, digits and underscores; one with quoted_identifiers (Snowflake) also takes $ and parts in
// double quotes whose inner quotes are doubled, joined by dots outside the quotes.
const SQL_IDENTIFIER_PART = /^[a-zA-Z_][a-zA-Z0-9_]*$/;
export const QUOTED_IDENTIFIER_PART = String.raw`(?:[A-Za-z_][A-Za-z0-9_$]*|"(?:[^"]|"")+")`;
export const QUOTED_IDENTIFIER_PATH = new RegExp(
  `^${QUOTED_IDENTIFIER_PART}(?:\\.${QUOTED_IDENTIFIER_PART})*$`,
);

export const QUOTED_NAME_HINT = "Use letters, numbers, _ and $, or put the name in double quotes.";

export interface IdentifierRule {
  // The connection's dialect takes $ and double-quoted names.
  quoted?: boolean;
}

/**
 * Validate a SQL identifier (table, schema or database name), allowing dotted
 * "schema.table" notation. Returns an error message string, or null when valid
 * (empty/unset is treated as valid here — required-ness is handled separately).
 * Strict unless the connection's dialect takes quoted names.
 */
export function validateSqlIdentifier(
  value: string | null | undefined,
  rule: IdentifierRule = {},
): string | null {
  if (value === null || value === undefined || value === "") {
    return null;
  }
  if (rule.quoted) {
    return QUOTED_IDENTIFIER_PATH.test(value)
      ? null
      : `Invalid SQL identifier: '${value}'. ${QUOTED_NAME_HINT}`;
  }
  for (const part of value.split(".")) {
    if (!part || !SQL_IDENTIFIER_PART.test(part)) {
      return `Invalid SQL identifier: '${value}'. Only letters, numbers, and underscores are allowed.`;
    }
  }
  return null;
}
