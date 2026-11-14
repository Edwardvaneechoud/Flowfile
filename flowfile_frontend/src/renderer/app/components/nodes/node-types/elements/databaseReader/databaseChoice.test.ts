import { describe, expect, it } from "vitest";

import {
  CHOOSE_SCHEMA_FIRST,
  MISSING_SCHEMA_MESSAGE,
  connectionOptionLabel,
  databaseChanged,
  databasePlaceholder,
  missingSchemaError,
  missingSchemaNotice,
  normalizeDatabase,
  shouldLoadTables,
  identifierParts,
  splitTableReference,
  tablesEmptyText,
} from "./databaseChoice";

describe("databaseChoice", () => {
  it("treats blank, null and absent as the connection's default", () => {
    expect(normalizeDatabase(undefined)).toBe("");
    expect(normalizeDatabase(null)).toBe("");
    expect(normalizeDatabase("  ")).toBe("");
    expect(normalizeDatabase(" SALES ")).toBe("SALES");
  });

  it("shows the connection's default, or all databases, as the placeholder", () => {
    expect(databasePlaceholder("ANALYTICS")).toBe("Default: ANALYTICS");
    expect(databasePlaceholder("")).toBe("All databases");
    expect(databasePlaceholder(null)).toBe("All databases");
  });

  it("only counts a real change of database", () => {
    expect(databaseChanged(undefined, null)).toBe(false);
    expect(databaseChanged("", undefined)).toBe(false);
    expect(databaseChanged("SALES", "SALES")).toBe(false);
    expect(databaseChanged("SALES", "HR")).toBe(true);
    expect(databaseChanged(null, "HR")).toBe(true);
    expect(databaseChanged("HR", "")).toBe(true);
  });

  it("asks for a schema once a database is set", () => {
    expect(missingSchemaError("SALES", "")).toBe(MISSING_SCHEMA_MESSAGE);
    expect(missingSchemaError("SALES", undefined)).toBe(MISSING_SCHEMA_MESSAGE);
    expect(missingSchemaError("SALES", "PUBLIC")).toBeNull();
    expect(missingSchemaError(null, "")).toBeNull();
  });

  it("splits schema.table at the first dot for every dialect, as before", () => {
    expect(splitTableReference("public.movies", false)).toEqual({
      schema: "public",
      table: "movies",
    });
    expect(splitTableReference("a.b.c", false)).toEqual({ schema: "a", table: "b.c" });
    expect(splitTableReference("movies", false)).toBeNull();
    expect(splitTableReference("", false)).toBeNull();
  });

  it("takes database.schema.table for a dialect that browses databases", () => {
    expect(splitTableReference("SALES.PUBLIC.ORDERS", true)).toEqual({
      database: "SALES",
      schema: "PUBLIC",
      table: "ORDERS",
    });
    expect(splitTableReference("PUBLIC.ORDERS", true)).toEqual({
      schema: "PUBLIC",
      table: "ORDERS",
    });
    expect(splitTableReference("a.b.c.d", true)).toBeNull();
    expect(splitTableReference("a..c", true)).toBeNull();
    expect(splitTableReference("ORDERS", true)).toBeNull();
  });

  it("labels a connection without a database cleanly", () => {
    const base = { connectionName: "sf", databaseType: "snowflake" };
    expect(connectionOptionLabel({ ...base, database: "" }, true)).toBe(
      "sf (snowflake - all databases)",
    );
    expect(connectionOptionLabel({ ...base, database: "DB" }, true)).toBe("sf (snowflake - DB)");
    expect(
      connectionOptionLabel(
        { connectionName: "pg", databaseType: "postgresql", database: "app" },
        false,
      ),
    ).toBe("pg (postgresql - app)");
    expect(connectionOptionLabel({ connectionName: "pg", databaseType: "postgresql" }, false)).toBe(
      "pg (postgresql - )",
    );
  });

  it("waits for a schema before listing tables of a dialect that browses databases", () => {
    expect(shouldLoadTables(true, undefined)).toBe(false);
    expect(shouldLoadTables(true, "  ")).toBe(false);
    expect(shouldLoadTables(true, "PUBLIC")).toBe(true);
    expect(shouldLoadTables(false, undefined)).toBe(true);
    expect(tablesEmptyText(true, null)).toBe(CHOOSE_SCHEMA_FIRST);
    expect(tablesEmptyText(true, "PUBLIC")).toBe("No data");
    expect(tablesEmptyText(false, undefined)).toBe("No data");
  });

  it("shows the missing schema as a hint until an apply or validate tried without one", () => {
    expect(missingSchemaNotice("SALES", "", false)).toEqual({
      text: MISSING_SCHEMA_MESSAGE,
      tone: "hint",
    });
    expect(missingSchemaNotice("SALES", undefined, true)).toEqual({
      text: MISSING_SCHEMA_MESSAGE,
      tone: "error",
    });
    expect(missingSchemaNotice("SALES", "PUBLIC", true)).toBeNull();
    expect(missingSchemaNotice(null, "", true)).toBeNull();
  });

  it("splits a quoted name on the dots outside the quotes", () => {
    expect(identifierParts('"my.db"."My Schema".T')).toEqual(['"my.db"', '"My Schema"', "T"]);
    expect(identifierParts('"open.T')).toBeNull();
    expect(splitTableReference('"USER$evaneechoud"."Raw".orders', true)).toEqual({
      database: '"USER$evaneechoud"',
      schema: '"Raw"',
      table: "orders",
    });
    expect(splitTableReference('"my.db"."My Schema"', true)).toEqual({
      schema: '"my.db"',
      table: '"My Schema"',
    });
    expect(splitTableReference('a"b.c', true)).toBeNull();
  });
});
