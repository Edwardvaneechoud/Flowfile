import { describe, it, expect } from "vitest";
import {
  consumerKeyFor,
  cursorStatusLine,
  findCursor,
  pendingLabel,
  readModeDisabledReason,
} from "./catalogCdc";
import type { CatalogTable, CdcCursor } from "../types";

function cursor(overrides: Partial<CdcCursor> = {}): CdcCursor {
  return {
    consumer_key: "flow:abc-123:node:7",
    consumer_label: null,
    owner_id: 1,
    last_version: 12,
    last_commit_timestamp: null,
    pending_commits: 3,
    updated_at: new Date().toISOString(),
    ...overrides,
  };
}

function table(overrides: Partial<CatalogTable> = {}): CatalogTable {
  return { table_type: "physical", scd2: null, ...overrides } as CatalogTable;
}

describe("consumerKeyFor", () => {
  it("prefixes a typed name and ignores blank ones", () => {
    expect(consumerKeyFor("nightly")).toBe("name:nightly");
    expect(consumerKeyFor("  nightly  ")).toBe("name:nightly");
    expect(consumerKeyFor("   ")).toBeNull();
    expect(consumerKeyFor(null)).toBeNull();
  });
});

describe("findCursor", () => {
  const cursors = [
    cursor({ consumer_key: "name:nightly", last_version: 4 }),
    cursor({ consumer_key: "flow:abc-123:node:7", last_version: 12 }),
    cursor({ consumer_key: "flow:abc-123:node:17", last_version: 99 }),
  ];

  it("matches a named cursor exactly", () => {
    expect(findCursor(cursors, "nightly", 7)?.last_version).toBe(4);
    expect(findCursor(cursors, "other", 7)).toBeNull();
  });

  it("matches an unnamed cursor by node suffix without partial-id bleed", () => {
    expect(findCursor(cursors, null, 7)?.last_version).toBe(12);
    expect(findCursor(cursors, null, 17)?.last_version).toBe(99);
    expect(findCursor(cursors, null, 3)).toBeNull();
  });
});

describe("cursorStatusLine", () => {
  it("describes what a first run will do", () => {
    expect(cursorStatusLine(null, "now")).toContain("starts from now");
    expect(cursorStatusLine(null, "beginning")).toContain("every tracked change");
  });

  it("reports version, age and pending commits", () => {
    const line = cursorStatusLine(cursor(), "now");
    expect(line).toContain("Cursor v12");
    expect(line).toContain("advanced just now");
    expect(line).toContain("3 new commits");
  });

  it("omits the pending segment when the head is unknown", () => {
    expect(cursorStatusLine(cursor({ pending_commits: null }), "now")).not.toContain("commit");
  });
});

describe("pendingLabel", () => {
  it("singularises and handles the unknown head", () => {
    expect(pendingLabel(1)).toBe("1 new commit");
    expect(pendingLabel(0)).toBe("up to date");
    expect(pendingLabel(null)).toBe("--");
  });
});

describe("readModeDisabledReason", () => {
  it("allows a plain physical table at the latest version", () => {
    expect(readModeDisabledReason(table(), null)).toBeNull();
  });

  it("refuses virtual, SCD2 and version-pinned reads", () => {
    expect(readModeDisabledReason(null, null)).toContain("Pick a table");
    expect(readModeDisabledReason(table({ table_type: "virtual" }), null)).toContain("Virtual");
    expect(readModeDisabledReason(table({ scd2: {} as never }), null)).toContain("SCD2");
    expect(readModeDisabledReason(table(), 3)).toContain("pinned");
  });
});
