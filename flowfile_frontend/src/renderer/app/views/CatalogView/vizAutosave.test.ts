import { describe, expect, it } from "vitest";
import { buildVizSaveBody, hashString, shouldCaptureThumbnail, vizDraftKey } from "./vizAutosave";

describe("hashString", () => {
  it("is stable for the same input", () => {
    expect(hashString("select * from t")).toBe(hashString("select * from t"));
  });

  it("differs for different inputs", () => {
    expect(hashString("select * from a")).not.toBe(hashString("select * from b"));
  });

  it("handles the empty string", () => {
    expect(hashString("")).toBe(hashString(""));
    expect(hashString("")).not.toBe(hashString("x"));
  });
});

describe("vizDraftKey", () => {
  it("scopes table sources by table id and user", () => {
    expect(vizDraftKey({ source_type: "table", table_id: 12 }, 7)).toBe(
      "catalog.vizDraft.7.table:12",
    );
  });

  it("scopes sql sources by query hash", () => {
    const a = vizDraftKey({ source_type: "sql", sql_query: "select 1" }, "alice");
    const b = vizDraftKey({ source_type: "sql", sql_query: "select 2" }, "alice");
    expect(a).toBe(vizDraftKey({ source_type: "sql", sql_query: "select 1" }, "alice"));
    expect(a).not.toBe(b);
    expect(a.startsWith("catalog.vizDraft.alice.sql:")).toBe(true);
  });

  it("separates users on the same source", () => {
    const source = { source_type: "table" as const, table_id: 3 };
    expect(vizDraftKey(source, "alice")).not.toBe(vizDraftKey(source, "bob"));
  });

  it("tolerates missing table id / query", () => {
    expect(vizDraftKey({ source_type: "table" }, 1)).toBe("catalog.vizDraft.1.table:unknown");
    expect(vizDraftKey({ source_type: "sql" }, 1)).toBe(
      vizDraftKey({ source_type: "sql", sql_query: "" }, 1),
    );
  });
});

describe("buildVizSaveBody", () => {
  const base = { name: "Revenue", spec: [{ a: 1 }] };

  it("attaches the thumbnail only on flush saves", () => {
    const flush = buildVizSaveBody(base, {
      reason: "flush",
      thumbnail: "data:image/png;base64,x",
      expectedUpdatedAt: "2026-01-01T00:00:00",
    });
    expect(flush.thumbnail_data_url).toBe("data:image/png;base64,x");
    for (const reason of ["debounce", "maxwait", "retry"] as const) {
      const tick = buildVizSaveBody(base, {
        reason,
        thumbnail: "data:image/png;base64,x",
        expectedUpdatedAt: null,
      });
      expect(tick.thumbnail_data_url).toBeUndefined();
      expect(shouldCaptureThumbnail(reason)).toBe(false);
    }
    expect(shouldCaptureThumbnail("flush")).toBe(true);
  });

  it("echoes the CAS token only when present", () => {
    expect(
      buildVizSaveBody(base, { reason: "debounce", thumbnail: null, expectedUpdatedAt: "t1" })
        .expected_updated_at,
    ).toBe("t1");
    expect(
      buildVizSaveBody(base, { reason: "debounce", thumbnail: null, expectedUpdatedAt: null })
        .expected_updated_at,
    ).toBeUndefined();
  });
});
