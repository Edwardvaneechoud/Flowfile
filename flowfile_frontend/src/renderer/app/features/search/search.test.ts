import { describe, expect, it } from "vitest";
import type { SearchHit, SearchResponse } from "../../types/search.types";
import { isComposingKey, isGlobalSearchShortcut } from "./globalSearchHotkey";
import { highlightSegments } from "./highlight";
import { extraMatches, flattenGroups, groupHits, totalMatches } from "./searchGroups";
import { SEARCH_KINDS, hitKindLabel } from "./searchKinds";
import { flowCatalogRef, primaryAction, secondaryAction } from "./searchNavigation";

const hit = (overrides: Partial<SearchHit>): SearchHit => ({
  kind: "table",
  id: 1,
  name: "orders",
  qualified_name: "main.sales.orders",
  namespace_id: 3,
  path: ["main", "sales"],
  description: null,
  subtitle: null,
  parent: null,
  matches: [],
  score: 50,
  uri: "flowfile://catalog/tables/1",
  updated_at: null,
  details: {},
  ...overrides,
});

const response = (hits: SearchHit[], counts: SearchResponse["counts"] = {}): SearchResponse => ({
  query: "orders",
  tokens: ["orders"],
  kinds: SEARCH_KINDS.map((k) => k.kind),
  hits,
  counts,
  truncated: false,
  truncated_kinds: [],
});

describe("highlightSegments", () => {
  it("marks every case-insensitive occurrence of every token", () => {
    expect(highlightSegments("Sales_orders", ["sales", "ORD"])).toEqual([
      { text: "Sales", match: true },
      { text: "_", match: false },
      { text: "ord", match: true },
      { text: "ers", match: false },
    ]);
  });

  it("merges overlapping matches and leaves unmatched text whole", () => {
    expect(highlightSegments("aaa", ["aa"])).toEqual([{ text: "aaa", match: true }]);
    expect(highlightSegments("orders", ["xyz"])).toEqual([{ text: "orders", match: false }]);
    expect(highlightSegments("", ["a"])).toEqual([]);
  });
});

describe("groupHits", () => {
  it("orders kinds by their best hit and keeps each kind's total", () => {
    const groups = groupHits(
      response(
        [
          hit({ kind: "flow", id: 9, score: 90, uri: "f9" }),
          hit({ kind: "table", id: 1, score: 80, uri: "t1" }),
          hit({ kind: "table", id: 2, score: 70, uri: "t2" }),
        ],
        { flow: 1, table: 12 },
      ),
    );
    expect(groups.map((g) => [g.kind, g.total, g.hits.length])).toEqual([
      ["flow", 1, 1],
      ["table", 12, 2],
    ]);
    expect(flattenGroups(groups).map((h) => h.uri)).toEqual(["f9", "t1", "t2"]);
  });

  it("breaks score ties by kind order and handles no response", () => {
    const groups = groupHits(
      response([
        hit({ kind: "flow", score: 50, uri: "f" }),
        hit({ kind: "table", score: 50, uri: "t" }),
      ]),
    );
    expect(groups.map((g) => g.kind)).toEqual(["table", "flow"]);
    expect(groupHits(null)).toEqual([]);
  });

  it("totals every kind's count", () => {
    expect(totalMatches(response([], { table: 3, flow: 2 }))).toBe(5);
    expect(totalMatches(null)).toBe(0);
  });
});

describe("extraMatches", () => {
  it("leaves out what the row already shows", () => {
    const shown = hit({
      subtitle: "Int64",
      matches: [
        { field: "name", value: "orders" },
        { field: "path", value: "main.sales" },
        { field: "package", value: "xgboost" },
        { field: "dtype", value: "Int64" },
      ],
    });
    expect(extraMatches(shown)).toEqual([{ field: "package", value: "xgboost", label: "package" }]);
  });
});

describe("primaryAction", () => {
  it("opens catalog items through the catalog's own query parameters", () => {
    expect(primaryAction(hit({ kind: "table", id: 4 }))).toEqual({
      type: "route",
      to: { name: "catalog", query: { tab: "catalog", tableId: "4" } },
      catalogTarget: { kind: "table", id: 4 },
    });
    expect(primaryAction(hit({ kind: "namespace", id: 2, details: { level: 0 } }))).toEqual({
      type: "route",
      to: { name: "catalog", query: { tab: "catalog", namespaceId: "2" } },
      catalogTarget: { kind: "namespace", id: 2 },
    });
    expect(primaryAction(hit({ kind: "schedule", id: 7 }))).toEqual({
      type: "route",
      to: { name: "catalog", query: { tab: "schedules", scheduleId: "7" } },
      catalogTarget: { kind: "schedule", id: 7 },
    });
    expect(primaryAction(hit({ kind: "visualization", id: 5 }))).toEqual({
      type: "route",
      to: { name: "catalog", query: { tab: "visuals", vizId: "5" } },
    });
  });

  it("opens a column's table", () => {
    const column = hit({ kind: "column", id: "4:amount", details: { table_id: 4 } });
    expect(primaryAction(column)).toEqual({
      type: "route",
      to: { name: "catalog", query: { tab: "catalog", tableId: "4" } },
      catalogTarget: { kind: "table", id: 4 },
    });
  });

  it("opens a schema's catalog, the page that shows catalog details", () => {
    const schema = hit({ kind: "namespace", id: 9, namespace_id: 2, details: { level: 1 } });
    expect(primaryAction(schema)).toEqual({
      type: "route",
      to: { name: "catalog", query: { tab: "catalog", namespaceId: "2" } },
      catalogTarget: { kind: "namespace", id: 9 },
    });
  });

  it("opens notebooks, dashboards, kernels and custom nodes in their own pages", () => {
    expect(primaryAction(hit({ kind: "notebook", id: 3 }))).toEqual({
      type: "notebook",
      notebookId: 3,
    });
    expect(primaryAction(hit({ kind: "dashboard", id: 8 }))).toEqual({
      type: "route",
      to: { name: "dashboard-view", params: { id: "8" } },
    });
    expect(primaryAction(hit({ kind: "kernel", id: "ml-1" }))).toEqual({
      type: "route",
      to: { name: "compute", query: { tab: "kernels", kernelId: "ml-1" } },
    });
    expect(
      primaryAction(
        hit({ kind: "custom_node", id: "dedupe", details: { file_name: "dedupe.py" } }),
      ),
    ).toEqual({ type: "route", to: { name: "nodeDesigner", query: { openFile: "dedupe.py" } } });
    expect(
      primaryAction(
        hit({ kind: "custom_node", id: "x", details: { file_name: "x.py", mounted: true } }),
      ),
    ).toEqual({ type: "route", to: { name: "catalog", query: { tab: "customNodes" } } });
  });
});

describe("secondaryAction", () => {
  it("opens a flow in the designer with its catalog reference", () => {
    const flow = hit({
      kind: "flow",
      id: 6,
      name: "load",
      path: ["main", "etl"],
      details: { flow_path: "/flows/load.yaml" },
    });
    expect(flowCatalogRef(flow)).toBe("main.etl.load");
    expect(secondaryAction(flow)).toEqual({
      label: "Open in designer",
      action: {
        type: "designer",
        flowPath: "/flows/load.yaml",
        name: "load",
        catalogRef: "main.etl.load",
        catalogId: 6,
      },
    });
  });

  it("opens a schedule's flow and a column's table; others have none", () => {
    const parentFlow = { kind: "flow" as const, id: 6, name: "load", uri: "x" };
    expect(secondaryAction(hit({ kind: "schedule", parent: parentFlow }))?.label).toBe("Open flow");
    const parentTable = { kind: "table" as const, id: 4, name: "orders", uri: "y" };
    expect(secondaryAction(hit({ kind: "column", parent: parentTable }))?.label).toBe("Open table");
    expect(secondaryAction(hit({ kind: "table" }))).toBeNull();
  });
});

describe("hitKindLabel", () => {
  it("names a schema as a schema", () => {
    expect(hitKindLabel(hit({ kind: "namespace", details: { level: 0 } }))).toBe("Catalog");
    expect(hitKindLabel(hit({ kind: "namespace", details: { level: 1 } }))).toBe("Schema");
  });
});

describe("isGlobalSearchShortcut", () => {
  const key = (init: Partial<KeyboardEvent>) =>
    ({
      key: "F",
      code: "KeyF",
      metaKey: false,
      ctrlKey: false,
      shiftKey: false,
      altKey: false,
      defaultPrevented: false,
      ...init,
    }) as KeyboardEvent;

  it("is Shift+Cmd+F on macOS and Shift+Ctrl+F elsewhere", () => {
    expect(isGlobalSearchShortcut(key({ metaKey: true, shiftKey: true }), true)).toBe(true);
    expect(isGlobalSearchShortcut(key({ ctrlKey: true, shiftKey: true }), false)).toBe(true);
    expect(isGlobalSearchShortcut(key({ ctrlKey: true, shiftKey: true }), true)).toBe(false);
    expect(isGlobalSearchShortcut(key({ metaKey: true, shiftKey: true }), false)).toBe(false);
  });

  it("ignores Cmd/Ctrl+K, a missing Shift and Alt", () => {
    expect(isGlobalSearchShortcut(key({ metaKey: true }), true)).toBe(false);
    expect(isGlobalSearchShortcut(key({ metaKey: true, shiftKey: true, altKey: true }), true)).toBe(
      false,
    );
    expect(isGlobalSearchShortcut(key({ key: "k", code: "KeyK", metaKey: true }), true)).toBe(
      false,
    );
    expect(
      isGlobalSearchShortcut(key({ metaKey: true, shiftKey: true, defaultPrevented: true }), true),
    ).toBe(false);
  });

  it("ignores other letters with the same modifiers", () => {
    for (const [k, code] of [
      ["g", "KeyG"],
      ["p", "KeyP"],
      ["Enter", "Enter"],
    ]) {
      expect(
        isGlobalSearchShortcut(key({ key: k, code, metaKey: true, shiftKey: true }), true),
      ).toBe(false);
    }
  });

  it("ignores a key an input method is composing", () => {
    const composing = key({ metaKey: true, shiftKey: true, isComposing: true });
    expect(isGlobalSearchShortcut(composing, true)).toBe(false);
    expect(isComposingKey(key({ keyCode: 229 }))).toBe(true);
    expect(isComposingKey(key({}))).toBe(false);
  });
});
