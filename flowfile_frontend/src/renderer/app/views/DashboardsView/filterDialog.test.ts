import { describe, expect, it } from "vitest";
import {
  draftTargets,
  filterSourceGroups,
  sourceFieldsOfKey,
  sourceIdsOfKey,
} from "./filterDialog";
import type { DashboardDatasource } from "../../composables/useDashboardDatasources";

describe("draftTargets", () => {
  it("saves no pick, or every candidate, as all tiles", () => {
    expect(draftTargets(["a", "b"], [], false)).toEqual({ target: "all", target_tile_ids: [] });
    expect(draftTargets(["a", "b"], ["b", "a"], false)).toEqual({
      target: "all",
      target_tile_ids: [],
    });
  });

  it("keeps an explicit pick of the candidates", () => {
    expect(draftTargets(["a", "b"], ["b", "gone"], false)).toEqual({
      target: "tiles",
      target_tile_ids: ["b"],
    });
  });

  it("keeps a filter whose targets were all removed filtering no tile", () => {
    expect(draftTargets(["a", "b"], [], true)).toEqual({ target: "tiles", target_tile_ids: [] });
  });
});

describe("source keys in the dialog", () => {
  it("turns a key into the table or chart reference, never both", () => {
    expect(sourceIdsOfKey("table:5")).toEqual({ datasource_id: 5, datasource_viz_id: null });
    expect(sourceIdsOfKey("viz:2")).toEqual({ datasource_id: null, datasource_viz_id: 2 });
  });

  it("offers the columns of the picked source", () => {
    const sources: DashboardDatasource[] = [
      {
        key: "viz:2",
        kind: "viz",
        id: 2,
        name: "q",
        schema_columns: [{ name: "region", dtype: "text" }],
        updated_at: null,
      },
    ];
    expect(sourceFieldsOfKey(sources, "viz:2")).toEqual([{ name: "region", dtype: "text" }]);
    expect(sourceFieldsOfKey(sources, "table:2")).toEqual([]);
    expect(sourceFieldsOfKey(sources, null)).toEqual([]);
  });
});

describe("filterSourceGroups", () => {
  const source = (key: string, name: string, kind: "table" | "viz"): DashboardDatasource => ({
    key,
    kind,
    id: Number(key.split(":")[1]),
    name,
    schema_columns: [],
    updated_at: null,
  });

  it("lists the dashboard's sources first, then every other catalog table, each by name", () => {
    const groups = filterSourceGroups(
      [source("table:2", "sales.orders", "table"), source("viz:7", "Top customers", "viz")],
      [
        { id: 5, name: "sales.returns" },
        { id: 2, name: "sales.orders" },
        { id: 3, name: "hr.people" },
      ],
    );
    expect(groups).toEqual([
      {
        label: "On this dashboard",
        options: [
          { key: "table:2", label: "sales.orders" },
          { key: "viz:7", label: "Top customers (SQL chart)" },
        ],
      },
      {
        label: "Other catalog tables",
        options: [
          { key: "table:3", label: "hr.people" },
          { key: "table:5", label: "sales.returns" },
        ],
      },
    ]);
  });

  it("offers every catalog table on an empty dashboard and leaves an empty group out", () => {
    const groups = filterSourceGroups([], [{ id: 1, name: "a" }]);
    expect(groups).toEqual([
      { label: "Other catalog tables", options: [{ key: "table:1", label: "a" }] },
    ]);
    expect(filterSourceGroups([], [])).toEqual([]);
  });

  it("keeps the edited filter's own source as a choice when no list holds it", () => {
    const current = { key: "table:9", label: "Table #9" };
    expect(filterSourceGroups([], [], current)).toEqual([
      { label: "On this dashboard", options: [current] },
    ]);
    const listed = filterSourceGroups([], [{ id: 9, name: "t" }], current);
    expect(listed).toHaveLength(1);
    expect(listed[0].options).toEqual([{ key: "table:9", label: "t" }]);
  });
});
