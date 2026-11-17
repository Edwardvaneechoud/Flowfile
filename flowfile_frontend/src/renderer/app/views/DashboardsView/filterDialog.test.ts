import { describe, expect, it } from "vitest";
import { draftTargets, sourceFieldsOfKey, sourceIdsOfKey } from "./filterDialog";
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
