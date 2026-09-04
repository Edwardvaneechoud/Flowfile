import { describe, expect, it, vi } from "vitest";

vi.mock("../api/catalog.api", () => ({ CatalogApi: {} }));
vi.mock("./useGraphicWalkerCompute", () => ({ useGraphicWalkerCompute: vi.fn() }));

import { filtersTargetingTile, type TileField } from "./useDashboardComputation";
import type { DashboardFilter } from "../types";

const filterOf = (over: Partial<DashboardFilter>): DashboardFilter => ({
  id: "f",
  field_name: "region",
  kind: "categorical",
  state: { selected: ["EU"] },
  target: "all",
  target_tile_ids: [],
  datasource_id: 5,
  ...over,
});

const datasourceOf = (map: Record<string, number | null>) => (tileId: string) =>
  map[tileId] ?? null;

const fieldsOf = (spec: Record<string, string | undefined>): TileField[] =>
  Object.entries(spec).map(([fid, semanticType]) => ({ fid, semanticType }));

describe("filtersTargetingTile", () => {
  it("datasource scope applies only to tiles on the same table", () => {
    const f = filterOf({});
    const ds = datasourceOf({ same: 5, other: 7, sql: null });
    const fields = fieldsOf({ region: "nominal" });

    expect(filtersTargetingTile([f], "same", ds, fields)).toEqual([f]);
    expect(filtersTargetingTile([f], "other", ds, fields)).toEqual([]);
    expect(filtersTargetingTile([f], "sql", ds, fields)).toEqual([]);
  });

  it("datasource scope ignores the tile's column types", () => {
    const f = filterOf({});
    const ds = datasourceOf({ same: 5 });
    expect(filtersTargetingTile([f], "same", ds, fieldsOf({ region: "quantitative" }))).toEqual([
      f,
    ]);
    expect(filtersTargetingTile([f], "same", ds, fieldsOf({ amount: "nominal" }))).toEqual([f]);
  });

  it("all scope applies to any tile whose data has the column", () => {
    const f = filterOf({ scope: "all" });
    const ds = datasourceOf({ same: 5, other: 7, sql: null });

    const withColumn = fieldsOf({ region: "nominal", amount: "quantitative" });
    expect(filtersTargetingTile([f], "other", ds, withColumn)).toEqual([f]);
    expect(filtersTargetingTile([f], "sql", ds, withColumn)).toEqual([f]);
    expect(filtersTargetingTile([f], "other", ds, fieldsOf({ amount: "quantitative" }))).toEqual(
      [],
    );
  });

  it("all scope needs the column's type to fit the filter kind", () => {
    const ds = datasourceOf({ t: 7 });
    const categorical = filterOf({ scope: "all" });
    const numeric = filterOf({ scope: "all", kind: "numeric_range", state: { min: 1, max: 9 } });
    const dates = filterOf({
      scope: "all",
      kind: "date_range",
      state: { start: "2024-01-01", end: null },
    });

    expect(filtersTargetingTile([categorical], "t", ds, fieldsOf({ region: "ordinal" }))).toEqual([
      categorical,
    ]);
    expect(
      filtersTargetingTile([categorical], "t", ds, fieldsOf({ region: "quantitative" })),
    ).toEqual([]);
    expect(filtersTargetingTile([numeric], "t", ds, fieldsOf({ region: "quantitative" }))).toEqual([
      numeric,
    ]);
    expect(filtersTargetingTile([numeric], "t", ds, fieldsOf({ region: "nominal" }))).toEqual([]);
    expect(filtersTargetingTile([dates], "t", ds, fieldsOf({ region: "temporal" }))).toEqual([
      dates,
    ]);
    expect(filtersTargetingTile([dates], "t", ds, fieldsOf({ region: "nominal" }))).toEqual([]);
  });

  it("all scope lets unclassified columns through", () => {
    const f = filterOf({ scope: "all", kind: "date_range", state: { start: "2024-01-01" } });
    const ds = datasourceOf({ t: 7 });
    expect(filtersTargetingTile([f], "t", ds, fieldsOf({ region: undefined }))).toEqual([f]);
    expect(filtersTargetingTile([f], "t", ds, fieldsOf({ region: "?" }))).toEqual([f]);
  });

  it("skips the column gate while the tile's fields are unknown", () => {
    const f = filterOf({ scope: "all" });
    expect(filtersTargetingTile([f], "any", datasourceOf({}), null)).toEqual([f]);
  });

  it("legacy untied filters behave as cross-source", () => {
    const f = filterOf({ datasource_id: null });
    const ds = datasourceOf({ t: 9 });

    expect(filtersTargetingTile([f], "t", ds, fieldsOf({ region: "nominal" }))).toEqual([f]);
    expect(filtersTargetingTile([f], "t", ds, fieldsOf({ amount: "nominal" }))).toEqual([]);
    expect(filtersTargetingTile([f], "t", ds, fieldsOf({ region: "temporal" }))).toEqual([]);
  });

  it("explicit tile targets still gate after the source check", () => {
    const f = filterOf({ scope: "all", target: "tiles", target_tile_ids: ["a"] });
    const ds = datasourceOf({ a: 7, b: 7 });
    const fields = fieldsOf({ region: "nominal" });

    expect(filtersTargetingTile([f], "a", ds, fields)).toEqual([f]);
    expect(filtersTargetingTile([f], "b", ds, fields)).toEqual([]);
  });
});
