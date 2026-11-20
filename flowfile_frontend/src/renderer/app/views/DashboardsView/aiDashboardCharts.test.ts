// Unit tests for the chart converter: Graphic Walker's own functions through the deep imports.

import { describe, expect, it, vi } from "vitest";

vi.mock("../../api/catalog.api", () => ({ CatalogApi: {} }));

import type { IMutField } from "@kanaries/graphic-walker/interfaces";
import { normalize } from "@kanaries/graphic-walker/models/normalize";
import {
  expandTerse,
  parseShorthand,
  projectTerse,
  terseComputedFid,
} from "@kanaries/graphic-walker/models/terse";
import { chartToWorkflow } from "@kanaries/graphic-walker/utils/workflow";

import type { GraphicWalkerToolkit } from "../CatalogView/graphicWalkerFunctions";
import { NOT_PREPARED, createChartConverter } from "./aiDashboardCharts";

const toolkit: GraphicWalkerToolkit = {
  normalize,
  expandTerse,
  projectTerse,
  parseShorthand,
  terseComputedFid,
  chartToWorkflow: (chart) => chartToWorkflow(chart) as unknown as Record<string, unknown>,
};

const FIELDS: IMutField[] = [
  { fid: "region", name: "region", semanticType: "nominal", analyticType: "dimension" },
  { fid: "revenue", name: "revenue", semanticType: "quantitative", analyticType: "measure" },
];

const deps = (
  loadFields = vi.fn<(tableId: number) => Promise<IMutField[]>>(async () => FIELDS),
) => ({
  loadToolkit: vi.fn(async () => toolkit),
  loadFields,
});

describe("createChartConverter", () => {
  it("builds the chart and the aggregate query its tile runs", async () => {
    const convert = createChartConverter(deps());
    const result = await convert(1, {
      mark: "bar",
      x: { field: "region" },
      y: { field: "revenue", aggregate: "sum" },
    });
    expect(result.ok).toBe(true);
    if (!result.ok) return;
    expect(result.chart.encodings.rows.map((f) => f.fid)).toContain("revenue");
    const view = (result.payload?.workflow as { type: string; query: any[] }[]).find(
      (step) => step.type === "view",
    );
    expect(view?.query[0]).toMatchObject({ op: "aggregate", groupBy: ["region"] });
    expect(view?.query[0].measures[0]).toMatchObject({ field: "revenue", agg: "sum" });
  });

  it("reads each table's fields once and another table's separately", async () => {
    const loadFields = vi.fn<(tableId: number) => Promise<IMutField[]>>(async () => FIELDS);
    const convert = createChartConverter(deps(loadFields));
    const request = { mark: "bar" as const, x: { field: "region" } };
    await Promise.all([convert(1, request), convert(1, request), convert(2, request)]);
    expect(loadFields.mock.calls.map(([id]) => id)).toEqual([1, 2]);
  });

  it("returns the converter's refusal for a column the table does not have", async () => {
    const result = await createChartConverter(deps())(1, { mark: "bar", x: { field: "nope" } });
    expect(result.ok).toBe(false);
    if (!result.ok) expect(result.error).toContain("nope");
  });

  it("never rejects when the fields cannot be read, and asks again next time", async () => {
    const loadFields = vi.fn<(tableId: number) => Promise<IMutField[]>>(async () => FIELDS);
    loadFields.mockRejectedValueOnce(new Error("worker down"));
    const convert = createChartConverter(deps(loadFields));
    const request = { mark: "bar" as const, x: { field: "region" } };
    expect(await convert(1, request)).toEqual({ ok: false, error: NOT_PREPARED });
    expect((await convert(1, request)).ok).toBe(true);
    expect(loadFields).toHaveBeenCalledTimes(2);
  });
});
