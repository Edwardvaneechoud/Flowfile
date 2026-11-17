import { describe, expect, it } from "vitest";

import type { IChart } from "@kanaries/graphic-walker/interfaces";
import {
  categoryColorField,
  colorDomain,
  colorValueKey,
  createWorksheetColorTracker,
  withCategoryRange,
  worksheetColorDomain,
} from "./chartColors";

const field = (fid: string, extra: Record<string, unknown> = {}) => ({
  fid,
  name: fid,
  semanticType: "nominal",
  analyticType: "dimension",
  ...extra,
});

const chartWith = (
  color: Record<string, unknown>[],
  layout: Record<string, unknown> = {},
  config: Record<string, unknown> = {},
): IChart =>
  ({
    visId: "c",
    name: "c",
    encodings: { color, rows: [], columns: [] },
    config: { geoms: ["bar"], coordSystem: "generic", ...config },
    layout,
  }) as unknown as IChart;

describe("colorDomain", () => {
  it("orders values the way the worksheet renderer lists them", () => {
    expect(colorDomain(["West", "east", "North", "East"])).toEqual([
      "east",
      "East",
      "North",
      "West",
    ]);
    expect(colorDomain(["item 10", "item 2", "item 1"])).toEqual(["item 1", "item 2", "item 10"]);
    expect(colorDomain([10, 2, 1])).toEqual([1, 2, 10]);
  });

  it("lists a value once, null included, as the text (null)", () => {
    expect(colorDomain(["z", null, "z", null])).toEqual([null, "z"]);
  });

  it("tells 1 from '1' but not 1 from 1.0", () => {
    expect(colorValueKey(1)).toBe(colorValueKey(1.0));
    expect(colorValueKey(1)).not.toBe(colorValueKey("1"));
    expect(colorDomain([1, "1", 1.0])).toHaveLength(2);
  });
});

describe("categoryColorField", () => {
  it("names a nominal or ordinal colour dimension", () => {
    expect(categoryColorField(chartWith([field("region")]))).toBe("region");
    expect(categoryColorField(chartWith([field("size", { semanticType: "ordinal" })]))).toBe(
      "size",
    );
    expect(categoryColorField(chartWith([field("region")], { renderer: "vega-lite" }))).toBe(
      "region",
    );
  });

  it("leaves measures, quantitative and temporal colours, a picked palette and other renderers alone", () => {
    expect(categoryColorField(chartWith([]))).toBeNull();
    expect(
      categoryColorField(chartWith([field("amount", { analyticType: "measure" })])),
    ).toBeNull();
    expect(
      categoryColorField(chartWith([field("n", { semanticType: "quantitative" })])),
    ).toBeNull();
    expect(categoryColorField(chartWith([field("d", { semanticType: "temporal" })]))).toBeNull();
    expect(categoryColorField(chartWith([field("region")], { colorPalette: "set2" }))).toBeNull();
    expect(
      categoryColorField(chartWith([field("region")], { renderer: "observable-plot" })),
    ).toBeNull();
    expect(
      categoryColorField(chartWith([field("region")], {}, { coordSystem: "geographic" })),
    ).toBeNull();
    expect(categoryColorField(chartWith([field("region")], {}, { geoms: ["table"] }))).toBeNull();
  });
});

describe("worksheetColorDomain", () => {
  it("collects the field's values over every answered query", () => {
    const domain = worksheetColorDomain(
      [
        { limited: false, rows: [{ region: "West" }, { region: "East" }] },
        { limited: false, rows: [{ region: "North" }, { region: "West" }] },
      ],
      "region",
    );
    expect(domain).toEqual(["East", "North", "West"]);
  });

  it("skips rows without the field, as a ranking query's", () => {
    expect(
      worksheetColorDomain(
        [{ limited: false, rows: [{ region: "West" }, { segment: "Home", order: 3 }] }],
        "region",
      ),
    ).toEqual(["West"]);
  });

  it("takes only the limited queries when the chart has a row limit", () => {
    expect(
      worksheetColorDomain(
        [
          { limited: true, rows: [{ region: "West" }] },
          { limited: false, rows: [{ region: "West" }, { region: "East" }] },
        ],
        "region",
      ),
    ).toEqual(["West"]);
  });
});

describe("createWorksheetColorTracker", () => {
  it("adds every query of one round", () => {
    const tracker = createWorksheetColorTracker();
    const first = tracker.sent({ workflow: [1] }, "region");
    const second = tracker.sent({ workflow: [2] }, "region");
    expect(first([{ region: "West" }])).toEqual(["West"]);
    expect(second([{ region: "East" }])).toEqual(["East", "West"]);
  });

  it("starts over with the first query after the chart changed and ignores the old round", () => {
    const tracker = createWorksheetColorTracker();
    const old = tracker.sent({ workflow: [1] }, "region");
    expect(old([{ region: "West" }])).toEqual(["West"]);
    tracker.newRound();
    const stale = tracker.sent({ workflow: [1] }, "region");
    const fresh = tracker.sent({ workflow: [2] }, "region");
    expect(fresh([{ region: "East" }])).toEqual(["East"]);
    tracker.newRound();
    tracker.sent({ workflow: [3] }, "region");
    expect(stale([{ region: "North" }])).toBeNull();
  });

  it("keeps the round when the chart changed but nothing was sent again", () => {
    const tracker = createWorksheetColorTracker();
    const pending = tracker.sent({ workflow: [1] }, "region");
    tracker.newRound();
    expect(pending([{ region: "West" }])).toEqual(["West"]);
  });

  it("reads a row limit from the query", () => {
    const tracker = createWorksheetColorTracker();
    const limited = tracker.sent({ workflow: [1], limit: 1 }, "region");
    const ranking = tracker.sent({ workflow: [2], limit: undefined }, "region");
    limited([{ region: "West" }]);
    expect(ranking([{ region: "West" }, { region: "East" }])).toEqual(["West"]);
  });
});

describe("withCategoryRange", () => {
  it("sets the colours as the chart's category range on a copy", () => {
    const chart = chartWith([field("region")], { format: {} });
    const drawn = withCategoryRange(chart, ["#111111", "#222222"]);
    expect(drawn.layout.colorPalette).toEqual(["#111111", "#222222"]);
    expect(drawn.layout.format).toEqual({});
    expect(chart.layout.colorPalette).toBeUndefined();
  });

  it("hands back the chart itself without colours", () => {
    const chart = chartWith([field("region")]);
    expect(withCategoryRange(chart, null)).toBe(chart);
  });
});
