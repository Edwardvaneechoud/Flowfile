import { describe, expect, it } from "vitest";
import type { DashboardFilter } from "../../types";
import {
  allowsSeveral,
  escapeRegExp,
  matchesQuery,
  selectedValues,
  selectionState,
  valueOptions,
  valueSearchPayload,
  valueSearchResult,
  withMultiple,
} from "./filterValues";

const filterOf = (over: Partial<DashboardFilter> = {}): DashboardFilter => ({
  id: "f",
  field_name: "region",
  kind: "categorical",
  state: { selected: ["EU", "US"] },
  target: "all",
  target_tile_ids: [],
  datasource_id: 5,
  ...over,
});

describe("single-select state", () => {
  it("allows several values unless the filter says otherwise, older layouts included", () => {
    expect(allowsSeveral(filterOf())).toBe(true);
    expect(allowsSeveral(filterOf({ multiple: true }))).toBe(true);
    expect(allowsSeveral(filterOf({ multiple: false }))).toBe(false);
  });

  it("reads at most one chosen value in single mode", () => {
    expect(selectedValues(filterOf())).toEqual(["EU", "US"]);
    expect(selectedValues(filterOf({ multiple: false }))).toEqual(["EU"]);
    expect(selectedValues(filterOf({ state: { selected: [7] } }))).toEqual(["7"]);
    expect(selectedValues(filterOf({ state: {} }))).toEqual([]);
  });

  it("keeps the selected list shape for one value, a cleared input and a list", () => {
    const single = filterOf({ multiple: false, state: { selected: [], other: 1 } });
    expect(selectionState(single, "West")).toEqual({ selected: ["West"], other: 1 });
    expect(selectionState(single, undefined)).toEqual({ selected: [], other: 1 });
    expect(selectionState(single, "")).toEqual({ selected: [], other: 1 });
    expect(selectionState(single, ["a", "b"])).toEqual({ selected: ["a"], other: 1 });
    expect(selectionState(filterOf(), ["a", "b"])).toEqual({ selected: ["a", "b"] });
  });

  it("keeps the first chosen value when a filter goes single", () => {
    const single = withMultiple(filterOf(), false);
    expect(single.multiple).toBe(false);
    expect(single.state).toEqual({ selected: ["EU"] });
    expect(withMultiple(single, true)).toEqual({ ...single, multiple: true });
    const range = filterOf({ kind: "numeric_range", state: { min: 1, max: 2 } });
    expect(withMultiple(range, false).state).toBe(range.state);
  });
});

describe("value search", () => {
  it("escapes every regex metacharacter so typed text matches literally", () => {
    expect(escapeRegExp("a.b*c+d?(e)[f]{g}^h$i|j\\k#l&m-n~o")).toBe(
      "a\\.b\\*c\\+d\\?\\(e\\)\\[f\\]\\{g\\}\\^h\\$i\\|j\\\\k\\#l\\&m\\-n\\~o",
    );
    expect(escapeRegExp("West")).toBe("West");
  });

  it("asks for the distinct values containing the text, sorted, one past the cap", () => {
    expect(valueSearchPayload("region", "w.e", 100)).toEqual({
      workflow: [
        {
          type: "filter",
          filters: [
            { fid: "region", rule: { type: "regexp", value: "w\\.e", caseSensitive: false } },
          ],
        },
        { type: "view", query: [{ op: "aggregate", groupBy: ["region"], measures: [] }] },
        { type: "sort", by: ["region"], sort: "ascending" },
      ],
      limit: 101,
    });
  });

  it("lists the first values when nothing is typed", () => {
    const payload = valueSearchPayload("region", "", 20);
    expect((payload.workflow as { type: string }[]).map((s) => s.type)).toEqual(["view", "sort"]);
    expect(payload.limit).toBe(21);
  });

  it("reads the rows, leaves out a missing value and tells a capped list", () => {
    const rows = [{ region: "A" }, { region: null }, { region: 3 }, { region: "D" }];
    expect(valueSearchResult(rows, "region", 3)).toEqual({ values: ["A", "3"], capped: true });
    expect(valueSearchResult(rows.slice(0, 2), "region", 3)).toEqual({
      values: ["A"],
      capped: false,
    });
  });

  it("matches like the search, ignoring case", () => {
    expect(matchesQuery("West", "wE")).toBe(true);
    expect(matchesQuery("West", "x")).toBe(false);
    expect(matchesQuery("West", "")).toBe(true);
  });

  it("keeps a chosen value an option when the search does not return it", () => {
    expect(valueOptions(["West", "East"], ["North", "West"])).toEqual(["West", "East", "North"]);
  });
});
