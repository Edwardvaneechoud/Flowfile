import { describe, expect, it } from "vitest";
import type { DashboardFilter } from "../../types";
import { decodeFilterState, encodeFilterState, filtersDiffer } from "./filterUrlState";

const filterOf = (over: Partial<DashboardFilter>): DashboardFilter => ({
  id: "f-region",
  field_name: "region",
  kind: "categorical",
  state: { selected: [] },
  target: "all",
  target_tile_ids: [],
  datasource_id: 5,
  ...over,
});

const saved = [
  filterOf({}),
  filterOf({
    id: "f-date",
    field_name: "order_date",
    kind: "date_range",
    state: { preset: "last_90_days" },
  }),
];

const withState = (id: string, state: Record<string, unknown>) =>
  saved.map((f) => (f.id === id ? { ...f, state } : f));

describe("encodeFilterState", () => {
  it("writes nothing while the viewer changed nothing", () => {
    expect(encodeFilterState(saved, saved)).toBe("");
    expect(filtersDiffer(saved, saved)).toBe(false);
  });

  it("writes only the filters that differ, in a URL-safe form", () => {
    const live = withState("f-region", { selected: ["Zuid-Öst", "North"] });
    const encoded = encodeFilterState(live, saved);
    expect(encoded).toMatch(/^[A-Za-z0-9_-]+$/);
    expect(filtersDiffer(live, saved)).toBe(true);
    expect(decodeFilterState(encoded, saved)).toEqual(live);
  });

  it("round-trips several changed filters", () => {
    const live = withState("f-date", { preset: "last_30_days" }).map((f) =>
      f.id === "f-region" ? { ...f, state: { selected: ["West"] } } : f,
    );
    expect(decodeFilterState(encodeFilterState(live, saved), saved)).toEqual(live);
  });
});

describe("decodeFilterState", () => {
  it("returns the saved filters for a missing or unreadable parameter", () => {
    for (const param of [undefined, null, "", "%%%", "bm90LWpzb24", ["a"]]) {
      expect(decodeFilterState(param, saved)).toEqual(saved);
    }
  });

  it("ignores a filter the dashboard no longer has", () => {
    const stale = encodeFilterState(
      [filterOf({ id: "gone", state: { selected: ["x"] } })],
      [filterOf({ id: "gone" })],
    );
    expect(decodeFilterState(stale, saved)).toEqual(saved);
  });

  it("hands back copies, so a viewer's edit never reaches the saved filters", () => {
    const decoded = decodeFilterState("", saved);
    expect(decoded[0]).not.toBe(saved[0]);
  });
});
