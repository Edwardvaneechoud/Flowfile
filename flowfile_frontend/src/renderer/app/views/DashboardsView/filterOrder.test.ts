import { describe, expect, it } from "vitest";
import type { DashboardFilter } from "../../types";
import { moveFilter } from "./filterOrder";

const f = (id: string): DashboardFilter => ({
  id,
  field_name: id,
  kind: "categorical",
  state: {},
  target: "all",
  target_tile_ids: [],
  datasource_id: 1,
});

const ids = (filters: DashboardFilter[]) => filters.map((x) => x.id);

describe("moveFilter", () => {
  const filters = [f("a"), f("b"), f("c")];

  it("moves a filter forward", () => {
    expect(ids(moveFilter(filters, "a", "c", "after"))).toEqual(["b", "c", "a"]);
    expect(ids(moveFilter(filters, "a", "c", "before"))).toEqual(["b", "a", "c"]);
  });

  it("moves a filter backward", () => {
    expect(ids(moveFilter(filters, "c", "a", "before"))).toEqual(["c", "a", "b"]);
    expect(ids(moveFilter(filters, "c", "a", "after"))).toEqual(["a", "c", "b"]);
  });

  it("returns the same array when dropped next to itself", () => {
    expect(moveFilter(filters, "b", "a", "after")).toBe(filters);
    expect(moveFilter(filters, "b", "c", "before")).toBe(filters);
  });

  it("returns the same array for equal or unknown ids", () => {
    expect(moveFilter(filters, "a", "a", "after")).toBe(filters);
    expect(moveFilter(filters, "x", "a", "after")).toBe(filters);
    expect(moveFilter(filters, "a", "x", "before")).toBe(filters);
  });

  it("does not mutate the input", () => {
    moveFilter(filters, "a", "c", "after");
    expect(ids(filters)).toEqual(["a", "b", "c"]);
  });
});
