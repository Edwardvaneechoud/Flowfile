import { describe, expect, it } from "vitest";

import { computeCloseTargets, tabMenuOptions } from "./flowTabActions";

const ids = [10, 20, 30, 40];

describe("computeCloseTargets", () => {
  it("close returns only the target", () => {
    expect(computeCloseTargets(ids, 20, "close")).toEqual([20]);
  });

  it("close-others returns everything but the target, in tab order", () => {
    expect(computeCloseTargets(ids, 20, "close-others")).toEqual([10, 30, 40]);
  });

  it("close-all returns all ids in tab order", () => {
    expect(computeCloseTargets(ids, 20, "close-all")).toEqual([10, 20, 30, 40]);
  });

  it("close-left returns tabs before the target", () => {
    expect(computeCloseTargets(ids, 30, "close-left")).toEqual([10, 20]);
  });

  it("close-left on the first tab returns nothing", () => {
    expect(computeCloseTargets(ids, 10, "close-left")).toEqual([]);
  });

  it("close-right returns tabs after the target", () => {
    expect(computeCloseTargets(ids, 20, "close-right")).toEqual([30, 40]);
  });

  it("close-right on the last tab returns nothing", () => {
    expect(computeCloseTargets(ids, 40, "close-right")).toEqual([]);
  });

  it("close-others with a single tab returns nothing", () => {
    expect(computeCloseTargets([10], 10, "close-others")).toEqual([]);
  });

  it("unknown target returns nothing for every action", () => {
    for (const action of [
      "close",
      "close-others",
      "close-all",
      "close-left",
      "close-right",
    ] as const) {
      expect(computeCloseTargets(ids, 99, action)).toEqual([]);
    }
  });

  it("does not mutate the input array", () => {
    const input = [1, 2, 3];
    computeCloseTargets(input, 2, "close-all");
    expect(input).toEqual([1, 2, 3]);
  });
});

describe("tabMenuOptions", () => {
  const actions = (opts: ReturnType<typeof tabMenuOptions>) => opts.map((o) => o.action);
  const disabledOf = (opts: ReturnType<typeof tabMenuOptions>, action: string) =>
    opts.find((o) => o.action === action)?.disabled ?? false;

  it("offers the full browser set", () => {
    expect(actions(tabMenuOptions(ids, 20))).toEqual([
      "rename",
      "close",
      "close-others",
      "close-all",
      "close-left",
      "close-right",
    ]);
  });

  it("disables close-left on the first tab only", () => {
    expect(disabledOf(tabMenuOptions(ids, 10), "close-left")).toBe(true);
    expect(disabledOf(tabMenuOptions(ids, 20), "close-left")).toBe(false);
  });

  it("disables close-right on the last tab only", () => {
    expect(disabledOf(tabMenuOptions(ids, 40), "close-right")).toBe(true);
    expect(disabledOf(tabMenuOptions(ids, 30), "close-right")).toBe(false);
  });

  it("disables close-others (and both directions) with a single tab", () => {
    const opts = tabMenuOptions([10], 10);
    expect(disabledOf(opts, "close-others")).toBe(true);
    expect(disabledOf(opts, "close-left")).toBe(true);
    expect(disabledOf(opts, "close-right")).toBe(true);
  });
});
