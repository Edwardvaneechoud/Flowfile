import { describe, expect, it } from "vitest";

import type { DashboardColorAssignment, DashboardLayout } from "../../types";
import {
  CATEGORY_DARK,
  CATEGORY_LIGHT,
} from "../../components/nodes/node-types/elements/exploreData/vueGraphicWalker/vizTheme";
import {
  OVER_PALETTE_DARK,
  OVER_PALETTE_LIGHT,
  PALETTE_SLOTS,
  assignColors,
  colorsForDomain,
  mergeAssignments,
  sameAssignments,
  withoutColorAssignments,
} from "./colorAssignment";

const a = (field: string, value: DashboardColorAssignment["value"], slot: number) => ({
  field,
  value,
  slot,
});

describe("assignColors", () => {
  it("hands out slots in the palette's order, values in Graphic Walker's order", () => {
    expect(assignColors([], "region", ["West", "East", "North"])).toEqual([
      a("region", "East", 0),
      a("region", "North", 1),
      a("region", "West", 2),
    ]);
  });

  it("is append-only: a value keeps its slot when a new one appears", () => {
    const first = assignColors([], "region", ["West", "North"]);
    const next = assignColors(first, "region", ["East", "West", "North"]);
    expect(next.slice(0, 2)).toEqual(first);
    expect(next[2]).toEqual(a("region", "East", 2));
  });

  it("hands back the same list when every value has a slot", () => {
    const held = [a("region", "West", 0)];
    expect(assignColors(held, "region", ["West"])).toBe(held);
  });

  it("gives the first free slot, so a slot a later UI cleared is used again", () => {
    const held = [a("region", "West", 0), a("region", "East", 2)];
    expect(assignColors(held, "region", ["North"]).at(-1)).toEqual(a("region", "North", 1));
  });

  it("keeps fields apart", () => {
    const held = assignColors([], "region", ["West"]);
    expect(assignColors(held, "segment", ["Home"]).at(-1)).toEqual(a("segment", "Home", 0));
  });

  it("gives no slot past the palette and never cycles", () => {
    const values = Array.from({ length: PALETTE_SLOTS + 3 }, (_, i) => `v${i + 1}`);
    const held = assignColors([], "code", values);
    expect(held).toHaveLength(PALETTE_SLOTS);
    expect(new Set(held.map((x) => x.slot)).size).toBe(PALETTE_SLOTS);
    expect(held.map((x) => x.value)).toEqual(values.slice(0, PALETTE_SLOTS));
    expect(assignColors(held, "code", ["v99"])).toBe(held);
  });

  it("tells 1 from '1' and colours null", () => {
    expect(assignColors([], "code", [1, "1", null])).toEqual([
      a("code", null, 0),
      a("code", 1, 1),
      a("code", "1", 2),
    ]);
  });
});

describe("colorsForDomain", () => {
  const held = [a("region", "East", 0), a("region", "North", 1), a("region", "West", 2)];

  it("colours a value by its slot whatever else the chart draws", () => {
    expect(colorsForDomain(held, "region", ["East", "North", "West"], false)).toEqual([
      CATEGORY_LIGHT[0],
      CATEGORY_LIGHT[1],
      CATEGORY_LIGHT[2],
    ]);
    // A filter took East and North away: West keeps its colour.
    expect(colorsForDomain(held, "region", ["West"], false)).toEqual([CATEGORY_LIGHT[2]]);
  });

  it("uses the same slot in the dark palette", () => {
    expect(colorsForDomain(held, "region", ["West"], true)).toEqual([CATEGORY_DARK[2]]);
  });

  it("draws a value with no slot in the neutral grey", () => {
    expect(colorsForDomain(held, "region", ["South"], false)).toEqual([OVER_PALETTE_LIGHT]);
    expect(colorsForDomain(held, "region", ["South"], true)).toEqual([OVER_PALETTE_DARK]);
    expect(colorsForDomain([a("region", "West", 40)], "region", ["West"], false)).toEqual([
      OVER_PALETTE_LIGHT,
    ]);
  });
});

describe("mergeAssignments", () => {
  it("keeps what is stored and adds the session's values on their own slots", () => {
    const stored = [a("region", "West", 0)];
    const current = [a("region", "West", 0), a("region", "East", 1)];
    expect(mergeAssignments(stored, current)).toBe(current);
    expect(mergeAssignments([], current)).toBe(current);
  });

  it("lets a stored slot win and moves the session's value to the first free one", () => {
    const stored = [a("region", "North", 1)];
    const current = [a("region", "East", 1)];
    expect(mergeAssignments(stored, current)).toEqual([
      a("region", "North", 1),
      a("region", "East", 0),
    ]);
  });

  it("takes a stored value's slot over the session's", () => {
    const stored = [a("region", "East", 3)];
    expect(mergeAssignments(stored, [a("region", "East", 0)])).toEqual(stored);
  });
});

describe("sameAssignments / withoutColorAssignments", () => {
  it("reads a missing list as empty", () => {
    expect(sameAssignments(undefined, [])).toBe(true);
    expect(sameAssignments([a("f", "x", 0)], [])).toBe(false);
  });

  it("drops the colours from a copy of the layout", () => {
    const layout = {
      tiles: [],
      grid: { cols: 48, row_height: 40, version: 2 },
      filters: [],
      color_assignments: [a("f", "x", 0)],
    } as DashboardLayout;
    expect(withoutColorAssignments(layout)).toEqual({
      tiles: [],
      grid: layout.grid,
      filters: [],
    });
    expect(layout.color_assignments).toHaveLength(1);
  });
});
