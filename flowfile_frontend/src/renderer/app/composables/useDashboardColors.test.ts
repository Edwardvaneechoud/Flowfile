import { describe, expect, it } from "vitest";
import { nextTick, ref } from "vue";

import type { DashboardColorAssignment } from "../types";
import { CATEGORY_LIGHT } from "../components/nodes/node-types/elements/exploreData/vueGraphicWalker/vizTheme";
import { createDashboardColors } from "./useDashboardColors";

describe("createDashboardColors", () => {
  it("gives two tiles on one field the same colour per value", () => {
    const colors = createDashboardColors(() => []);
    const first = colors.categoryColors.value("region", ["East", "West"], false);
    const second = colors.categoryColors.value("region", ["West"], false);
    expect(first).toEqual([CATEGORY_LIGHT[0], CATEGORY_LIGHT[1]]);
    expect(second).toEqual([CATEGORY_LIGHT[1]]);
  });

  it("starts from the stored slots", () => {
    const stored: DashboardColorAssignment[] = [{ field: "region", value: "West", slot: 5 }];
    const colors = createDashboardColors(() => stored);
    expect(colors.categoryColors.value("region", ["East", "West"], false)).toEqual([
      CATEGORY_LIGHT[0],
      CATEGORY_LIGHT[5],
    ]);
    expect(colors.sync()).toEqual([...stored, { field: "region", value: "East", slot: 0 }]);
  });

  it("hands tiles a new function only when an assignment changed", () => {
    const colors = createDashboardColors(() => []);
    const before = colors.categoryColors.value;
    before("region", ["West"], false);
    const after = colors.categoryColors.value;
    expect(after).not.toBe(before);
    after("region", ["West"], false);
    expect(colors.categoryColors.value).toBe(after);
  });

  it("leaves a chart whose values cannot be stored to Graphic Walker", () => {
    const colors = createDashboardColors(() => []);
    expect(colors.categoryColors.value("bin", [[0, 10]], false)).toBeNull();
    expect(colors.categoryColors.value("region", [], false)).toBeNull();
    expect(colors.sync()).toEqual([]);
  });

  it("keeps the session's colours when the stored list is replaced", async () => {
    const stored = ref<DashboardColorAssignment[] | undefined>(undefined);
    const colors = createDashboardColors(() => stored.value);
    colors.categoryColors.value("region", ["West"], false);
    stored.value = [{ field: "region", value: "North", slot: 0 }];
    await nextTick();
    expect(colors.sync()).toEqual([
      { field: "region", value: "North", slot: 0 },
      { field: "region", value: "West", slot: 1 },
    ]);
  });
});
