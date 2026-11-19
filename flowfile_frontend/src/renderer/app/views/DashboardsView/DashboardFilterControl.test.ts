// @vitest-environment happy-dom
import { defineComponent, h, ref } from "vue";
import { describe, expect, it, vi } from "vitest";
import { element, mountIn, settle } from "../../__fixtures__/mount";
import type { ColumnStatsResponse, DashboardFilter } from "../../types";
import DashboardFilterControl from "./DashboardFilterControl.vue";

const amount: DashboardFilter = {
  id: "f1",
  field_name: "amount",
  kind: "numeric_range",
  state: {},
  target: "all",
  target_tile_ids: [],
  datasource_id: 1,
  datasource_viz_id: null,
  scope: "datasource",
  multiple: true,
} as DashboardFilter;

const stats = { values: [], truncated: false, min: 5, max: 90 } as unknown as ColumnStatsResponse;

const mountControl = async () => {
  const filter = ref(amount);
  const getColumnStats = vi.fn().mockResolvedValue(stats);
  const host = await mountIn(
    defineComponent({
      setup: () => () => h(DashboardFilterControl, { filter: filter.value, getColumnStats }),
    }),
    { stubs: { "el-input-number": element("input") } },
  );
  const placeholders = () =>
    Array.from(host.querySelectorAll("input")).map((i) => i.getAttribute("placeholder"));
  return { filter, placeholders };
};

describe("the filter control's value list", () => {
  it("drops the previous source's values when the filter is bound to a source that lists none", async () => {
    const { filter, placeholders } = await mountControl();
    expect(placeholders()).toEqual(["≥ 5", "≤ 90"]);

    filter.value = { ...amount, datasource_id: null, datasource_viz_id: 7 };
    await settle();

    expect(placeholders()).toEqual(["Min", "Max"]);
  });
});
