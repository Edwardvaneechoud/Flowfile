// @vitest-environment happy-dom
import { afterEach, describe, expect, it, vi } from "vitest";
import { mount, type Mounted } from "../../__fixtures__/mount";
import type { Dashboard } from "../../types";
import type { PreviewChart } from "./layoutPreview";

vi.mock("@element-plus/icons-vue", () => {
  const icon = { render: () => null };
  return { Delete: icon, Edit: icon, MoreFilled: icon, Share: icon, View: icon };
});

vi.mock("../../composables/useResourceSharing", () => ({
  useResourceSharing: () => ({ canManage: () => false, canShare: () => false }),
}));
vi.mock("../../components/sharing/SharedBadge.vue", () => ({ default: { render: () => null } }));

import DashboardCard from "./DashboardCard.vue";

const THUMB = "data:image/png;base64,iVBORw0KGgo=";

const dashboard = {
  id: 1,
  name: "Sales",
  description: null,
  namespace_name: null,
  updated_at: "2026-11-23T10:00:00Z",
  layout: {
    tiles: [
      { id: "with-thumb", type: "viz", viz_id: 10, chart_index: 0, x: 0, y: 0, w: 6, h: 4 },
      { id: "no-thumb", type: "viz", viz_id: 11, chart_index: 0, x: 6, y: 0, w: 6, h: 4 },
    ],
  },
} as unknown as Dashboard;

const charts: Record<number, PreviewChart> = {
  10: { thumbnail: THUMB, chartType: "line" },
  11: { thumbnail: null, chartType: "arc" },
};

let mounted: Mounted | undefined;
afterEach(() => mounted?.unmount());

const plots = (el: HTMLElement) => Array.from(el.querySelectorAll<HTMLElement>(".pv-plot"));

describe("a dashboard card's chart tiles", () => {
  it("show the saved thumbnail, else a sketch of the chart's mark", () => {
    mounted = mount(DashboardCard, { dashboard, charts });

    const [withThumb, noThumb] = plots(mounted.el);
    expect(withThumb.querySelector("img")?.getAttribute("src")).toBe(THUMB);
    expect(withThumb.querySelector(".sketch")).toBeNull();
    expect(noThumb.querySelector("img")).toBeNull();
    expect(noThumb.querySelector(".sketch--arc")).not.toBeNull();
  });

  it("stay empty until the chart library is known", () => {
    mounted = mount(DashboardCard, { dashboard });

    for (const plot of plots(mounted.el)) {
      expect(plot.querySelector("img, .sketch")).toBeNull();
    }
  });
});
