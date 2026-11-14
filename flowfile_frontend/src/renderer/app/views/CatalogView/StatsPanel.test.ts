// @vitest-environment happy-dom
import { afterEach, describe, expect, it, vi } from "vitest";

const getOverview = vi.fn();
vi.mock("../../api/catalog.api", () => ({
  CatalogApi: { getOverview: (...args: unknown[]) => getOverview(...args) },
}));
vi.mock("./RunOverviewPanel.vue", () => ({ default: { render: () => null } }));
vi.mock("./ScheduleOverviewPanel.vue", () => ({ default: { render: () => null } }));

import { mount, settle, type Mounted } from "../../__fixtures__/mount";
import { useCatalogStore } from "../../stores/catalog-store";
import type { CatalogStats } from "../../types";
import StatsPanel from "./StatsPanel.vue";

const emptyStats = {
  total_flows: 0,
  total_runs: 0,
  total_tables: 0,
  total_virtual_tables: 0,
  total_favorites: 0,
  total_table_favorites: 0,
  total_schedules: 0,
  favorite_tables: [],
} as unknown as CatalogStats;

const props = (stats: CatalogStats | null) => ({
  stats,
  flows: [],
  tables: [],
  favorites: [],
  runs: [],
});

let mounted: Mounted | undefined;
afterEach(() => mounted?.unmount());

const query = (selector: string) => mounted!.el.querySelector(selector);

describe("StatsPanel", () => {
  it("holds the stat grid's place, without the welcome block, until the overview lands", () => {
    mounted = mount(StatsPanel, props(null));

    expect(query('[data-testid="stats-pending"]')).not.toBeNull();
    expect(mounted.el.querySelectorAll(".stat-card")).toHaveLength(5);
    expect(mounted.text()).not.toContain("Welcome to Catalog");
    expect(mounted.text()).not.toContain("Getting Started");
  });

  it("shows the welcome block only for a catalog that loaded empty", () => {
    mounted = mount(StatsPanel, props(emptyStats));

    expect(query('[data-testid="stats-pending"]')).toBeNull();
    expect(mounted.text()).toContain("Welcome to Catalog");
    expect(mounted.text()).toContain("Getting Started");
  });

  it("offers a retry when the first overview failed", async () => {
    mounted = mount(StatsPanel, props(null), () => {
      useCatalogStore().overviewError = "Network Error";
    });

    expect(query('[data-testid="stats-pending"]')).toBeNull();
    expect(mounted.text()).toContain("Couldn't load the catalog overview: Network Error");

    getOverview.mockReturnValueOnce(new Promise(() => undefined));
    (query(".overview-error button") as HTMLButtonElement).click();
    await settle();
    expect(getOverview).toHaveBeenCalledTimes(1);
    expect(query('[data-testid="stats-pending"]')).not.toBeNull();
  });
});
