// Which saved charts a list shows. A chart made from inside a dashboard belongs to that dashboard:
// it is listed with it, and in the chart library only when asked for.

import type { CatalogVisualization } from "../../types";

type Owned = Pick<CatalogVisualization, "dashboard_id" | "dashboard_name">;

export const belongsToDashboard = (chart: Pick<Owned, "dashboard_id">): boolean =>
  chart.dashboard_id != null;

/** The Charts list: the charts of their own, and with `includeOwned` the dashboards' charts too. */
export function libraryCharts<T extends Owned>(charts: readonly T[], includeOwned: boolean): T[] {
  return includeOwned ? [...charts] : charts.filter((c) => !belongsToDashboard(c));
}

/** The charts a dashboard may put on a tile: the charts of their own and its own, never another's. */
export function chartsForDashboard<T extends Owned>(
  charts: readonly T[],
  dashboardId: number | null | undefined,
): T[] {
  return charts.filter((c) => !belongsToDashboard(c) || c.dashboard_id === dashboardId);
}

/** The tag an owned chart's card carries; null for a chart of its own. */
export function ownerLabel(chart: Owned): string | null {
  if (!belongsToDashboard(chart)) return null;
  return chart.dashboard_name ? `On ${chart.dashboard_name}` : "On a dashboard";
}

const INCLUDE_OWNED_KEY = "flowfile.charts.include_dashboard_charts";

/** Whether the Charts list was last left showing the dashboards' charts. */
export function readIncludeOwned(storage: Pick<Storage, "getItem"> | undefined): boolean {
  try {
    return storage?.getItem(INCLUDE_OWNED_KEY) === "1";
  } catch {
    return false;
  }
}

export function writeIncludeOwned(
  storage: Pick<Storage, "setItem"> | undefined,
  include: boolean,
): void {
  try {
    storage?.setItem(INCLUDE_OWNED_KEY, include ? "1" : "0");
  } catch {
    // The choice just is not remembered.
  }
}
