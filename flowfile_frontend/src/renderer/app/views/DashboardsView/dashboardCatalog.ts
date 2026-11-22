// Which catalog keeps a dashboard, where a new one is saved, and the charts and tables it may then use. Pure.
import type { SaveTargetOption } from "../../composables/useWritableNamespaces";
import {
  NO_HUB_SCHEMA_TO_SAVE_IN,
  isHubRow,
  isOffered,
  unavailableReason,
} from "../CatalogView/hubRows";

interface Row {
  id: number;
  source?: string | null;
}

interface Kept {
  id: number;
  namespace_id?: number | null;
}

const onHub = (row: Row): boolean => isHubRow(row) || row.id < 0;

/** Whether a hub's catalog keeps the dashboard: by its own id once saved, by the schema picked for it before. */
export const dashboardOnHub = (dashboard: Kept | null | undefined): boolean =>
  !!dashboard && (dashboard.id < 0 || (dashboard.namespace_id ?? 0) < 0);

export interface DashboardTargets {
  /** Whether the editor asks where a new dashboard goes; without a hub in the tree it never did. */
  asks: boolean;
  /** The hub's schemas the person may write; this machine's catalog is the choice of none of them. */
  options: SaveTargetOption[];
  defaultId: number | null;
  /** Why a new dashboard cannot be saved at all. */
  blocked: string | null;
}

/**
 * Where a new dashboard is saved: on this machine where its catalog is offered, with the hub's schemas beside it,
 * and in the first hub schema the person may write on a server that joined a hub.
 */
export function dashboardTargets(
  targets: SaveTargetOption[],
  shown: { ownOffered: boolean; hubShown: boolean },
): DashboardTargets {
  if (!shown.hubShown) return { asks: false, options: [], defaultId: null, blocked: null };
  const options = targets.filter((o) => o.hub);
  if (shown.ownOffered) return { asks: true, options, defaultId: null, blocked: null };
  const blocked = options.length ? null : NO_HUB_SCHEMA_TO_SAVE_IN;
  return { asks: true, options, defaultId: options[0]?.id ?? null, blocked };
}

/** The charts a dashboard may show: those of the catalog that keeps it, since core refuses any other. */
export function chartsOfDashboardCatalog<T extends Row>(
  charts: readonly T[],
  dashboard: Kept | null | undefined,
): T[] {
  const hub = dashboardOnHub(dashboard);
  return charts.filter((chart) => onHub(chart) === hub);
}

interface TableHolder<T> {
  source?: string | null;
  offered?: boolean;
  unavailable_reason?: string | null;
  tables: T[];
  children: TableHolder<T>[];
}

/**
 * The tables a dashboard may read directly: a hub dashboard the tables of the hub's catalogs this Flowfile can
 * use, taken from the tree; a dashboard of this machine the list it always had.
 */
export function tablesOfDashboardCatalog<T extends Row>(
  local: readonly T[],
  tree: readonly TableHolder<T>[],
  dashboard: Kept | null | undefined,
): T[] {
  if (!dashboardOnHub(dashboard)) return [...local];
  const under = (holder: TableHolder<T>): T[] => [
    ...holder.tables,
    ...holder.children.flatMap(under),
  ];
  return tree
    .filter((root) => isHubRow(root) && isOffered(root) && unavailableReason(root) === null)
    .flatMap(under);
}
