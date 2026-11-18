import { inject, provide, type InjectionKey } from "vue";
import type { DashboardTile } from "../types";
import { imageBytesOf } from "../views/DashboardsView/imageTile";

/** The image bytes the whole dashboard (every tab) holds outside one tile, or all of them for null. */
export type DashboardImageBudget = (exceptTileId: string | null) => number;

const KEY: InjectionKey<DashboardImageBudget> = Symbol("dashboardImageBudget");

/** The editor provides it, since a canvas sees only its own tab's tiles. */
export const provideDashboardImageBudget = (tiles: () => readonly DashboardTile[]): void =>
  provide(KEY, (exceptTileId) => imageBytesOf(tiles(), exceptTileId));

/** Without a provider nothing is counted; core still refuses a layout over the cap. */
export const useDashboardImageBudget = (): DashboardImageBudget => inject(KEY, () => 0);
