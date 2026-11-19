import {
  computed,
  inject,
  provide,
  shallowRef,
  watch,
  type ComputedRef,
  type InjectionKey,
} from "vue";
import type { DashboardColorAssignment } from "../types";
import type { CategoryColors } from "../components/nodes/node-types/elements/exploreData/vueGraphicWalker/chartColors";
import {
  assignColors,
  colorsForDomain,
  isColorValue,
  mergeAssignments,
} from "../views/DashboardsView/colorAssignment";

export interface DashboardColors {
  /** What chart tiles hand their renderer; a new function whenever an assignment changes. */
  categoryColors: ComputedRef<CategoryColors>;
  /** The stored assignments plus the ones this session gave, merged now. */
  sync(): DashboardColorAssignment[];
}

/**
 * The page's value colours: the dashboard's stored assignments, and the values its tiles drew
 * since that had none, each given a slot for the session. The editor writes them into the layout
 * with the author's next edit (`sync`); a viewer cannot save, so its slots last the session.
 */
export function createDashboardColors(
  stored: () => DashboardColorAssignment[] | undefined,
): DashboardColors {
  const current = shallowRef<DashboardColorAssignment[]>(stored() ?? []);

  const sync = () => {
    current.value = mergeAssignments(stored() ?? [], current.value);
    return current.value;
  };

  watch(stored, sync);

  // A new function per change, so every drawn tile works its colours out again.
  const categoryColors = computed<CategoryColors>(() => {
    void current.value;
    return (field, domain, dark) => {
      if (!domain.length || !domain.every(isColorValue)) return null;
      const next = assignColors(current.value, field, domain);
      if (next !== current.value) current.value = next;
      return colorsForDomain(next, field, domain, dark);
    };
  });

  return { categoryColors, sync };
}

const KEY: InjectionKey<DashboardColors> = Symbol("dashboardColors");

/** Called by the dashboard page; every chart tile it mounts then colours values alike. */
export function provideDashboardColors(
  stored: () => DashboardColorAssignment[] | undefined,
): DashboardColors {
  const colors = createDashboardColors(stored);
  provide(KEY, colors);
  return colors;
}

/** The page's value colours, or null outside a dashboard page (Graphic Walker's own then). */
export function useDashboardColors(): DashboardColors | null {
  return inject(KEY, null);
}
