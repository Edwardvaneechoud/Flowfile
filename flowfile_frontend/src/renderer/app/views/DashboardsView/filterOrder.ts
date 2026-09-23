import type { DashboardFilter } from "../../types";

export type DropPosition = "before" | "after";

/** A new array with ``draggedId`` moved before/after ``targetId``; the same
 * array when the ids are equal, either is unknown, or the order would not change. */
export const moveFilter = (
  filters: DashboardFilter[],
  draggedId: string,
  targetId: string,
  position: DropPosition,
): DashboardFilter[] => {
  if (draggedId === targetId) return filters;
  const dragged = filters.find((f) => f.id === draggedId);
  if (!dragged || !filters.some((f) => f.id === targetId)) return filters;
  const rest = filters.filter((f) => f.id !== draggedId);
  const at = rest.findIndex((f) => f.id === targetId) + (position === "after" ? 1 : 0);
  const next = [...rest.slice(0, at), dragged, ...rest.slice(at)];
  return next.every((x, i) => x === filters[i]) ? filters : next;
};
