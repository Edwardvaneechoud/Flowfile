import type { DashboardColorAssignment, DashboardColorValue, DashboardLayout } from "../../types";
import {
  colorDomain,
  colorValueKey,
} from "../../components/nodes/node-types/elements/exploreData/vueGraphicWalker/chartColors";
import {
  CATEGORY_DARK,
  CATEGORY_LIGHT,
} from "../../components/nodes/node-types/elements/exploreData/vueGraphicWalker/vizTheme";

/**
 * Which colour a value of a colour field has on a dashboard.
 *
 * Colour belongs to the value, never to its rank in one chart: every chart tile that colours by a
 * field of the same name gives a value the same palette slot, so a filter that removes values
 * repaints none of the others. Slots are handed out in the palette's fixed order, never cycled,
 * and a value keeps its slot once it has one (append-only). A field with more values than the
 * palette has colours gives the values past it one neutral grey, so no hue is ever shared
 * silently.
 */

export const PALETTE_SLOTS = CATEGORY_LIGHT.length;

/** Values past the palette: the muted text token of each theme (`--color-text-muted`). */
export const OVER_PALETTE_LIGHT = "#a0aec0";
export const OVER_PALETTE_DARK = "#64748b";

export const isColorValue = (value: unknown): value is DashboardColorValue =>
  value === null ||
  typeof value === "string" ||
  typeof value === "boolean" ||
  (typeof value === "number" && Number.isFinite(value));

const slotsOf = (assignments: DashboardColorAssignment[], field: string) => {
  const slots = new Map<string, number>();
  for (const a of assignments) {
    if (a.field === field) slots.set(colorValueKey(a.value), a.slot);
  }
  return slots;
};

const firstFreeSlot = (taken: Set<number>): number | null => {
  for (let slot = 0; slot < PALETTE_SLOTS; slot += 1) if (!taken.has(slot)) return slot;
  return null;
};

/**
 * ``assignments`` with every value of ``domain`` that has no slot yet for ``field`` given the
 * first free one, in the order Graphic Walker lists the values. Hands back the same list when
 * nothing was added, and skips values once the palette is full.
 */
export function assignColors(
  assignments: DashboardColorAssignment[],
  field: string,
  domain: unknown[],
): DashboardColorAssignment[] {
  const known = slotsOf(assignments, field);
  const taken = new Set(known.values());
  const added: DashboardColorAssignment[] = [];
  for (const value of colorDomain(domain)) {
    if (!isColorValue(value) || known.has(colorValueKey(value))) continue;
    const slot = firstFreeSlot(taken);
    if (slot == null) break;
    taken.add(slot);
    added.push({ field, value, slot });
  }
  return added.length ? [...assignments, ...added] : assignments;
}

/** The colour of each value of ``domain``, in its order; past the palette, the neutral grey. */
export function colorsForDomain(
  assignments: DashboardColorAssignment[],
  field: string,
  domain: unknown[],
  dark: boolean,
): string[] {
  const palette = dark ? CATEGORY_DARK : CATEGORY_LIGHT;
  const grey = dark ? OVER_PALETTE_DARK : OVER_PALETTE_LIGHT;
  const slots = slotsOf(assignments, field);
  return domain.map((value) => {
    const slot = slots.get(colorValueKey(value));
    return slot != null && slot < palette.length ? palette[slot] : grey;
  });
}

/**
 * The stored assignments, then the ones only ``current`` holds. Each of those keeps its slot
 * unless a stored one took it, and then takes the first free slot (or none once the palette is
 * full). Hands back ``current`` itself when nothing differs.
 */
export function mergeAssignments(
  stored: DashboardColorAssignment[],
  current: DashboardColorAssignment[],
): DashboardColorAssignment[] {
  const merged = [...stored];
  const known = new Set(stored.map((a) => JSON.stringify([a.field, colorValueKey(a.value)])));
  const taken = new Map<string, Set<number>>();
  for (const a of stored) {
    if (!taken.has(a.field)) taken.set(a.field, new Set());
    taken.get(a.field)!.add(a.slot);
  }
  for (const a of current) {
    if (known.has(JSON.stringify([a.field, colorValueKey(a.value)]))) continue;
    if (!taken.has(a.field)) taken.set(a.field, new Set());
    const slots = taken.get(a.field)!;
    const slot = slots.has(a.slot) ? firstFreeSlot(slots) : a.slot;
    if (slot == null) continue;
    slots.add(slot);
    merged.push({ ...a, slot });
  }
  return sameAssignments(merged, current) ? current : merged;
}

export const sameAssignments = (
  a: DashboardColorAssignment[] | undefined,
  b: DashboardColorAssignment[] | undefined,
): boolean => JSON.stringify(a ?? []) === JSON.stringify(b ?? []);

/** The layout without its value colours, which the edit history does not follow. */
export function withoutColorAssignments(layout: DashboardLayout): DashboardLayout {
  const copy = { ...layout };
  delete copy.color_assignments;
  return copy;
}
