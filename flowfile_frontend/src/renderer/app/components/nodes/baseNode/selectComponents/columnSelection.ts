/**
 * Pure selection and reorder model shared by every column picker in node settings.
 *
 * Deliberately free of Vue, DOM and axios imports so it can be unit tested under
 * vitest's `node` environment — the repo has neither jsdom nor @vue/test-utils.
 *
 * Reorder targets are expressed as an *insertion gap* in `0..items.length`, not
 * as a target row index. "Insert before row 3" and "append after the last row"
 * then become the same value, which also drives the drop indicator.
 */

export interface SelectionState {
  /** Where the next shift-range pivots from. */
  anchorIndex: number | null;
  /** Ascending, deduped, all in range. */
  selectedIndices: number[];
}

export interface ClickModifiers {
  shift: boolean;
  /** meta on mac, ctrl elsewhere. */
  toggle: boolean;
}

export interface ApplyClickOptions {
  /** Re-clicking the only selected row clears it. GroupBy/Sort do this; selectDynamic does not. */
  clearOnRepeatClick?: boolean;
}

export type SortDirection = "none" | "asc" | "desc";
export type ReorderCommand = "top" | "up" | "down" | "bottom";

export const EMPTY_SELECTION: Readonly<SelectionState> = Object.freeze({
  anchorIndex: null,
  selectedIndices: Object.freeze([]) as unknown as number[],
});

const sortedUnique = (indices: readonly number[]): number[] =>
  Array.from(new Set(indices)).sort((a, b) => a - b);

const inRange = (indices: readonly number[], length: number): number[] =>
  indices.filter((i) => Number.isInteger(i) && i >= 0 && i < length);

/** Kept out of `readModifiers` so that stays a pure function of its arguments. */
export const isMacPlatform = (): boolean =>
  typeof navigator !== "undefined" && /Mac|iPhone|iPad/.test(navigator.platform ?? "");

export const readModifiers = (
  event: { shiftKey: boolean; ctrlKey: boolean; metaKey: boolean },
  isMac: boolean,
): ClickModifiers => ({
  shift: event.shiftKey,
  toggle: isMac ? event.metaKey : event.ctrlKey,
});

export const rangeBetween = (a: number, b: number): number[] => {
  const [lo, hi] = a <= b ? [a, b] : [b, a];
  return Array.from({ length: hi - lo + 1 }, (_, i) => lo + i);
};

export const applyClick = (
  state: SelectionState,
  clickedIndex: number,
  modifiers: ClickModifiers,
  options: ApplyClickOptions = {},
): SelectionState => {
  if (modifiers.shift && state.anchorIndex !== null) {
    // The anchor deliberately stays put, so a second shift-click re-ranges from
    // the original pivot rather than from the previous click.
    const range = rangeBetween(state.anchorIndex, clickedIndex);
    const selectedIndices = modifiers.toggle
      ? sortedUnique([...state.selectedIndices, ...range])
      : range;
    return { anchorIndex: state.anchorIndex, selectedIndices };
  }

  if (modifiers.toggle) {
    const isSelected = state.selectedIndices.includes(clickedIndex);
    const selectedIndices = isSelected
      ? state.selectedIndices.filter((i) => i !== clickedIndex)
      : sortedUnique([...state.selectedIndices, clickedIndex]);
    // The anchor moves on both add and remove, so a following shift-click ranges
    // from the row the user last touched.
    return { anchorIndex: clickedIndex, selectedIndices };
  }

  const isOnlySelection =
    state.selectedIndices.length === 1 && state.selectedIndices[0] === clickedIndex;
  if (options.clearOnRepeatClick && isOnlySelection) {
    return { anchorIndex: null, selectedIndices: [] };
  }

  return { anchorIndex: clickedIndex, selectedIndices: [clickedIndex] };
};

/**
 * Right-click or drag-start on a row: keep the current selection when the row is
 * part of it, otherwise collapse to just that row. Returns the same state object
 * when nothing changes so callers can skip work.
 */
export const ensureSelected = (state: SelectionState, index: number): SelectionState =>
  state.selectedIndices.includes(index) ? state : { anchorIndex: index, selectedIndices: [index] };

/** Drop indices that fell off the end after the list shrank. */
export const clampSelection = (state: SelectionState, length: number): SelectionState => {
  const selectedIndices = inRange(state.selectedIndices, length);
  const anchorIndex =
    state.anchorIndex !== null && state.anchorIndex < length ? state.anchorIndex : null;
  return { anchorIndex, selectedIndices };
};

export const applySelectAll = (length: number): SelectionState => ({
  anchorIndex: length > 0 ? 0 : null,
  selectedIndices: Array.from({ length }, (_, i) => i),
});

export interface MoveResult<T> {
  items: T[];
  /** Where the moved block landed, so the highlight follows the rows. */
  selectedIndices: number[];
  /** False when the drop was a no-op. */
  changed: boolean;
}

/**
 * Move every selected row, as one block preserving relative order, to the
 * insertion gap `insertIndex` (0..items.length).
 *
 * Returns a NEW array holding the SAME object references: column order travels
 * over the wire as each item's `position` field, which callers stamp in place on
 * the objects the parent still holds. Cloning here would silently break reorder
 * on Join, CrossJoin, FuzzyMatch and Unique, none of which re-emit on reorder.
 */
export const moveItems = <T>(
  items: readonly T[],
  selectedIndices: readonly number[],
  insertIndex: number,
): MoveResult<T> => {
  const selected = sortedUnique(inRange(selectedIndices, items.length));
  const noop: MoveResult<T> = {
    items: items.slice(),
    selectedIndices: selected,
    changed: false,
  };
  if (selected.length === 0 || selected.length === items.length) return noop;

  const at = Math.max(0, Math.min(insertIndex, items.length));
  // Every selected row above the gap vanishes from the remainder, so the gap
  // slides up by that many places. This is the whole of the drop-index math.
  const adjusted = at - selected.filter((i) => i < at).length;

  const selectedSet = new Set(selected);
  const moved = selected.map((i) => items[i]);
  const rest = items.filter((_, i) => !selectedSet.has(i));
  const next = [...rest.slice(0, adjusted), ...moved, ...rest.slice(adjusted)];

  const landedAt = moved.map((_, offset) => adjusted + offset);
  const changed = next.some((item, i) => item !== items[i]);
  return changed ? { items: next, selectedIndices: landedAt, changed: true } : noop;
};

/** The insertion gap a reorder command targets, or null when it is a no-op. */
export const reorderInsertIndex = (
  selectedIndices: readonly number[],
  itemCount: number,
  command: ReorderCommand,
): number | null => {
  const selected = sortedUnique(inRange(selectedIndices, itemCount));
  if (selected.length === 0 || selected.length === itemCount) return null;

  const first = selected[0];
  const last = selected[selected.length - 1];
  const isContiguous = last - first + 1 === selected.length;

  switch (command) {
    case "top":
      return isContiguous && first === 0 ? null : 0;
    case "bottom":
      return isContiguous && last === itemCount - 1 ? null : itemCount;
    case "up":
      return first === 0 ? null : first - 1;
    case "down":
      return last === itemCount - 1 ? null : last + 2;
  }
};

export const applyReorder = <T>(
  items: readonly T[],
  selectedIndices: readonly number[],
  command: ReorderCommand,
): MoveResult<T> => {
  const insertIndex = reorderInsertIndex(selectedIndices, items.length, command);
  if (insertIndex === null) {
    return {
      items: items.slice(),
      selectedIndices: sortedUnique(inRange(selectedIndices, items.length)),
      changed: false,
    };
  }
  return moveItems(items, selectedIndices, insertIndex);
};

/** Top half of a row inserts before it, bottom half after it. */
export const insertIndexFromPointer = (
  rowTop: number,
  rowHeight: number,
  clientY: number,
  rowIndex: number,
): number => (clientY < rowTop + rowHeight / 2 ? rowIndex : rowIndex + 1);

/**
 * IN-PLACE: writes `position = i` onto the same objects the parent holds, then
 * returns the same array. See the note on `moveItems` — this mutation is the
 * wire contract, not an implementation detail.
 */
export const assignPositions = <T extends { position: number }>(items: T[]): T[] => {
  items.forEach((item, index) => {
    item.position = index;
  });
  return items;
};

export const nextSortDirection = (direction: SortDirection): SortDirection =>
  direction === "none" ? "asc" : direction === "asc" ? "desc" : "none";

export const sortForDirection = <T>(
  items: readonly T[],
  direction: SortDirection,
  getName: (item: T) => string,
  getOriginalPosition: (item: T) => number,
): T[] => {
  const next = items.slice();
  if (direction === "asc") next.sort((a, b) => getName(a).localeCompare(getName(b)));
  else if (direction === "desc") next.sort((a, b) => getName(b).localeCompare(getName(a)));
  else next.sort((a, b) => getOriginalPosition(a) - getOriginalPosition(b));
  return next;
};

/** Stable partition: available rows first, unavailable last, order otherwise kept. */
export const partitionAvailable = <T>(
  items: readonly T[],
  isAvailable: (item: T) => boolean,
): T[] => [...items.filter(isAvailable), ...items.filter((item) => !isAvailable(item))];

/**
 * Merge an incoming props array into the current local order rather than
 * clobbering it: keep local order for keys that survive, append new keys in
 * incoming order, drop keys that vanished. Incoming items win on identity so
 * fresh objects from the parent are the ones rendered.
 */
export const reconcileOrder = <T>(
  local: readonly T[],
  incoming: readonly T[],
  getKey: (item: T) => string,
): T[] => {
  const incomingByKey = new Map(incoming.map((item) => [getKey(item), item]));
  const kept: T[] = [];
  const seen = new Set<string>();

  for (const item of local) {
    const key = getKey(item);
    const match = incomingByKey.get(key);
    if (match !== undefined && !seen.has(key)) {
      kept.push(match);
      seen.add(key);
    }
  }
  for (const item of incoming) {
    const key = getKey(item);
    if (!seen.has(key)) {
      kept.push(item);
      seen.add(key);
    }
  }
  return kept;
};
