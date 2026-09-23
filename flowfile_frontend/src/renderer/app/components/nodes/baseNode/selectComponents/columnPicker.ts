/**
 * Pure helpers behind ColumnPickerCard: the split clamp, drop-zone hit-testing,
 * usage-chip capping and the row bookkeeping every settings table needs.
 *
 * No Vue or DOM imports so it runs under vitest's `node` environment.
 */

export interface UsageChip {
  label: string;
  title: string;
  isKey: boolean;
  /** Settings row indices the chip reveals when clicked. */
  rows: number[];
}

export interface DropZoneSpec {
  value: string;
  label: string;
  disabled?: boolean;
}

/** The settings strip on its own, and the column list's toolbar-plus-one-row floor. */
export const SETTINGS_STRIP_PX = 30;
export const COLUMNS_MIN_PX = 84;
/** A pinned pane shorter than the strip, a table header and one row folds instead. */
export const SETTINGS_MIN_OPEN_PX = SETTINGS_STRIP_PX + 26 + 28;

/** Keeps a dragged settings height between the bare strip and the column list's floor. */
export const clampSettingsHeight = (height: number, available: number): number => {
  const max = Math.max(SETTINGS_STRIP_PX, available - COLUMNS_MIN_PX);
  return Math.round(Math.min(Math.max(height, SETTINGS_STRIP_PX), max));
};

/** The zone under the pointer: the settings pane is split into equal horizontal bands. */
export const dropZoneAt = <T extends DropZoneSpec>(
  rect: { left: number; width: number },
  clientX: number,
  zones: readonly T[],
): T | null => {
  if (zones.length === 0 || rect.width <= 0) return null;
  const band = Math.floor(((clientX - rect.left) / rect.width) * zones.length);
  const zone = zones[Math.min(zones.length - 1, Math.max(0, band))];
  return zone.disabled ? null : zone;
};

export const pluralize = (count: number, singular: string, plural = `${singular}s`): string =>
  `${count} ${count === 1 ? singular : plural}`;

/** The rows left after dropping the given indices, in their original order. */
export const withoutRows = <T>(rows: readonly T[], indices: readonly number[]): T[] => {
  const drop = new Set(indices);
  return rows.filter((_, index) => !drop.has(index));
};

/** Selected indices that survive a removal, shifted down past the rows that went. */
export const shiftAfterRemoval = (
  selected: readonly number[],
  removed: readonly number[],
): number[] => {
  const drop = new Set(removed);
  return selected
    .filter((index) => !drop.has(index))
    .map((index) => index - removed.filter((gone) => gone < index).length);
};

export const MAX_USAGE_CHIPS = 2;

/** At most two chips per column plus a "+N" overflow, so a heavily used column can't widen the table. */
export const capUsageChips = (chips: readonly UsageChip[]): UsageChip[] => {
  if (chips.length <= MAX_USAGE_CHIPS) return [...chips];
  const hidden = chips.slice(MAX_USAGE_CHIPS);
  return [
    ...chips.slice(0, MAX_USAGE_CHIPS),
    {
      label: `+${hidden.length}`,
      title: hidden.map((chip) => chip.label).join(", "),
      isKey: false,
      rows: hidden.flatMap((chip) => chip.rows),
    },
  ];
};
