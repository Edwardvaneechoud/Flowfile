import type { DashboardTile, DashboardTileType } from "../../types";

/** Core refuses a longer note (`DashboardTile.note_md`). */
export const NOTE_MAX_LENGTH = 4000;

/** Hover delays of the ⓘ: open after a short rest, close a moment after the pointer leaves. */
export const NOTE_OPEN_DELAY_MS = 300;
export const NOTE_CLOSE_DELAY_MS = 200;

const NOTE_TILE_TYPES: ReadonlySet<DashboardTileType> = new Set(["viz", "kpi", "table", "filter"]);

/** Tiles that show data or a control take a note; text blocks, separators and images do not. */
export const takesNote = (type: DashboardTileType): boolean => NOTE_TILE_TYPES.has(type);

/** The note a tile shows, or null when it has none (or is a type that takes none). */
export const tileNote = (tile: DashboardTile): string | null =>
  takesNote(tile.type) && tile.note_md?.trim() ? tile.note_md : null;

/** The menu entry that opens the note dialog. */
export const noteMenuLabel = (tile: DashboardTile): string =>
  tileNote(tile) ? "Edit note…" : "Add note…";

/**
 * The tile with the dialog's draft as its note (a blank draft removes it), or null when that
 * changes nothing, so an unchanged Save records no undo step.
 */
export const withNote = (tile: DashboardTile, draft: string): DashboardTile | null => {
  const next = draft.trim() ? draft.trimEnd() : null;
  if (next === (tileNote(tile)?.trimEnd() ?? null)) return null;
  return { ...tile, note_md: next };
};
