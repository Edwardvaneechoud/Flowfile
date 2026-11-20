import type { DashboardLayout } from "../../types";

export const EDIT_HISTORY_LIMIT = 100;
/** Changes with one key that follow each other this closely are one step (a drag, a burst of typing). */
export const EDIT_COALESCE_MS = 1000;
/** A follow-up change (the grid compacting after an add or remove) this soon joins the step before it. */
export const EDIT_ABSORB_MS = 250;

export interface RecordOptions {
  /** Consecutive changes with the same non-null key coalesce while each comes within the coalesce window. */
  key?: string | null;
  /** Join the step before (or the state an undo, redo or reset landed on) when within the absorb window. */
  absorb?: boolean;
  now: number;
}

export interface EditHistoryOptions<T> {
  limit?: number;
  coalesceMs?: number;
  absorbMs?: number;
  equals?: (a: T, b: T) => boolean;
}

/**
 * Session undo/redo over whole-document snapshots. `record` is told the state after every
 * change; it pushes the state before it as an undo step unless the change coalesces with the
 * last one. `undo`/`redo` return the state to restore and move `present` there, so recording
 * that state again afterwards is a no-op and an undo never records itself.
 */
export class EditHistory<T> {
  private past: T[] = [];
  private future: T[] = [];
  private current: T | null = null;
  private lastKey: string | null = null;
  private lastAt = Number.NEGATIVE_INFINITY;
  private readonly limit: number;
  private readonly coalesceMs: number;
  private readonly absorbMs: number;
  private readonly equals: (a: T, b: T) => boolean;

  constructor(options: EditHistoryOptions<T> = {}) {
    this.limit = Math.max(1, options.limit ?? EDIT_HISTORY_LIMIT);
    this.coalesceMs = options.coalesceMs ?? EDIT_COALESCE_MS;
    this.absorbMs = options.absorbMs ?? EDIT_ABSORB_MS;
    this.equals = options.equals ?? ((a, b) => a === b);
  }

  get present(): T | null {
    return this.current;
  }

  get canUndo(): boolean {
    return this.past.length > 0;
  }

  get canRedo(): boolean {
    return this.future.length > 0;
  }

  get undoDepth(): number {
    return this.past.length;
  }

  get redoDepth(): number {
    return this.future.length;
  }

  /** Forget every step; `present` becomes the new base. */
  reset(present: T | null, now = Number.NEGATIVE_INFINITY): void {
    this.past = [];
    this.future = [];
    this.current = present;
    this.lastKey = null;
    this.lastAt = now;
  }

  /** Returns true when the change was taken in (a new step or a coalesced one). */
  record(next: T, { key = null, absorb = false, now }: RecordOptions): boolean {
    if (this.current === null) {
      this.current = next;
      this.lastKey = key;
      this.lastAt = now;
      return true;
    }
    if (this.equals(this.current, next)) return false;
    const elapsed = now - this.lastAt;
    const coalesces =
      (key !== null && key === this.lastKey && elapsed <= this.coalesceMs) ||
      (absorb && elapsed <= this.absorbMs);
    if (!coalesces) {
      this.past.push(this.current);
      if (this.past.length > this.limit) this.past.splice(0, this.past.length - this.limit);
      this.future = [];
      this.lastKey = key;
    }
    this.current = next;
    this.lastAt = now;
    return true;
  }

  /** Replace `present` without making a step (a restored state normalised on the way in). */
  replacePresent(next: T): void {
    this.current = next;
  }

  undo(now: number): T | null {
    if (this.current === null) return null;
    const target = this.past.pop();
    if (target === undefined) return null;
    this.future.push(this.current);
    return this.land(target, now);
  }

  redo(now: number): T | null {
    if (this.current === null) return null;
    const target = this.future.pop();
    if (target === undefined) return null;
    this.past.push(this.current);
    return this.land(target, now);
  }

  private land(target: T, now: number): T {
    this.current = target;
    this.lastKey = null;
    this.lastAt = now;
    return target;
  }
}

/** What the dashboard editor saves, as the user sees it (the drafts untrimmed). */
export interface DashboardSnapshot {
  name: string;
  description: string;
  layout: DashboardLayout;
}

export const sameDashboardSnapshot = (a: DashboardSnapshot, b: DashboardSnapshot): boolean =>
  JSON.stringify(a) === JSON.stringify(b);

const withoutGeometry = (layout: DashboardLayout): string =>
  JSON.stringify({
    ...layout,
    tiles: layout.tiles.map((t) => ({ ...t, x: 0, y: 0, w: 0, h: 0 })),
  });

/**
 * How a change between two snapshots records: typing in the name or the description coalesces
 * per field, and moving or resizing tiles coalesces and also absorbs the grid's compaction
 * right after another edit. Anything else is a step of its own.
 */
export const dashboardChange = (
  prev: DashboardSnapshot | null,
  next: DashboardSnapshot,
): Pick<RecordOptions, "key" | "absorb"> => {
  if (!prev) return { key: null };
  const name = prev.name !== next.name;
  const description = prev.description !== next.description;
  const layout = JSON.stringify(prev.layout) !== JSON.stringify(next.layout);
  if (name && !description && !layout) return { key: "name" };
  if (description && !name && !layout) return { key: "description" };
  if (
    layout &&
    !name &&
    !description &&
    withoutGeometry(prev.layout) === withoutGeometry(next.layout)
  ) {
    return { key: "geometry", absorb: true };
  }
  return { key: null };
};
