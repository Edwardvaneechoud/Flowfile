import { describe, expect, it } from "vitest";
import type { DashboardLayout, DashboardTile } from "../../types";
import {
  EDIT_ABSORB_MS,
  EDIT_COALESCE_MS,
  EditHistory,
  dashboardChange,
  sameDashboardSnapshot,
  type DashboardSnapshot,
} from "./editHistory";

const started = (base = "a") => {
  const h = new EditHistory<string>();
  h.reset(base, 0);
  return h;
};

describe("EditHistory", () => {
  it("undoes and redoes discrete steps in order", () => {
    const h = started();
    h.record("b", { now: 5000 });
    h.record("c", { now: 10000 });
    expect(h.undo(11000)).toBe("b");
    expect(h.undo(12000)).toBe("a");
    expect(h.canUndo).toBe(false);
    expect(h.undo(13000)).toBeNull();
    expect(h.redo(14000)).toBe("b");
    expect(h.redo(15000)).toBe("c");
    expect(h.canRedo).toBe(false);
    expect(h.present).toBe("c");
  });

  it("coalesces a burst with one key into one step", () => {
    const h = started("");
    h.record("n", { key: "name", now: 5000 });
    h.record("na", { key: "name", now: 5200 });
    h.record("nam", { key: "name", now: 5400 });
    expect(h.undoDepth).toBe(1);
    expect(h.undo(6000)).toBe("");
  });

  it("starts a new step after a pause or a different key", () => {
    const h = started("");
    h.record("x", { key: "name", now: 5000 });
    h.record("xy", { key: "name", now: 5000 + EDIT_COALESCE_MS + 1 });
    h.record("xyz", { key: "description", now: 5000 + EDIT_COALESCE_MS + 2 });
    expect(h.undoDepth).toBe(3);
  });

  it("never coalesces changes without a key", () => {
    const h = started();
    h.record("b", { now: 5000 });
    h.record("c", { now: 5001 });
    expect(h.undoDepth).toBe(2);
  });

  it("absorbs a follow-up into the step just made", () => {
    const h = started();
    h.record("removed", { now: 5000 });
    h.record("removed+compacted", { key: "geometry", absorb: true, now: 5000 + EDIT_ABSORB_MS });
    expect(h.undoDepth).toBe(1);
    expect(h.undo(6000)).toBe("a");
  });

  it("does not absorb a follow-up that comes late", () => {
    const h = started();
    h.record("b", { now: 5000 });
    h.record("moved", { key: "geometry", absorb: true, now: 5000 + EDIT_ABSORB_MS + 1 });
    expect(h.undoDepth).toBe(2);
  });

  it("absorbs a follow-up after an undo without dropping redo", () => {
    const h = started();
    h.record("b", { now: 5000 });
    expect(h.undo(6000)).toBe("a");
    h.record("a+compacted", { key: "geometry", absorb: true, now: 6010 });
    expect(h.canRedo).toBe(true);
    expect(h.present).toBe("a+compacted");
    expect(h.redo(7000)).toBe("b");
    expect(h.undo(8000)).toBe("a+compacted");
  });

  it("drops the redo steps when a new edit follows an undo", () => {
    const h = started();
    h.record("b", { now: 5000 });
    h.record("c", { now: 10000 });
    h.undo(11000);
    h.record("d", { now: 12000 });
    expect(h.canRedo).toBe(false);
    expect(h.undo(13000)).toBe("b");
  });

  it("does not coalesce typing after an undo into the step before it", () => {
    const h = started("");
    h.record("ab", { key: "name", now: 5000 });
    h.undo(5100);
    h.record("x", { key: "name", now: 5200 });
    expect(h.undoDepth).toBe(1);
    expect(h.undo(6000)).toBe("");
  });

  it("ignores recording the state it already holds", () => {
    const h = started();
    h.record("b", { now: 5000 });
    h.undo(6000);
    expect(h.record("a", { now: 6001 })).toBe(false);
    expect(h.canRedo).toBe(true);
  });

  it("caps the number of undo steps, dropping the oldest", () => {
    const h = new EditHistory<number>({ limit: 3 });
    h.reset(0, 0);
    for (let i = 1; i <= 5; i++) h.record(i, { now: i * 5000 });
    expect(h.undoDepth).toBe(3);
    expect([h.undo(1e6), h.undo(1e6), h.undo(1e6), h.undo(1e6)]).toEqual([4, 3, 2, null]);
  });

  it("forgets everything on reset", () => {
    const h = started();
    h.record("b", { now: 5000 });
    h.record("c", { now: 10000 });
    h.undo(11000);
    h.reset("z", 12000);
    expect([h.canUndo, h.canRedo, h.present]).toEqual([false, false, "z"]);
  });

  it("takes the first state recorded without a base as the base", () => {
    const h = new EditHistory<string>();
    h.record("a", { now: 0 });
    expect([h.canUndo, h.present]).toEqual([false, "a"]);
  });

  it("compares with the given equality", () => {
    const h = new EditHistory<{ v: number }>({ equals: (a, b) => a.v === b.v });
    h.reset({ v: 1 }, 0);
    expect(h.record({ v: 1 }, { now: 5000 })).toBe(false);
    expect(h.canUndo).toBe(false);
  });

  it("replaces the present without making a step", () => {
    const h = started();
    h.record("b", { now: 5000 });
    h.replacePresent("b'");
    expect(h.undoDepth).toBe(1);
    h.undo(6000);
    expect(h.redo(7000)).toBe("b'");
  });
});

const tile = (id: string, x = 0, y = 0): DashboardTile => ({
  id,
  type: "viz",
  viz_id: 1,
  chart_index: 0,
  x,
  y,
  w: 6,
  h: 6,
});

const layout = (tiles: DashboardTile[]): DashboardLayout => ({
  grid: { cols: 24, row_height: 40, version: 2 },
  tiles,
  filters: [],
});

const snap = (patch: Partial<DashboardSnapshot> = {}): DashboardSnapshot => ({
  name: "Sales",
  description: "",
  layout: layout([tile("a"), tile("b", 6)]),
  ...patch,
});

describe("dashboardChange", () => {
  it("keys name and description typing per field", () => {
    expect(dashboardChange(snap(), snap({ name: "Sales 2" }))).toEqual({ key: "name" });
    expect(dashboardChange(snap(), snap({ description: "d" }))).toEqual({ key: "description" });
  });

  it("treats moving or resizing tiles as geometry that absorbs follow-ups", () => {
    const moved = snap({ layout: layout([tile("a", 0, 6), tile("b", 6)]) });
    expect(dashboardChange(snap(), moved)).toEqual({ key: "geometry", absorb: true });
  });

  it("makes adding, removing or editing a tile a step of its own", () => {
    expect(dashboardChange(snap(), snap({ layout: layout([tile("a")]) }))).toEqual({ key: null });
    const retitled = snap({ layout: layout([{ ...tile("a"), title: "T" }, tile("b", 6)]) });
    expect(dashboardChange(snap(), retitled)).toEqual({ key: null });
  });

  it("makes a change to several fields at once a step of its own", () => {
    expect(dashboardChange(snap(), snap({ name: "x", description: "y" }))).toEqual({ key: null });
    expect(dashboardChange(null, snap())).toEqual({ key: null });
  });

  it("compares snapshots by content", () => {
    expect(sameDashboardSnapshot(snap(), snap())).toBe(true);
    expect(sameDashboardSnapshot(snap(), snap({ name: "x" }))).toBe(false);
  });
});
