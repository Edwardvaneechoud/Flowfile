import { describe, it, expect } from "vitest";
import {
  EMPTY_SELECTION,
  applyClick,
  applyReorder,
  applySelectAll,
  assignPositions,
  clampSelection,
  ensureSelected,
  insertIndexFromPointer,
  moveItems,
  nextSortDirection,
  partitionAvailable,
  rangeBetween,
  readModifiers,
  reconcileOrder,
  reorderInsertIndex,
  sortForDirection,
  type SelectionState,
} from "./columnSelection";

const ITEMS = ["A", "B", "C", "D", "E"];

const state = (anchorIndex: number | null, selectedIndices: number[]): SelectionState => ({
  anchorIndex,
  selectedIndices,
});

/** Modifiers built directly, so these cases never depend on the host platform. */
const mods = (shift: boolean, toggle = false) => ({ shift, toggle });

describe("rangeBetween", () => {
  it("is inclusive and direction-agnostic", () => {
    expect(rangeBetween(1, 4)).toEqual([1, 2, 3, 4]);
    expect(rangeBetween(4, 1)).toEqual([1, 2, 3, 4]);
    expect(rangeBetween(2, 2)).toEqual([2]);
  });
});

describe("readModifiers", () => {
  const meta = { shiftKey: false, ctrlKey: false, metaKey: true };
  const ctrl = { shiftKey: false, ctrlKey: true, metaKey: false };

  it("maps meta on mac and ctrl elsewhere", () => {
    expect(readModifiers(meta, true).toggle).toBe(true);
    expect(readModifiers(ctrl, true).toggle).toBe(false);
    expect(readModifiers(ctrl, false).toggle).toBe(true);
    expect(readModifiers(meta, false).toggle).toBe(false);
  });

  it("passes shift through untouched", () => {
    expect(readModifiers({ ...ctrl, shiftKey: true }, false).shift).toBe(true);
  });
});

describe("applyClick", () => {
  it("selects a single row and sets the anchor on a plain click", () => {
    expect(applyClick(EMPTY_SELECTION, 2, mods(false))).toEqual(state(2, [2]));
  });

  it("falls back to a plain click when shift is held with no anchor", () => {
    expect(applyClick(EMPTY_SELECTION, 3, mods(true))).toEqual(state(3, [3]));
  });

  it("shift-selects an inclusive range in both directions", () => {
    expect(applyClick(state(1, [1]), 4, mods(true))).toEqual(state(1, [1, 2, 3, 4]));
    expect(applyClick(state(4, [4]), 1, mods(true))).toEqual(state(4, [1, 2, 3, 4]));
  });

  it("re-ranges from the original anchor, not from the previous shift-click", () => {
    const afterFirst = applyClick(state(2, [2]), 5, mods(true));
    expect(afterFirst).toEqual(state(2, [2, 3, 4, 5]));
    const afterSecond = applyClick(afterFirst, 3, mods(true));
    expect(afterSecond).toEqual(state(2, [2, 3]));
  });

  it("toggles a row in and out, moving the anchor either way", () => {
    const added = applyClick(state(0, [0]), 3, mods(false, true));
    expect(added).toEqual(state(3, [0, 3]));
    const removed = applyClick(added, 0, mods(false, true));
    expect(removed).toEqual(state(0, [3]));
  });

  it("unions the range into the selection on ctrl+shift-click", () => {
    expect(applyClick(state(3, [0, 3]), 5, mods(true, true))).toEqual(state(3, [0, 3, 4, 5]));
  });

  it("clears on re-click of the only selected row when clearOnRepeatClick is set", () => {
    expect(
      applyClick(state(2, [2]), 2, mods(false), {
        clearOnRepeatClick: true,
      }),
    ).toEqual(state(null, []));
  });

  it("keeps the row selected on re-click by default", () => {
    expect(applyClick(state(2, [2]), 2, mods(false))).toEqual(state(2, [2]));
  });

  it("does not clear on re-click when more than one row is selected", () => {
    expect(applyClick(state(2, [2, 3]), 2, mods(false), { clearOnRepeatClick: true })).toEqual(
      state(2, [2]),
    );
  });
});

describe("ensureSelected", () => {
  it("keeps a multi-row selection when the row is part of it", () => {
    const current = state(1, [1, 2, 3, 4]);
    expect(ensureSelected(current, 3)).toBe(current);
  });

  it("collapses to the row when it sits outside the selection", () => {
    expect(ensureSelected(state(1, [1, 2]), 4)).toEqual(state(4, [4]));
  });
});

describe("clampSelection", () => {
  it("drops indices and an anchor that fell off the end", () => {
    expect(clampSelection(state(4, [0, 4, 7]), 3)).toEqual(state(null, [0]));
  });
});

describe("applySelectAll", () => {
  it("selects every row and anchors at the top", () => {
    expect(applySelectAll(3)).toEqual(state(0, [0, 1, 2]));
    expect(applySelectAll(0)).toEqual(state(null, []));
  });
});

describe("moveItems", () => {
  it("moves a single row down, matching the previous single-row splice", () => {
    const result = moveItems(ITEMS, [0], 4);
    expect(result.items).toEqual(["B", "C", "D", "A", "E"]);
    expect(result.selectedIndices).toEqual([3]);
    expect(result.changed).toBe(true);
  });

  it("moves a single row up, matching the previous single-row splice", () => {
    const result = moveItems(ITEMS, [4], 1);
    expect(result.items).toEqual(["A", "E", "B", "C", "D"]);
    expect(result.selectedIndices).toEqual([1]);
  });

  it("moves a contiguous block down past its own removal", () => {
    // The naive splice(target, 0, ...block) overshoots by the block size here.
    const result = moveItems(ITEMS, [0, 1], 4);
    expect(result.items).toEqual(["C", "D", "A", "B", "E"]);
    expect(result.selectedIndices).toEqual([2, 3]);
  });

  it("moves a contiguous block up", () => {
    const result = moveItems(ITEMS, [3, 4], 1);
    expect(result.items).toEqual(["A", "D", "E", "B", "C"]);
    expect(result.selectedIndices).toEqual([1, 2]);
  });

  it("moves the whole selection to the top", () => {
    const result = moveItems(ITEMS, [2, 3, 4], 0);
    expect(result.items).toEqual(["C", "D", "E", "A", "B"]);
    expect(result.selectedIndices).toEqual([0, 1, 2]);
  });

  it("collapses a non-contiguous selection into one block, preserving relative order", () => {
    const result = moveItems(ITEMS, [0, 2, 4], 3);
    expect(result.items).toEqual(["B", "A", "C", "E", "D"]);
    expect(result.selectedIndices).toEqual([1, 2, 3]);
  });

  it("appends the block at the tail", () => {
    const result = moveItems(ITEMS, [0, 1], 5);
    expect(result.items).toEqual(["C", "D", "E", "A", "B"]);
    expect(result.selectedIndices).toEqual([3, 4]);
  });

  it("is a no-op when dropped inside its own block", () => {
    for (const gap of [1, 2, 3]) {
      const result = moveItems(ITEMS, [1, 2], gap);
      expect(result.changed).toBe(false);
      expect(result.items).toEqual(ITEMS);
    }
  });

  it("is a no-op for an empty selection, a full selection, or an unmoved row", () => {
    expect(moveItems(ITEMS, [], 2).changed).toBe(false);
    expect(moveItems(ITEMS, [0, 1, 2, 3, 4], 2).changed).toBe(false);
    expect(moveItems(ITEMS, [0], 0).changed).toBe(false);
  });

  it("ignores out-of-range indices and clamps an out-of-range gap", () => {
    expect(moveItems(ITEMS, [7, -1], 2).changed).toBe(false);
    expect(moveItems(ITEMS, [0], 99).items).toEqual(["B", "C", "D", "E", "A"]);
    expect(moveItems(ITEMS, [4], -5).items).toEqual(["E", "A", "B", "C", "D"]);
  });

  it("normalizes duplicate and unsorted indices", () => {
    expect(moveItems(ITEMS, [1, 0, 1], 4).items).toEqual(["C", "D", "A", "B", "E"]);
  });

  it("does not mutate the input array", () => {
    const original = ITEMS.slice();
    moveItems(original, [0, 1], 4);
    expect(original).toEqual(ITEMS);
  });

  it("preserves object identity so the in-place position write reaches the parent", () => {
    const objects = ITEMS.map((name) => ({ name }));
    const result = moveItems(objects, [0, 1], 4);
    expect(result.items[2]).toBe(objects[0]);
    expect(result.items[3]).toBe(objects[1]);
    expect(result.items[0]).toBe(objects[2]);
  });
});

describe("reorderInsertIndex", () => {
  it("targets the gaps each command implies", () => {
    expect(reorderInsertIndex([2, 3], 5, "top")).toBe(0);
    expect(reorderInsertIndex([1, 2], 5, "bottom")).toBe(5);
    expect(reorderInsertIndex([2, 3], 5, "up")).toBe(1);
    expect(reorderInsertIndex([1, 2], 5, "down")).toBe(4);
  });

  it("returns null at the edges", () => {
    expect(reorderInsertIndex([0, 1], 5, "top")).toBeNull();
    expect(reorderInsertIndex([3, 4], 5, "bottom")).toBeNull();
    expect(reorderInsertIndex([0, 2], 5, "up")).toBeNull();
    expect(reorderInsertIndex([2, 4], 5, "down")).toBeNull();
  });

  it("still moves a non-contiguous selection that merely touches an edge", () => {
    // [0, 2] is not already "at the top" — moving it there compacts it to [0, 1].
    expect(reorderInsertIndex([0, 2], 5, "top")).toBe(0);
    expect(reorderInsertIndex([2, 4], 5, "bottom")).toBe(5);
  });

  it("returns null for an empty or complete selection", () => {
    expect(reorderInsertIndex([], 5, "top")).toBeNull();
    expect(reorderInsertIndex([0, 1, 2, 3, 4], 5, "bottom")).toBeNull();
  });
});

describe("applyReorder", () => {
  it("moves a block to the top and reports the landing indices", () => {
    const result = applyReorder(ITEMS, [2, 4], "top");
    expect(result.items).toEqual(["C", "E", "A", "B", "D"]);
    expect(result.selectedIndices).toEqual([0, 1]);
  });

  it("passes through unchanged on a no-op command", () => {
    const result = applyReorder(ITEMS, [0, 1], "top");
    expect(result.changed).toBe(false);
    expect(result.items).toEqual(ITEMS);
    expect(result.selectedIndices).toEqual([0, 1]);
  });

  it("nudges a single row one place in each direction", () => {
    expect(applyReorder(ITEMS, [2], "up").items).toEqual(["A", "C", "B", "D", "E"]);
    expect(applyReorder(ITEMS, [2], "down").items).toEqual(["A", "B", "D", "C", "E"]);
  });
});

describe("insertIndexFromPointer", () => {
  it("splits the row at its midpoint", () => {
    expect(insertIndexFromPointer(100, 30, 104, 3)).toBe(3);
    expect(insertIndexFromPointer(100, 30, 126, 3)).toBe(4);
    // Exactly the midpoint counts as the bottom half.
    expect(insertIndexFromPointer(100, 30, 115, 3)).toBe(4);
  });
});

describe("assignPositions", () => {
  it("writes indices onto the same objects and returns the same array", () => {
    const items = [{ position: 7 }, { position: 2 }];
    const first = items[0];
    const result = assignPositions(items);
    expect(result).toBe(items);
    expect(result[0]).toBe(first);
    expect(items.map((i) => i.position)).toEqual([0, 1]);
  });
});

describe("nextSortDirection", () => {
  it("cycles none to asc to desc and back", () => {
    expect(nextSortDirection("none")).toBe("asc");
    expect(nextSortDirection("asc")).toBe("desc");
    expect(nextSortDirection("desc")).toBe("none");
  });
});

describe("sortForDirection", () => {
  const rows = [
    { name: "b", original: 2 },
    { name: "c", original: 0 },
    { name: "a", original: 1 },
  ];
  const byName = (r: (typeof rows)[number]) => r.name;
  const byOriginal = (r: (typeof rows)[number]) => r.original;

  it("sorts ascending and descending by name", () => {
    expect(sortForDirection(rows, "asc", byName, byOriginal).map(byName)).toEqual(["a", "b", "c"]);
    expect(sortForDirection(rows, "desc", byName, byOriginal).map(byName)).toEqual(["c", "b", "a"]);
  });

  it("restores the original upstream order for none", () => {
    expect(sortForDirection(rows, "none", byName, byOriginal).map(byName)).toEqual(["c", "a", "b"]);
  });

  it("does not mutate the input", () => {
    const input = rows.slice();
    sortForDirection(input, "asc", byName, byOriginal);
    expect(input.map(byName)).toEqual(["b", "c", "a"]);
  });
});

describe("partitionAvailable", () => {
  it("moves unavailable rows to the end, keeping order within each group", () => {
    const rows = [
      { name: "a", ok: true },
      { name: "b", ok: false },
      { name: "c", ok: true },
      { name: "d", ok: false },
    ];
    expect(partitionAvailable(rows, (r) => r.ok).map((r) => r.name)).toEqual(["a", "c", "b", "d"]);
  });
});

describe("reconcileOrder", () => {
  const key = (item: { name: string }) => item.name;

  it("keeps the local order for surviving keys", () => {
    const local = [{ name: "c" }, { name: "a" }, { name: "b" }];
    const incoming = [{ name: "a" }, { name: "b" }, { name: "c" }];
    expect(reconcileOrder(local, incoming, key).map(key)).toEqual(["c", "a", "b"]);
  });

  it("appends new keys in incoming order and drops vanished ones", () => {
    const local = [{ name: "c" }, { name: "gone" }, { name: "a" }];
    const incoming = [{ name: "a" }, { name: "c" }, { name: "x" }, { name: "y" }];
    expect(reconcileOrder(local, incoming, key).map(key)).toEqual(["c", "a", "x", "y"]);
  });

  it("is order-stable when the parent replaces the array with equivalent items", () => {
    // The exact regression: a fresh props array must not reset a user's drag order.
    const local = [{ name: "c" }, { name: "a" }, { name: "b" }];
    const incoming = local.map((item) => ({ ...item }));
    expect(reconcileOrder(local, incoming, key).map(key)).toEqual(["c", "a", "b"]);
  });

  it("adopts the incoming object identities", () => {
    const local = [{ name: "a" }];
    const incoming = [{ name: "a" }];
    expect(reconcileOrder(local, incoming, key)[0]).toBe(incoming[0]);
  });

  it("handles an empty local list", () => {
    const incoming = [{ name: "a" }, { name: "b" }];
    expect(reconcileOrder([], incoming, key).map(key)).toEqual(["a", "b"]);
  });
});
