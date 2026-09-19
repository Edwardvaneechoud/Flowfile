import { describe, it, expect } from "vitest";
import {
  applyOperation,
  cellMoveAnnouncement,
  duplicateCell,
  insertCell,
  moveCell,
  moveCellBy,
  newCellId,
  removeCell,
  type CellLike,
} from "./cellOperations";

interface TestCell extends CellLike {
  code: string;
  output: string | null;
}

function makeCells(n: number): TestCell[] {
  return Array.from({ length: n }, (_, i) => ({
    id: `c${i}`,
    code: `code ${i}`,
    output: `out ${i}`,
  }));
}

const ids = (cells: TestCell[]) => cells.map((c) => c.id);

const cloneCell = (src: TestCell, id: string): TestCell => ({ ...src, id, output: null });

describe("moveCell", () => {
  it("treats the target as the FINAL index when moving later", () => {
    const cells = makeCells(5);
    const r = moveCell(cells, "c0", 3)!;
    expect(ids(r.cells)).toEqual(["c1", "c2", "c3", "c0", "c4"]);
    expect(r.cells[3]).toBe(cells[0]);
    expect(r.op).toEqual({ kind: "move", cellId: "c0", from: 0, to: 3 });
  });

  it("treats the target as the FINAL index when moving earlier", () => {
    const r = moveCell(makeCells(5), "c4", 1)!;
    expect(ids(r.cells)).toEqual(["c0", "c4", "c1", "c2", "c3"]);
  });

  it("moves to the first and last positions", () => {
    expect(ids(moveCell(makeCells(4), "c2", 0)!.cells)).toEqual(["c2", "c0", "c1", "c3"]);
    expect(ids(moveCell(makeCells(4), "c1", 3)!.cells)).toEqual(["c0", "c2", "c3", "c1"]);
  });

  it("clamps a target past either end", () => {
    expect(ids(moveCell(makeCells(4), "c1", 99)!.cells)).toEqual(["c0", "c2", "c3", "c1"]);
    expect(ids(moveCell(makeCells(4), "c2", -5)!.cells)).toEqual(["c2", "c0", "c1", "c3"]);
  });

  it("returns null on a no-op: same index, unknown id, or a clamp landing on the current index", () => {
    const cells = makeCells(4);
    expect(moveCell(cells, "c2", 2)).toBeNull();
    expect(moveCell(cells, "nope", 1)).toBeNull();
    expect(moveCell(cells, "c3", 99)).toBeNull();
    expect(moveCell(cells, "c0", -3)).toBeNull();
  });

  it("never mutates the input and keeps unaffected cells identical", () => {
    const cells = makeCells(4);
    const before = ids(cells);
    const r = moveCell(cells, "c0", 2)!;
    expect(ids(cells)).toEqual(before);
    expect(r.cells).not.toBe(cells);
    for (const cell of cells) expect(r.cells).toContain(cell);
  });

  it("inverts every (from, to) pair on a 5-cell list, restoring order and object identity", () => {
    const base = makeCells(5);
    for (let from = 0; from < base.length; from += 1) {
      for (let to = 0; to < base.length; to += 1) {
        if (from === to) continue;
        const moved = moveCell(base, base[from].id, to)!;
        expect(moved.cells[to]).toBe(base[from]);
        const back = applyOperation(moved.cells, moved.inverse)!;
        expect(ids(back.cells)).toEqual(ids(base));
        back.cells.forEach((cell, i) => expect(cell).toBe(base[i]));
      }
    }
  });
});

describe("moveCellBy", () => {
  it("steps one position in either direction", () => {
    expect(ids(moveCellBy(makeCells(4), "c1", 1)!.cells)).toEqual(["c0", "c2", "c1", "c3"]);
    expect(ids(moveCellBy(makeCells(4), "c2", -1)!.cells)).toEqual(["c0", "c2", "c1", "c3"]);
  });

  it("returns null at the ends and for an unknown id", () => {
    const cells = makeCells(3);
    expect(moveCellBy(cells, "c0", -1)).toBeNull();
    expect(moveCellBy(cells, "c2", 1)).toBeNull();
    expect(moveCellBy(cells, "nope", 1)).toBeNull();
  });
});

describe("insertCell", () => {
  const extra = (): TestCell => ({ id: "new", code: "fresh", output: null });

  it("inserts at the given index", () => {
    const r = insertCell(makeCells(3), extra(), 1);
    expect(ids(r.cells)).toEqual(["c0", "new", "c1", "c2"]);
    expect(r.op).toMatchObject({ kind: "insert", index: 1 });
  });

  it("clamps the index to [0, len]", () => {
    expect(ids(insertCell(makeCells(3), extra(), 99).cells)).toEqual(["c0", "c1", "c2", "new"]);
    expect(ids(insertCell(makeCells(3), extra(), -4).cells)).toEqual(["new", "c0", "c1", "c2"]);
    const clamped = insertCell(makeCells(3), extra(), 99).op;
    expect(clamped.kind === "insert" && clamped.index).toBe(3);
  });

  it("inverts to a remove of the same reference at the same index", () => {
    const cell = extra();
    const r = insertCell(makeCells(3), cell, 2);
    expect(r.inverse).toEqual({ kind: "remove", cell, index: 2 });
    expect(r.inverse.kind === "remove" && r.inverse.cell).toBe(cell);
    const back = applyOperation(r.cells, r.inverse)!;
    expect(ids(back.cells)).toEqual(["c0", "c1", "c2"]);
  });

  it("never mutates the input", () => {
    const cells = makeCells(2);
    insertCell(cells, extra(), 0);
    expect(ids(cells)).toEqual(["c0", "c1"]);
  });
});

describe("removeCell", () => {
  it("returns the live cell reference with its output intact", () => {
    const cells = makeCells(3);
    const r = removeCell(cells, "c1")!;
    expect(ids(r.cells)).toEqual(["c0", "c2"]);
    expect(r.op.kind === "remove" && r.op.cell).toBe(cells[1]);
    expect(r.op.kind === "remove" && r.op.cell.output).toBe("out 1");
  });

  it("refuses to drop below minCells (default 1)", () => {
    expect(removeCell(makeCells(1), "c0")).toBeNull();
    expect(removeCell(makeCells(3), "c0", { minCells: 3 })).toBeNull();
    expect(removeCell(makeCells(1), "c0", { minCells: 0 })).not.toBeNull();
  });

  it("returns null for an unknown id", () => {
    expect(removeCell(makeCells(3), "nope")).toBeNull();
  });

  it("inverts to an insert of the same reference at the same index", () => {
    const cells = makeCells(4);
    const r = removeCell(cells, "c2")!;
    const back = applyOperation(r.cells, r.inverse)!;
    expect(ids(back.cells)).toEqual(ids(cells));
    expect(back.cells[2]).toBe(cells[2]);
    expect(back.cells[2].output).toBe("out 2");
  });

  it("never mutates the input", () => {
    const cells = makeCells(3);
    removeCell(cells, "c0");
    expect(ids(cells)).toEqual(["c0", "c1", "c2"]);
  });
});

describe("duplicateCell", () => {
  it("inserts the clone directly below the source with a fresh id", () => {
    const cells = makeCells(3);
    const r = duplicateCell(cells, "c1", cloneCell)!;
    expect(r.cells).toHaveLength(4);
    expect(r.cells[2].code).toBe("code 1");
    expect(r.cells[2].output).toBeNull();
    expect(r.cells[2].id).not.toBe("c1");
    expect(r.cells[1]).toBe(cells[1]);
    expect(ids(r.cells).slice(0, 2)).toEqual(["c0", "c1"]);
    expect(ids(r.cells)[3]).toBe("c2");
  });

  it("uses the injected id factory", () => {
    const r = duplicateCell(makeCells(2), "c0", cloneCell, () => "fixed")!;
    expect(r.cells[1].id).toBe("fixed");
    expect(r.op).toEqual({
      kind: "duplicate",
      cell: r.cells[1],
      index: 1,
      sourceId: "c0",
    });
  });

  it("returns null for an unknown id", () => {
    expect(duplicateCell(makeCells(2), "nope", cloneCell)).toBeNull();
  });

  it("inverts to a remove of the new cell at sourceIndex + 1", () => {
    const cells = makeCells(3);
    const r = duplicateCell(cells, "c0", cloneCell, () => "dup")!;
    expect(r.inverse).toEqual({ kind: "remove", cell: r.cells[1], index: 1 });
    const back = applyOperation(r.cells, r.inverse)!;
    expect(ids(back.cells)).toEqual(ids(cells));
    back.cells.forEach((cell, i) => expect(cell).toBe(cells[i]));
  });

  it("never mutates the input", () => {
    const cells = makeCells(2);
    duplicateCell(cells, "c0", cloneCell);
    expect(ids(cells)).toEqual(["c0", "c1"]);
  });
});

describe("applyOperation", () => {
  it("replays each op kind", () => {
    const cells = makeCells(4);
    const moved = moveCell(cells, "c0", 2)!;
    expect(ids(applyOperation(cells, moved.op)!.cells)).toEqual(ids(moved.cells));

    const inserted = insertCell(cells, { id: "new", code: "", output: null }, 1);
    expect(ids(applyOperation(cells, inserted.op)!.cells)).toEqual(ids(inserted.cells));

    const removed = removeCell(cells, "c1")!;
    expect(ids(applyOperation(cells, removed.op)!.cells)).toEqual(ids(removed.cells));

    const duplicated = duplicateCell(cells, "c1", cloneCell, () => "dup")!;
    const replayed = applyOperation(cells, duplicated.op)!;
    expect(ids(replayed.cells)).toEqual(ids(duplicated.cells));
    expect(replayed.op.kind).toBe("duplicate");
  });

  it("returns null when the state diverged", () => {
    const cells = makeCells(3);
    expect(applyOperation(cells, { kind: "move", cellId: "gone", from: 0, to: 1 })).toBeNull();
    expect(
      applyOperation(cells, {
        kind: "remove",
        cell: { id: "gone", code: "", output: null },
        index: 0,
      }),
    ).toBeNull();
  });

  it("replays a remove that empties the list — the minCells floor is a UI policy", () => {
    const cells = makeCells(1);
    const replayed = applyOperation(cells, { kind: "remove", cell: cells[0], index: 0 })!;
    expect(replayed.cells).toEqual([]);
  });

  it("normalizes a clamped index on the returned op", () => {
    const cell: TestCell = { id: "new", code: "", output: null };
    const r = applyOperation(makeCells(2), { kind: "insert", cell, index: 99 })!;
    expect(r.op.kind === "insert" && r.op.index).toBe(2);
  });
});

describe("cellMoveAnnouncement", () => {
  it("speaks 1-based positions", () => {
    expect(cellMoveAnnouncement(2, 1, 7)).toBe("Cell 3 moved to position 2 of 7");
    expect(cellMoveAnnouncement(0, 2, 5)).toBe("Cell 1 moved to position 3 of 5");
  });
});

describe("newCellId", () => {
  it("returns unique non-empty ids", () => {
    const seen = new Set(Array.from({ length: 50 }, () => newCellId()));
    expect(seen.size).toBe(50);
    for (const id of seen) expect(id.length).toBeGreaterThan(0);
  });
});
