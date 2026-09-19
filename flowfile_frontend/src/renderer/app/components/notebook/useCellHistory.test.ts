import { describe, it, expect } from "vitest";
import {
  applyOperation,
  moveCell,
  removeCell,
  type CellLike,
  type CellOperation,
} from "./cellOperations";
import {
  createCellHistory,
  disposeCellHistory,
  getCellHistory,
  type HistoryEntry,
} from "./useCellHistory";

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

function entry(id: string): HistoryEntry<TestCell> {
  const cell: TestCell = { id, code: id, output: null };
  return {
    op: { kind: "insert", cell, index: 0 },
    inverse: { kind: "remove", cell, index: 0 },
  };
}

function labelOf(op: CellOperation<TestCell>): string {
  return op.kind === "insert" ? op.cell.id : op.kind;
}

describe("createCellHistory", () => {
  it("undoes and redoes in LIFO order", () => {
    const history = createCellHistory<TestCell>();
    history.push(entry("a"));
    history.push(entry("b"));

    expect(labelOf(history.undo()!.op)).toBe("b");
    expect(labelOf(history.undo()!.op)).toBe("a");
    expect(history.undo()).toBeNull();

    expect(labelOf(history.redo()!.op)).toBe("a");
    expect(labelOf(history.redo()!.op)).toBe("b");
    expect(history.redo()).toBeNull();
  });

  it("tracks canUndo / canRedo", () => {
    const history = createCellHistory<TestCell>();
    expect(history.canUndo.value).toBe(false);
    expect(history.canRedo.value).toBe(false);

    history.push(entry("a"));
    expect(history.canUndo.value).toBe(true);
    expect(history.canRedo.value).toBe(false);

    history.undo();
    expect(history.canUndo.value).toBe(false);
    expect(history.canRedo.value).toBe(true);

    history.redo();
    expect(history.canUndo.value).toBe(true);
    expect(history.canRedo.value).toBe(false);
  });

  it("clears the redo stack on push", () => {
    const history = createCellHistory<TestCell>();
    history.push(entry("a"));
    history.undo();
    expect(history.canRedo.value).toBe(true);

    history.push(entry("b"));
    expect(history.canRedo.value).toBe(false);
    expect(history.redo()).toBeNull();
    expect(labelOf(history.undo()!.op)).toBe("b");
  });

  it("keeps at most 50 entries by default, dropping the oldest", () => {
    const history = createCellHistory<TestCell>();
    for (let i = 0; i < 55; i += 1) history.push(entry(`e${i}`));

    const undone: string[] = [];
    for (let i = 0; i < 50; i += 1) undone.push(labelOf(history.undo()!.op));
    expect(history.undo()).toBeNull();
    expect(undone[0]).toBe("e54");
    expect(undone[49]).toBe("e5");
  });

  it("honours a custom limit", () => {
    const history = createCellHistory<TestCell>(2);
    history.push(entry("a"));
    history.push(entry("b"));
    history.push(entry("c"));
    expect(labelOf(history.undo()!.op)).toBe("c");
    expect(labelOf(history.undo()!.op)).toBe("b");
    expect(history.undo()).toBeNull();
  });

  it("clear() empties both stacks", () => {
    const history = createCellHistory<TestCell>();
    history.push(entry("a"));
    history.push(entry("b"));
    history.undo();

    history.clear();
    expect(history.canUndo.value).toBe(false);
    expect(history.canRedo.value).toBe(false);
    expect(history.undo()).toBeNull();
    expect(history.redo()).toBeNull();
  });
});

describe("the owner-keyed registry", () => {
  it("returns the same instance per owner and a separate one per owner id", () => {
    const a = getCellHistory<TestCell>("owner-a");
    expect(getCellHistory<TestCell>("owner-a")).toBe(a);
    expect(getCellHistory<TestCell>("owner-b")).not.toBe(a);
    disposeCellHistory("owner-a");
    disposeCellHistory("owner-b");
  });

  it("drops the entry on dispose so the next get starts fresh", () => {
    const a = getCellHistory<TestCell>("owner-c");
    a.push(entry("a"));
    expect(a.canUndo.value).toBe(true);

    disposeCellHistory("owner-c");
    const fresh = getCellHistory<TestCell>("owner-c");
    expect(fresh).not.toBe(a);
    expect(fresh.canUndo.value).toBe(false);
    disposeCellHistory("owner-c");
  });
});

describe("history + cellOperations", () => {
  it("restores order, source and a deleted cell's output across two undos", () => {
    const history = createCellHistory<TestCell>();
    const original = makeCells(5);

    const moved = moveCell(original, "c2", 0)!;
    history.push({ op: moved.op, inverse: moved.inverse });
    expect(ids(moved.cells)).toEqual(["c2", "c0", "c1", "c3", "c4"]);

    const removed = removeCell(moved.cells, "c0")!;
    history.push({ op: removed.op, inverse: removed.inverse });
    expect(ids(removed.cells)).toEqual(["c2", "c1", "c3", "c4"]);

    const undoRemove = applyOperation(removed.cells, history.undo()!.inverse)!;
    expect(ids(undoRemove.cells)).toEqual(["c2", "c0", "c1", "c3", "c4"]);
    expect(undoRemove.cells[1]).toBe(original[0]);
    expect(undoRemove.cells[1].output).toBe("out 0");
    expect(undoRemove.cells[1].code).toBe("code 0");

    const undoMove = applyOperation(undoRemove.cells, history.undo()!.inverse)!;
    expect(ids(undoMove.cells)).toEqual(ids(original));
    undoMove.cells.forEach((cell, i) => expect(cell).toBe(original[i]));

    expect(history.canUndo.value).toBe(false);
    expect(history.canRedo.value).toBe(true);
  });
});
