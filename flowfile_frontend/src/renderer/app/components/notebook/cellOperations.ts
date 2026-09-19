// Pure structural cell edits (move/insert/remove/duplicate). Every result carries its own
// inverse so the history stacks can stay ops-only — no snapshots, no copied outputs.

export interface CellLike {
  id: string;
}

export type CellOperation<C extends CellLike> =
  | { kind: "move"; cellId: string; from: number; to: number }
  | { kind: "insert"; cell: C; index: number }
  | { kind: "remove"; cell: C; index: number }
  | { kind: "duplicate"; cell: C; index: number; sourceId: string };

export interface OperationResult<C extends CellLike> {
  cells: C[];
  op: CellOperation<C>;
  inverse: CellOperation<C>;
}

let fallbackCounter = 0;

export function newCellId(): string {
  const webCrypto = globalThis.crypto;
  if (webCrypto && typeof webCrypto.randomUUID === "function") return webCrypto.randomUUID();
  fallbackCounter += 1;
  return `cell-${Date.now().toString(36)}-${fallbackCounter.toString(36)}`;
}

function clamp(value: number, min: number, max: number): number {
  return Math.min(Math.max(value, min), max);
}

/** `targetIndex` is the cell's FINAL index, so the op inverts exactly as `{from: to, to: from}`. */
export function moveCell<C extends CellLike>(
  cells: C[],
  cellId: string,
  targetIndex: number,
): OperationResult<C> | null {
  const from = cells.findIndex((c) => c.id === cellId);
  if (from < 0) return null;
  const to = clamp(targetIndex, 0, cells.length - 1);
  if (to === from) return null;
  const next = cells.slice();
  const [cell] = next.splice(from, 1);
  next.splice(to, 0, cell);
  return {
    cells: next,
    op: { kind: "move", cellId, from, to },
    inverse: { kind: "move", cellId, from: to, to: from },
  };
}

export function moveCellBy<C extends CellLike>(
  cells: C[],
  cellId: string,
  direction: -1 | 1,
): OperationResult<C> | null {
  const from = cells.findIndex((c) => c.id === cellId);
  if (from < 0) return null;
  return moveCell(cells, cellId, from + direction);
}

export function insertCell<C extends CellLike>(
  cells: C[],
  cell: C,
  index: number,
): OperationResult<C> {
  const at = clamp(index, 0, cells.length);
  const next = cells.slice();
  next.splice(at, 0, cell);
  return {
    cells: next,
    op: { kind: "insert", cell, index: at },
    inverse: { kind: "remove", cell, index: at },
  };
}

/** The op carries the live cell reference, so undoing a delete restores its output too. */
export function removeCell<C extends CellLike>(
  cells: C[],
  cellId: string,
  opts: { minCells?: number } = {},
): OperationResult<C> | null {
  if (cells.length <= (opts.minCells ?? 1)) return null;
  const index = cells.findIndex((c) => c.id === cellId);
  if (index < 0) return null;
  const cell = cells[index];
  const next = cells.slice();
  next.splice(index, 1);
  return {
    cells: next,
    op: { kind: "remove", cell, index },
    inverse: { kind: "insert", cell, index },
  };
}

export function duplicateCell<C extends CellLike>(
  cells: C[],
  cellId: string,
  clone: (src: C, id: string) => C,
  newId: () => string = newCellId,
): OperationResult<C> | null {
  const sourceIndex = cells.findIndex((c) => c.id === cellId);
  if (sourceIndex < 0) return null;
  const cell = clone(cells[sourceIndex], newId());
  const index = sourceIndex + 1;
  const next = cells.slice();
  next.splice(index, 0, cell);
  return {
    cells: next,
    op: { kind: "duplicate", cell, index, sourceId: cellId },
    inverse: { kind: "remove", cell, index },
  };
}

/** Replays a recorded op (or its inverse); `null` means the state diverged. */
export function applyOperation<C extends CellLike>(
  cells: C[],
  op: CellOperation<C>,
): OperationResult<C> | null {
  switch (op.kind) {
    case "move":
      return moveCell(cells, op.cellId, op.to);
    case "insert":
      return insertCell(cells, op.cell, op.index);
    case "remove":
      // A replay must be exact: the minCells floor is a UI policy, not a history one.
      return removeCell(cells, op.cell.id, { minCells: 0 });
    case "duplicate": {
      const index = clamp(op.index, 0, cells.length);
      const inserted = insertCell(cells, op.cell, index);
      return { cells: inserted.cells, op: { ...op, index }, inverse: inserted.inverse };
    }
  }
}

export function cellMoveAnnouncement(from: number, to: number, total: number): string {
  return `Cell ${from + 1} moved to position ${to + 1} of ${total}`;
}
