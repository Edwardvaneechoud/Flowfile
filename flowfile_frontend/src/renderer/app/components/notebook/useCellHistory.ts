// Undo/redo stacks for structural cell edits, one per open notebook (owner id).
// Stacks hold ops with cell references and indices only — never snapshots or copied outputs.
import { computed, shallowRef } from "vue";
import type { ComputedRef } from "vue";
import type { CellLike, CellOperation } from "./cellOperations";

export interface HistoryEntry<C extends CellLike> {
  op: CellOperation<C>;
  inverse: CellOperation<C>;
}

export interface CellHistory<C extends CellLike> {
  push(entry: HistoryEntry<C>): void;
  undo(): HistoryEntry<C> | null;
  redo(): HistoryEntry<C> | null;
  canUndo: ComputedRef<boolean>;
  canRedo: ComputedRef<boolean>;
  clear(): void;
}

export function createCellHistory<C extends CellLike>(limit = 50): CellHistory<C> {
  // shallowRef + whole-array replacement: a deep ref would proxy the cell references we store.
  const undoStack = shallowRef<HistoryEntry<C>[]>([]);
  const redoStack = shallowRef<HistoryEntry<C>[]>([]);

  return {
    push(entry: HistoryEntry<C>) {
      redoStack.value = [];
      const next = [...undoStack.value, entry];
      undoStack.value = next.length > limit ? next.slice(next.length - limit) : next;
    },
    undo() {
      const stack = undoStack.value;
      if (!stack.length) return null;
      const entry = stack[stack.length - 1];
      undoStack.value = stack.slice(0, -1);
      redoStack.value = [...redoStack.value, entry];
      return entry;
    },
    redo() {
      const stack = redoStack.value;
      if (!stack.length) return null;
      const entry = stack[stack.length - 1];
      redoStack.value = stack.slice(0, -1);
      undoStack.value = [...undoStack.value, entry];
      return entry;
    },
    canUndo: computed(() => undoStack.value.length > 0),
    canRedo: computed(() => redoStack.value.length > 0),
    clear() {
      undoStack.value = [];
      redoStack.value = [];
    },
  };
}

const histories = new Map<string, CellHistory<any>>();

export function getCellHistory<C extends CellLike>(ownerId: string): CellHistory<C> {
  let history = histories.get(ownerId);
  if (!history) {
    history = createCellHistory<C>();
    histories.set(ownerId, history);
  }
  return history as CellHistory<C>;
}

export function disposeCellHistory(ownerId: string): void {
  histories.delete(ownerId);
}
