/**
 * An undo/redo stack of whole-flow states.
 *
 * Snapshot-based: an entry is the state before a change, so undo never has to
 * know how to reverse the change. One history belongs to one open flow (a tab
 * carries its own), and it lives in memory only.
 *
 * Not a Pinia store: the flow store owns the active history and decides what a
 * state is and how to restore one.
 */

/** Same-key edits closer together than this are one step (typing, dragging). */
export const CONTINUOUS_EDIT_MS = 1000
/** Any edits closer together than this are one gesture (a drop that also connects). */
export const GESTURE_MS = 250
const DEFAULT_LIMIT = 100

export interface RecordOptions {
  /** Edits sharing a key merge while they keep coming. Omit for a step that never merges. */
  key?: string
  /** How long the key keeps merging after the previous edit; 0 merges within a gesture only. */
  mergeMs?: number
}

export class FlowHistory<State> {
  private undoStack: State[] = []
  private redoStack: State[] = []
  private lastKey: string | null = null
  private lastAt = Number.NEGATIVE_INFINITY

  constructor(
    private readonly limit = DEFAULT_LIMIT,
    private readonly now: () => number = () => Date.now()
  ) {}

  get canUndo(): boolean {
    return this.undoStack.length > 0
  }

  get canRedo(): boolean {
    return this.redoStack.length > 0
  }

  /**
   * Note a change that is about to happen. `capture` returns the state before
   * it, and is only called when the change starts a new step.
   */
  record(capture: () => State, options: RecordOptions = {}): boolean {
    const at = this.now()
    const elapsed = at - this.lastAt
    const merges =
      options.key !== undefined &&
      this.lastKey !== null &&
      (elapsed < GESTURE_MS || (options.key === this.lastKey && elapsed < (options.mergeMs ?? 0)))
    this.lastKey = options.key ?? null
    this.lastAt = at
    if (merges) return false

    this.undoStack.push(capture())
    if (this.undoStack.length > this.limit) this.undoStack.shift()
    this.redoStack = []
    return true
  }

  /** Rewrite every stored state, for a fact that holds across the whole timeline. */
  amend(change: (state: State) => void): void {
    this.undoStack.forEach(change)
    this.redoStack.forEach(change)
  }

  /** The state to go back to, or null. Entries equal to `current` changed nothing and are skipped. */
  undo(current: State, same: (a: State, b: State) => boolean): State | null {
    return this.step(this.undoStack, this.redoStack, current, same)
  }

  redo(current: State, same: (a: State, b: State) => boolean): State | null {
    return this.step(this.redoStack, this.undoStack, current, same)
  }

  private step(from: State[], onto: State[], current: State, same: (a: State, b: State) => boolean): State | null {
    this.lastKey = null
    while (from.length > 0) {
      const target = from.pop()!
      if (same(target, current)) continue
      onto.push(current)
      return target
    }
    return null
  }
}
