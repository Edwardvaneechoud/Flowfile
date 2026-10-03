/**
 * The undo/redo stack: what counts as one step, and what undo hands back.
 */

import { describe, it, expect } from 'vitest'
import { CONTINUOUS_EDIT_MS, FlowHistory, GESTURE_MS } from '../../src/stores/flow-history'

const same = (a: string, b: string) => a === b

/** A history over plain strings with a clock the test moves. */
function setup(limit = 100) {
  const clock = { at: 0 }
  const history = new FlowHistory<string>(limit, () => clock.at)
  let state = 'a'
  /** Make an edit at `at`: note the state before it, then move to `next`. */
  const edit = (next: string, at: number, key?: string, mergeMs?: number) => {
    clock.at = at
    const before = state
    history.record(() => before, { key, mergeMs })
    state = next
  }
  const undo = () => {
    const target = history.undo(state, same)
    if (target !== null) state = target
    return target
  }
  const redo = () => {
    const target = history.redo(state, same)
    if (target !== null) state = target
    return target
  }
  return { history, edit, undo, redo, state: () => state }
}

describe('FlowHistory', () => {
  it('undoes and redoes separate steps in order', () => {
    const { history, edit, undo, redo, state } = setup()
    edit('b', 0, 'graph')
    edit('c', 1000, 'graph')

    expect(undo()).toBe('b')
    expect(undo()).toBe('a')
    expect(undo()).toBeNull()
    expect(history.canUndo).toBe(false)

    expect(redo()).toBe('b')
    expect(redo()).toBe('c')
    expect(redo()).toBeNull()
    expect(state()).toBe('c')
  })

  it('merges same-key edits that keep coming, however long the burst lasts', () => {
    const { edit, undo } = setup()
    for (let i = 0; i < 10; i++) edit(`typing-${i}`, i * (CONTINUOUS_EDIT_MS - 1), 'settings:4', CONTINUOUS_EDIT_MS)

    expect(undo()).toBe('a')
    expect(undo()).toBeNull()
  })

  it('starts a new step after a pause, or for another key', () => {
    const { edit, undo } = setup()
    edit('b', 0, 'settings:4', CONTINUOUS_EDIT_MS)
    edit('c', CONTINUOUS_EDIT_MS + 1, 'settings:4', CONTINUOUS_EDIT_MS)
    edit('d', CONTINUOUS_EDIT_MS + 1 + GESTURE_MS, 'settings:5', CONTINUOUS_EDIT_MS)

    expect(undo()).toBe('c')
    expect(undo()).toBe('b')
    expect(undo()).toBe('a')
  })

  it('treats edits within one gesture as one step, whatever their keys', () => {
    const { edit, undo } = setup()
    edit('node added', 0, 'graph')
    edit('edge added', GESTURE_MS - 1, 'graph')
    edit('node nudged', 2 * GESTURE_MS - 2, 'move', CONTINUOUS_EDIT_MS)

    expect(undo()).toBe('a')
    expect(undo()).toBeNull()
  })

  it('never merges an unkeyed step, nor merges the next edit into it', () => {
    const { edit, undo } = setup()
    edit('b', 0, 'graph')
    edit('patched', 1)
    edit('patched again', 2)
    edit('c', 3, 'graph')

    expect(undo()).toBe('patched again')
    expect(undo()).toBe('patched')
    expect(undo()).toBe('b')
    expect(undo()).toBe('a')
  })

  it('does not merge the first edit after an undo into what came before', () => {
    const { edit, undo } = setup()
    edit('b', 0, 'settings:4', CONTINUOUS_EDIT_MS)
    undo()
    edit('c', 1, 'settings:4', CONTINUOUS_EDIT_MS)

    expect(undo()).toBe('a')
  })

  it('drops the redo steps once a new edit is made', () => {
    const { history, edit, undo, redo } = setup()
    edit('b', 0, 'graph')
    undo()
    edit('c', 1000, 'graph')

    expect(history.canRedo).toBe(false)
    expect(redo()).toBeNull()
  })

  it('skips entries that changed nothing', () => {
    const { history, edit, undo } = setup()
    edit('b', 0, 'graph')
    // An "edit" that left the state as it was, e.g. a panel re-sending unchanged settings.
    edit('b', 1000, 'settings:4', CONTINUOUS_EDIT_MS)

    expect(undo()).toBe('a')
    expect(history.canUndo).toBe(false)
  })

  it('only captures the state when a step starts', () => {
    const history = new FlowHistory<string>(100, () => 0)
    let captures = 0
    const capture = () => {
      captures++
      return 'a'
    }
    for (let i = 0; i < 50; i++) history.record(capture, { key: 'move', mergeMs: CONTINUOUS_EDIT_MS })

    expect(captures).toBe(1)
  })

  it('amends every stored state, on both stacks', () => {
    const history = new FlowHistory<{ files: string[] }>(100, () => 0)
    const sameFiles = (a: { files: string[] }, b: { files: string[] }) => a.files.join() === b.files.join()
    history.record(() => ({ files: [] }))
    history.record(() => ({ files: ['x'] }))
    // One entry stays on the undo stack, the state undone from goes onto the redo stack.
    history.undo({ files: ['x', 'y'] }, sameFiles)

    history.amend(state => state.files.push('late'))

    expect(history.redo({ files: ['x'] }, sameFiles)).toEqual({ files: ['x', 'y', 'late'] })
    expect(history.undo({ files: ['x', 'y', 'late'] }, sameFiles)).toEqual({ files: ['x'] })
    expect(history.undo({ files: ['x'] }, sameFiles)).toEqual({ files: ['late'] })
  })

  it('forgets the oldest steps beyond its limit', () => {
    const { edit, undo } = setup(2)
    edit('b', 0, 'graph')
    edit('c', 1000, 'graph')
    edit('d', 2000, 'graph')

    expect(undo()).toBe('c')
    expect(undo()).toBe('b')
    expect(undo()).toBeNull()
  })
})
