/**
 * Flow Tab Actions Unit Tests
 * Pure helpers behind the tab strip's right-click menu: which tab ids a
 * close action targets, and the menu options with their disabled states.
 */

import { describe, it, expect } from 'vitest'

import { computeCloseTargets, tabMenuOptions } from '../../src/utils/flowTabActions'

const ids = ['a', 'b', 'c', 'd']

describe('computeCloseTargets', () => {
  it('close returns only the target', () => {
    expect(computeCloseTargets(ids, 'b', 'close')).toEqual(['b'])
  })

  it('close-others returns everything but the target, in tab order', () => {
    expect(computeCloseTargets(ids, 'b', 'close-others')).toEqual(['a', 'c', 'd'])
  })

  it('close-all returns all ids in tab order', () => {
    expect(computeCloseTargets(ids, 'b', 'close-all')).toEqual(['a', 'b', 'c', 'd'])
  })

  it('close-left returns tabs before the target', () => {
    expect(computeCloseTargets(ids, 'c', 'close-left')).toEqual(['a', 'b'])
  })

  it('close-left on the first tab returns nothing', () => {
    expect(computeCloseTargets(ids, 'a', 'close-left')).toEqual([])
  })

  it('close-right returns tabs after the target', () => {
    expect(computeCloseTargets(ids, 'b', 'close-right')).toEqual(['c', 'd'])
  })

  it('close-right on the last tab returns nothing', () => {
    expect(computeCloseTargets(ids, 'd', 'close-right')).toEqual([])
  })

  it('close-others with a single tab returns nothing', () => {
    expect(computeCloseTargets(['a'], 'a', 'close-others')).toEqual([])
  })

  it('unknown target returns nothing for every action', () => {
    for (const action of [
      'close',
      'close-others',
      'close-all',
      'close-left',
      'close-right'
    ] as const) {
      expect(computeCloseTargets(ids, 'zz', action)).toEqual([])
    }
  })

  it('does not mutate the input array', () => {
    const input = ['x', 'y', 'z']
    computeCloseTargets(input, 'y', 'close-all')
    expect(input).toEqual(['x', 'y', 'z'])
  })
})

describe('tabMenuOptions', () => {
  const actions = (opts: ReturnType<typeof tabMenuOptions>) => opts.map((o) => o.action)
  const disabledOf = (opts: ReturnType<typeof tabMenuOptions>, action: string) =>
    opts.find((o) => o.action === action)?.disabled ?? false

  it('offers the full browser set', () => {
    expect(actions(tabMenuOptions(ids, 'b'))).toEqual([
      'rename',
      'close',
      'close-others',
      'close-all',
      'close-left',
      'close-right'
    ])
  })

  it('disables close-left on the first tab only', () => {
    expect(disabledOf(tabMenuOptions(ids, 'a'), 'close-left')).toBe(true)
    expect(disabledOf(tabMenuOptions(ids, 'b'), 'close-left')).toBe(false)
  })

  it('disables close-right on the last tab only', () => {
    expect(disabledOf(tabMenuOptions(ids, 'd'), 'close-right')).toBe(true)
    expect(disabledOf(tabMenuOptions(ids, 'c'), 'close-right')).toBe(false)
  })

  it('disables close-others (and both directions) with a single tab', () => {
    const opts = tabMenuOptions(['a'], 'a')
    expect(disabledOf(opts, 'close-others')).toBe(true)
    expect(disabledOf(opts, 'close-left')).toBe(true)
    expect(disabledOf(opts, 'close-right')).toBe(true)
  })
})
