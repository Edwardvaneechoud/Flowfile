/**
 * Pins the node catalog's internal consistency: SUPPORTED_NODE_TYPES must be
 * exactly the palette's available entries. The settings-panel map is pinned at
 * compile time (`satisfies Record<SupportedNodeType, …>` in Canvas.vue), and
 * core's share-link manifest generator parses this same file — so this test is
 * what keeps the whole chain honest when a node is added.
 */

import { describe, it, expect } from 'vitest'
import {
  createNodeCategories,
  isSupportedNodeType,
  lockedNodeTypes,
  SUPPORTED_NODE_TYPES
} from '../../src/config/nodeCatalog'

describe('node catalog consistency', () => {
  const palette = createNodeCategories().flatMap(cat => cat.nodes)
  const available = palette.filter(n => n.available !== false).map(n => n.type)
  const locked = palette.filter(n => n.available === false).map(n => n.type)

  it('SUPPORTED_NODE_TYPES is exactly the available palette set', () => {
    expect(new Set(SUPPORTED_NODE_TYPES)).toEqual(new Set(available))
  })

  it('supported and locked partition the palette (no overlap, expected sizes)', () => {
    expect(available).toHaveLength(23)
    expect(locked).toHaveLength(16)
    expect(available.filter(t => locked.includes(t))).toEqual([])
    expect(new Set(lockedNodeTypes())).toEqual(new Set(locked))
  })

  it('palette types are unique', () => {
    expect(new Set(palette.map(n => n.type)).size).toBe(palette.length)
  })

  it('isSupportedNodeType answers for both tiers and for unknowns', () => {
    expect(isSupportedNodeType('polars_code')).toBe(true)
    expect(isSupportedNodeType('head')).toBe(true)
    expect(isSupportedNodeType('gate')).toBe(false)
    expect(isSupportedNodeType('database_reader')).toBe(false)
    expect(isSupportedNodeType('filter__unsupported')).toBe(false)
    expect(isSupportedNodeType('some_custom_node')).toBe(false)
  })

  it('every locked entry carries a docs anchor or at least a category docs page', () => {
    for (const cat of createNodeCategories()) {
      for (const node of cat.nodes) {
        if (node.available === false) {
          expect(cat.docsUrl, `${node.type} needs a docs destination`).toBeTruthy()
        }
      }
    }
  })
})
