/**
 * Persistence-layer tests for panel layout intent (ported from the desktop
 * frontend's layoutIntent.test.ts): v4 record validation, and one-time
 * migration of legacy v3 rendered-rect records — including the corruption
 * fingerprints that must heal to defaults.
 */

import { describe, expect, it } from 'vitest'

import type { PanelConfig } from '../../src/components/common/DraggableItem/layoutGeometry'
import {
  intentStorageKey,
  legacyV3StorageKey,
  migrateV3,
  sanitizeIntent,
} from '../../src/components/common/DraggableItem/layoutIntent'

const rightDrawerCfg: PanelConfig = {
  defaultDock: 'right',
  h: { behaviour: 'fixed', defaultSize: 600, defaultOffset: 0 },
  v: { behaviour: 'scale', defaultSize: 700, defaultOffset: 8 },
}

const bottomDockCfg: PanelConfig = {
  defaultDock: 'bottom',
  h: { behaviour: 'scale', defaultSize: null, defaultOffset: 180 },
  v: { behaviour: 'fixed', defaultSize: 225, defaultOffset: 0 },
}

const v3Record = (patch: Record<string, unknown> = {}): Record<string, unknown> => ({
  width: 640,
  height: 800,
  left: 560,
  top: 8,
  stickynessPosition: 'right',
  fullWidth: false,
  fullHeight: false,
  zIndex: 100,
  fullScreen: false,
  clicked: false,
  ...patch,
})

describe('storage keys', () => {
  it('v4 and v3 keys use the shared prefix with distinct versions', () => {
    expect(intentStorageKey('rightDrawer')).toBe('overlayPositionAndSize.v4_rightDrawer')
    expect(legacyV3StorageKey('rightDrawer')).toBe('overlayPositionAndSize.v3_rightDrawer')
  })
})

describe('migrateV3 — corruption fingerprints', () => {
  it('rejects the exact reported bug record (150x100 at top:0 left:464)', () => {
    const bug = v3Record({ width: 150, height: 100, left: 464, top: 0 })
    expect(migrateV3(bug, rightDrawerCfg)).toBeNull()
  })

  it('rejects one-axis crushes that older sweeps missed', () => {
    expect(migrateV3(v3Record({ width: 600, height: 100 }), rightDrawerCfg)).toBeNull()
    expect(migrateV3(v3Record({ width: 100, height: 400 }), rightDrawerCfg)).toBeNull()
  })

  it('rejects non-finite, negative, and absurd geometry', () => {
    expect(migrateV3(v3Record({ width: NaN }), rightDrawerCfg)).toBeNull()
    expect(migrateV3(v3Record({ left: -20 }), rightDrawerCfg)).toBeNull()
    expect(migrateV3(v3Record({ top: 50000 }), rightDrawerCfg)).toBeNull()
    expect(migrateV3(v3Record({ height: '300' }), rightDrawerCfg)).toBeNull()
    expect(migrateV3('{ not json', rightDrawerCfg)).toBeNull()
    expect(migrateV3(null, rightDrawerCfg)).toBeNull()
  })

  it('rejects unknown and retired dock values', () => {
    // wasm's old ItemLayout union included 'bottom-center' — it must not survive.
    expect(migrateV3(v3Record({ stickynessPosition: 'bottom-center' }), rightDrawerCfg)).toBeNull()
    expect(migrateV3(v3Record({ stickynessPosition: 'middle' }), rightDrawerCfg)).toBeNull()
  })
})

describe('migrateV3 — healthy records', () => {
  it('keeps only the fixed-axis size for a right-docked drawer', () => {
    expect(migrateV3(v3Record(), rightDrawerCfg)).toEqual({
      dock: 'right',
      h: { size: 640, offset: null, gap: null },
      // Scale-axis height is a derived value in v3 (the corruptible one) and
      // the top offset was a seeded default — both re-derive from defaults.
      v: { size: null, offset: null, gap: null },
      fullScreen: false,
    })
  })

  it('keeps only the height for the bottom dock (left was a seeded default)', () => {
    const record = v3Record({
      width: 1020,
      height: 225,
      left: 180,
      top: 675,
      stickynessPosition: 'bottom',
    })
    expect(migrateV3(record, bottomDockCfg)).toEqual({
      dock: 'bottom',
      h: { size: null, offset: null, gap: null },
      v: { size: 225, offset: null, gap: null },
      fullScreen: false,
    })
  })

  it('keeps sizes and offsets for a free panel', () => {
    const record = v3Record({
      width: 500,
      height: 400,
      left: 120,
      top: 60,
      stickynessPosition: 'free',
    })
    expect(migrateV3(record, rightDrawerCfg)).toEqual({
      dock: 'free',
      h: { size: 500, offset: 120, gap: null },
      v: { size: 400, offset: 60, gap: null },
      fullScreen: false,
    })
  })

  it('drops an out-of-range fixed size but keeps the rest of the record', () => {
    const record = v3Record({ width: 7000 })
    expect(migrateV3(record, rightDrawerCfg)).toEqual({
      dock: 'right',
      h: { size: null, offset: null, gap: null },
      v: { size: null, offset: null, gap: null },
      fullScreen: false,
    })
  })
})

describe('sanitizeIntent', () => {
  it('accepts migrateV3 output unchanged (idempotence)', () => {
    const migrated = migrateV3(v3Record(), rightDrawerCfg)
    expect(migrated).not.toBeNull()
    expect(sanitizeIntent(migrated)).toEqual(migrated)
  })

  it('normalizes persisted fullScreen back to false', () => {
    const migrated = migrateV3(v3Record(), rightDrawerCfg)
    expect(sanitizeIntent({ ...migrated, fullScreen: true })?.fullScreen).toBe(false)
  })

  it('rejects structural garbage', () => {
    expect(sanitizeIntent(null)).toBeNull()
    expect(sanitizeIntent('nope')).toBeNull()
    expect(sanitizeIntent({})).toBeNull()
    expect(sanitizeIntent({ dock: 'diagonal', h: {}, v: {} })).toBeNull()
    const migrated = migrateV3(v3Record(), rightDrawerCfg)
    expect(sanitizeIntent({ ...migrated, h: undefined })).toBeNull()
  })

  it('rejects below-minimum, negative, and non-finite axis values', () => {
    const base = migrateV3(v3Record(), rightDrawerCfg)!
    expect(sanitizeIntent({ ...base, h: { size: 100, offset: null, gap: null } })).toBeNull()
    expect(sanitizeIntent({ ...base, v: { size: null, offset: -5, gap: null } })).toBeNull()
    expect(sanitizeIntent({ ...base, v: { size: null, offset: null, gap: NaN } })).toBeNull()
    expect(sanitizeIntent({ ...base, v: { size: null, offset: null, gap: '8' } })).toBeNull()
  })

  it('clamps over-maximum values instead of resetting the layout (huge displays)', () => {
    const base = migrateV3(v3Record(), rightDrawerCfg)!
    expect(sanitizeIntent({ ...base, h: { size: 7000, offset: null, gap: null } })?.h.size).toBe(
      6000,
    )
    expect(sanitizeIntent({ ...base, v: { size: null, offset: 20000, gap: null } })?.v.offset).toBe(
      10000,
    )
  })

  it('treats missing axis fields as null', () => {
    const intent = sanitizeIntent({ dock: 'right', h: { size: 600 }, v: {} })
    expect(intent).toEqual({
      dock: 'right',
      h: { size: 600, offset: null, gap: null },
      v: { size: null, offset: null, gap: null },
      fullScreen: false,
    })
  })
})
