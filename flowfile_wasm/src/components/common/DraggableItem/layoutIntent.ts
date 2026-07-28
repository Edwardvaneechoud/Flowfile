// Persistence layer for panel layout intent: storage keys, validation of
// stored records, and one-time migration of legacy v3 geometry. Pure — no
// Vue, no DOM, no direct localStorage access (the store owns I/O).

import type { AxisIntent, PanelConfig, PanelDock, PanelIntent } from './layoutGeometry'
import { MIN_PANEL_H, MIN_PANEL_W } from './layoutGeometry'

// v4: layout records store user INTENT (dock + per-axis size/offset/gap),
// not derived on-screen geometry. v3 records stored the rendered rect and
// were corruptible by adaptive clamps; they are migrated on first load.
export const STORAGE_VERSION = 4
export const intentStorageKey = (id: string): string =>
  `overlayPositionAndSize.v${STORAGE_VERSION}_${id}`
export const legacyV3StorageKey = (id: string): string => `overlayPositionAndSize.v3_${id}`

const MAX_PANEL_PX = 6000
const MAX_OFFSET_PX = 10000

const DOCK_VALUES: readonly PanelDock[] = ['top', 'bottom', 'left', 'right', 'free']

const isFiniteNumber = (value: unknown): value is number =>
  typeof value === 'number' && Number.isFinite(value)

const inRange = (value: number, min: number, max: number): boolean => value >= min && value <= max

// Below-minimum and non-finite values are rejected (gestures can't produce
// them — they mark corruption); over-maximum values are CLAMPED, not
// rejected: a legitimate gesture on a very large display can exceed the
// caps, and resetting the whole layout over it would punish real intent.
const sanitizeAxis = (raw: unknown, minSize: number): AxisIntent | null => {
  if (typeof raw !== 'object' || raw === null) return null
  const axis = raw as Record<string, unknown>
  const out: AxisIntent = { size: null, offset: null, gap: null }
  if (axis.size !== null && axis.size !== undefined) {
    if (!isFiniteNumber(axis.size) || axis.size < minSize) return null
    out.size = Math.min(axis.size, MAX_PANEL_PX)
  }
  if (axis.offset !== null && axis.offset !== undefined) {
    if (!isFiniteNumber(axis.offset) || axis.offset < 0) return null
    out.offset = Math.min(axis.offset, MAX_OFFSET_PX)
  }
  if (axis.gap !== null && axis.gap !== undefined) {
    if (!isFiniteNumber(axis.gap) || axis.gap < 0) return null
    out.gap = Math.min(axis.gap, MAX_OFFSET_PX)
  }
  return out
}

// Validate a stored v4 record. Returns a normalized intent or null (caller
// falls back to defaults). fullScreen is deliberately reset: it is transient
// view state whose driver (e.g. ExploreData) re-establishes it per session.
export function sanitizeIntent(raw: unknown): PanelIntent | null {
  if (typeof raw !== 'object' || raw === null) return null
  const record = raw as Record<string, unknown>
  if (!DOCK_VALUES.includes(record.dock as PanelDock)) return null
  const h = sanitizeAxis(record.h, MIN_PANEL_W)
  const v = sanitizeAxis(record.v, MIN_PANEL_H)
  if (!h || !v) return null
  return { dock: record.dock as PanelDock, h, v, fullScreen: false }
}

interface V3Record {
  width: number
  height: number
  left: number
  top: number
  stickynessPosition: PanelDock
}

const parseV3 = (raw: unknown): V3Record | null => {
  if (typeof raw !== 'object' || raw === null) return null
  const record = raw as Record<string, unknown>
  if (
    !isFiniteNumber(record.width) ||
    !isFiniteNumber(record.height) ||
    !isFiniteNumber(record.left) ||
    !isFiniteNumber(record.top)
  ) {
    return null
  }
  if (!DOCK_VALUES.includes(record.stickynessPosition as PanelDock)) return null
  return {
    width: record.width,
    height: record.height,
    left: record.left,
    top: record.top,
    stickynessPosition: record.stickynessPosition as PanelDock,
  }
}

// Migrate a legacy v3 record (rendered-rect shape) to intent. Returns null
// for unusable records — including the corruption fingerprint: the gesture
// handlers floored strictly above 100px per axis, so any stored extent at or
// below that can only come from the old adaptive clamps gone wrong.
export function migrateV3(raw: unknown, cfg: PanelConfig): PanelIntent | null {
  const record = parseV3(raw)
  if (!record) return null
  if (record.width <= 100 || record.height <= 100) return null
  if (record.left < 0 || record.top < 0) return null
  if (record.left > MAX_OFFSET_PX || record.top > MAX_OFFSET_PX) return null

  const sizeOrNull = (value: number, minSize: number): number | null =>
    inRange(value, minSize, MAX_PANEL_PX) ? value : null

  const empty = (): AxisIntent => ({ size: null, offset: null, gap: null })

  const dock = record.stickynessPosition
  const h = empty()
  const v = empty()

  // Docked panels keep only the perpendicular "fixed" size — the one value
  // that is unambiguously user intent in a v3 record. Scale/fill sizes are
  // derived values (exactly where corruption lived), and the parallel-axis
  // offsets (right-dock top, bottom-dock left) were seeded app defaults far
  // more often than user choices — dropping them lets the current defaults
  // (flush top, next-to-palette) apply.
  switch (dock) {
    case 'right':
    case 'left':
      if (cfg.h.behaviour === 'fixed') h.size = sizeOrNull(record.width, MIN_PANEL_W)
      if (cfg.v.behaviour === 'fixed') v.size = sizeOrNull(record.height, MIN_PANEL_H)
      break
    case 'top':
    case 'bottom':
      if (cfg.v.behaviour === 'fixed') v.size = sizeOrNull(record.height, MIN_PANEL_H)
      if (cfg.h.behaviour === 'fixed') h.size = sizeOrNull(record.width, MIN_PANEL_W)
      break
    case 'free':
      h.size = sizeOrNull(record.width, MIN_PANEL_W)
      h.offset = record.left
      v.size = sizeOrNull(record.height, MIN_PANEL_H)
      v.offset = record.top
      break
  }

  return { dock, h, v, fullScreen: false }
}
