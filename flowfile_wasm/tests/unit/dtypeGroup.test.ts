import { describe, it, expect } from 'vitest'
import { dataTypeGroup, dataTypeBadgeClass, dataTypeLabel } from '../../src/utils/dtypeGroup'

describe('dataTypeGroup', () => {
  it('maps string dtypes to String', () => {
    for (const t of ['String', 'Utf8', 'VARCHAR', 'CHAR', 'NVARCHAR']) {
      expect(dataTypeGroup(t)).toBe('String')
    }
  })

  it('maps numeric (and boolean/binary, mirroring the backend) to Numeric', () => {
    for (const t of ['Int64', 'Int32', 'UInt8', 'Float64', 'Float32', 'Boolean', 'Binary', 'Decimal']) {
      expect(dataTypeGroup(t)).toBe('Numeric')
    }
  })

  it('maps date/time dtypes to Date', () => {
    for (const t of ['Date', 'Datetime', 'Time']) {
      expect(dataTypeGroup(t)).toBe('Date')
    }
  })

  it('strips dtype parameters before matching', () => {
    expect(dataTypeGroup("Datetime(time_unit='us', time_zone=None)")).toBe('Date')
    expect(dataTypeGroup('Decimal(38,0)')).toBe('Numeric')
  })

  it('falls back to Other for unknown/complex dtypes and empty input', () => {
    expect(dataTypeGroup('List(Int64)')).toBe('Other')
    expect(dataTypeGroup('Struct')).toBe('Other')
    expect(dataTypeGroup('')).toBe('Other')
    expect(dataTypeGroup(undefined)).toBe('Other')
    expect(dataTypeGroup(null)).toBe('Other')
  })
})

describe('dataTypeLabel', () => {
  it('returns short dtypes unchanged', () => {
    expect(dataTypeLabel('Int64')).toBe('Int64')
    expect(dataTypeLabel('String')).toBe('String')
  })

  it('strips parameters so the label fits a badge', () => {
    expect(dataTypeLabel("Datetime(time_unit='us', time_zone=None)")).toBe('Datetime')
    expect(dataTypeLabel('Decimal(38,0)')).toBe('Decimal')
    expect(dataTypeLabel('List(Int64)')).toBe('List')
  })

  it('falls back to "unknown" for empty input', () => {
    expect(dataTypeLabel('')).toBe('unknown')
    expect(dataTypeLabel('   ')).toBe('unknown')
    expect(dataTypeLabel(undefined)).toBe('unknown')
    expect(dataTypeLabel(null)).toBe('unknown')
  })
})

describe('dataTypeBadgeClass', () => {
  it('maps each group to its badge class', () => {
    expect(dataTypeBadgeClass('String')).toBe('badge-string')
    expect(dataTypeBadgeClass('Numeric')).toBe('badge-numeric')
    expect(dataTypeBadgeClass('Date')).toBe('badge-date')
    expect(dataTypeBadgeClass('Other')).toBe('badge-other')
  })
})
