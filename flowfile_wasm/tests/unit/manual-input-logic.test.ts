/**
 * Tests for the Manual Input column data-type inference.
 *
 * Guards the strict numeric check: parseFloat('3 apples') === 3 used to
 * misclassify text-leading-with-digits columns as Float64, which then
 * failed/nulled at the Polars cast on run.
 */
import { describe, it, expect } from 'vitest'
import { inferColumnDataType } from '../../src/utils/manualInputLogic'

describe('inferColumnDataType', () => {
  it('does not misclassify text leading with digits as numeric', () => {
    expect(inferColumnDataType(['3 apples', '5 pears'])).toBe('String')
    expect(inferColumnDataType(['3', '3 apples'])).toBe('String')
    expect(inferColumnDataType(['12abc'])).toBe('String')
  })

  it('infers Int64 for integer strings', () => {
    expect(inferColumnDataType(['3', '42', '-7'])).toBe('Int64')
  })

  it('infers Float64 for float strings', () => {
    expect(inferColumnDataType(['3.5', '-0.25'])).toBe('Float64')
  })

  it('infers Float64 for mixed ints and floats', () => {
    expect(inferColumnDataType(['3', '3.5'])).toBe('Float64')
  })

  it('ignores empty, null and undefined values when inferring', () => {
    expect(inferColumnDataType(['', '3', null, '4', undefined])).toBe('Int64')
  })

  it('defaults to String when there are no valid values', () => {
    expect(inferColumnDataType([])).toBe('String')
    expect(inferColumnDataType(['', null, undefined])).toBe('String')
  })

  it('treats whitespace-only values as non-numeric', () => {
    expect(inferColumnDataType([' ', '3'])).toBe('String')
  })

  it('infers Boolean for true/false strings', () => {
    expect(inferColumnDataType(['true', 'false', 'true'])).toBe('Boolean')
    expect(inferColumnDataType(['true', 'maybe'])).toBe('String')
  })

  it('keeps date-like strings as String', () => {
    expect(inferColumnDataType(['2024-01-01', '2024-02-01'])).toBe('String')
  })
})
