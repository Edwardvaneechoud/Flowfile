/**
 * Guards the CSV "Infer data types" option in ReadFileSettings.vue (#593):
 * the checkbox must stay wired to table_settings.infer_schema through
 * updateTableSetting, and every CSV table_settings default the component
 * constructs must carry infer_schema: true so exported flows serialize the
 * setting explicitly. Source-shape assertions, no component mounting.
 */

import { describe, it, expect } from 'vitest'
import { readFileSync } from 'fs'
import { resolve } from 'path'

const source = readFileSync(
  resolve(__dirname, '../../src/components/nodes/ReadFileSettings.vue'),
  'utf-8'
)

describe('ReadFileSettings CSV infer_schema', () => {
  it('exposes an Infer data types checkbox wired to table_settings.infer_schema', () => {
    expect(source).toContain('Infer data types')
    expect(source).toContain("updateTableSetting('infer_schema'")
    expect(source).toContain("csvSettings?.infer_schema ?? true")
  })

  it('explains that unchecking loads every column as text', () => {
    expect(source).toMatch(/unchecked.*loads as text/i)
  })

  it('defaults infer_schema to true in every CSV table_settings object', () => {
    const csvDefaults = source.match(/file_type: 'csv',[\s\S]*?starting_from_line[^\n]*\n[\s\S]*?\}/g) ?? []
    expect(csvDefaults.length).toBeGreaterThanOrEqual(3)
    for (const block of csvDefaults) {
      expect(block).toContain('infer_schema: true')
    }
  })
})
