/**
 * Files this package copies from the full app's notebook must stay the copy they are.
 * A change there is taken over here by copying the file again.
 */

import { describe, it, expect } from 'vitest'
import { readFileSync } from 'node:fs'
import { resolve } from 'node:path'

const DESKTOP_NOTEBOOK = resolve(__dirname, '../../../flowfile_frontend/src/renderer/app/components/notebook')
const DESKTOP_SCRIPT = resolve(
  __dirname,
  '../../../flowfile_frontend/src/renderer/app/components/nodes/node-types/elements/pythonScript'
)
const BROWSER = resolve(__dirname, '../../src/components/notebook')

const COPIES: [string, string][] = [
  ['syncErrorLine.ts', DESKTOP_NOTEBOOK],
  ['flCompletions.json', DESKTOP_NOTEBOOK],
  ['dataframeColumnContext.ts', DESKTOP_SCRIPT],
  ['dataframeSchemaInference.ts', DESKTOP_SCRIPT],
  ['dataframeSchemaTypes.ts', DESKTOP_SCRIPT]
]

describe('copies of the full app notebook modules', () => {
  it.each(COPIES)('%s is the full app file, byte for byte', (name, desktop) => {
    expect(readFileSync(resolve(BROWSER, name), 'utf8')).toBe(readFileSync(resolve(desktop, name), 'utf8'))
  })
})
