/**
 * Files this package copies from the full app's notebook must stay the copy they are.
 * A change there is taken over here by copying the file again.
 */

import { describe, it, expect } from 'vitest'
import { readFileSync } from 'node:fs'
import { resolve } from 'node:path'

const DESKTOP = resolve(__dirname, '../../../flowfile_frontend/src/renderer/app/components/notebook')
const BROWSER = resolve(__dirname, '../../src/components/notebook')

describe('copies of the full app notebook modules', () => {
  it.each(['syncErrorLine.ts'])('%s is the full app file, byte for byte', name => {
    expect(readFileSync(resolve(BROWSER, name), 'utf8')).toBe(readFileSync(resolve(DESKTOP, name), 'utf8'))
  })
})
