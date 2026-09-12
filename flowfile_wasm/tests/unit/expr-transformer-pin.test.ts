import { describe, it, expect } from 'vitest'
import { readFileSync, readdirSync } from 'node:fs'
import { resolve, join, relative } from 'node:path'

/**
 * One Pyodide runtime holds one polars-expr-transformer. Two micropip requests
 * for different versions means whichever node loads first wins, so a second
 * hardcoded pin turns Dynamic Rename's function list into a load-order lottery
 * (that is exactly what PR #727 left behind). Guard the invariant directly:
 * the version literal may appear in exactly one file in src/.
 */
describe('polars-expr-transformer pin', () => {
  const SRC = resolve(__dirname, '../../src')
  const CANONICAL = 'composables/useFormulaTranslation.ts'
  const LITERAL = 'polars-expr-transformer=='

  const walk = (dir: string): string[] =>
    readdirSync(dir, { withFileTypes: true }).flatMap(e => {
      const full = join(dir, e.name)
      if (e.isDirectory()) return walk(full)
      return /\.(ts|vue)$/.test(e.name) ? [full] : []
    })

  const pinned = walk(SRC)
    .filter(f => readFileSync(f, 'utf-8').includes(LITERAL))
    .map(f => relative(SRC, f).split(/[\\/]/).join('/'))

  it('is declared in exactly one file in src/', () => {
    expect(pinned).toEqual([CANONICAL])
  })

  it('matches the CPython test suite pin', () => {
    const source = readFileSync(join(SRC, CANONICAL), 'utf-8')
    const version = source.match(/EXPR_TRANSFORMER_PACKAGE\s*=\s*'([^']+)'/)?.[1]
    const requirements = readFileSync(
      resolve(__dirname, '../python/requirements.txt'),
      'utf-8'
    )
    expect(version).toBeDefined()
    expect(requirements).toContain(`${version}\n`)
  })
})
