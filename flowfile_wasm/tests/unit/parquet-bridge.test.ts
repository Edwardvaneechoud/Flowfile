import { describe, it, expect } from 'vitest'
import { readFileSync } from 'node:fs'
import { resolve } from 'node:path'

/**
 * Guard the bundler-opacity of the parquet-wasm CDN import: a literal
 * import("https://...") that survives into dist/flowfile-editor.js hard-fails
 * webpack5/esbuild embedders even if they never use Parquet, and pragma
 * comments don't survive esbuild minify. The mechanism that prevents it is the
 * `new Function('u', 'return import(u)')` indirection — assert it stays.
 */
describe('parquet-bridge bundler opacity', () => {
  const source = readFileSync(resolve(__dirname, '../../src/utils/parquet-bridge.ts'), 'utf-8')

  it('uses a new Function indirection for the CDN import', () => {
    expect(source).toContain("new Function('u', 'return import(u)')")
  })

  it('never writes a literal import("https://...") call', () => {
    expect(source).not.toMatch(/import\s*\(\s*['"`]https:/)
  })

  it('pins the parquet-wasm version', () => {
    expect(source).toMatch(/PARQUET_WASM_VERSION = '\d+\.\d+\.\d+'/)
  })
})
