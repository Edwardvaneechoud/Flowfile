/**
 * Find a CPython interpreter for suites that shell out to real Python.
 *
 * Resolution order: FLOWFILE_TEST_PYTHON, the PATH pythons, this checkout's
 * Poetry venv, then any monorepo Poetry venv — a git worktree gets its own
 * (often empty) venv, so `poetry env info` alone can point at an interpreter
 * with no Polars while the main checkout's venv has one.
 */

import { execFileSync } from 'node:child_process'
import { existsSync, readdirSync } from 'node:fs'
import { join, resolve } from 'node:path'

/** The Poetry venv for whichever checkout we are in, if Poetry is installed. */
function poetryPython(): string | null {
  try {
    return execFileSync('poetry', ['env', 'info', '--executable'], {
      cwd: resolve(__dirname, '../../..'),
      encoding: 'utf-8',
      stdio: ['ignore', 'pipe', 'ignore']
    }).trim()
  } catch {
    return null
  }
}

function poetryVenvPythons(): string[] {
  const root = join(process.env.HOME ?? '', '.cache/pypoetry/virtualenvs')
  if (!existsSync(root)) return []
  return readdirSync(root)
    .filter(name => name.startsWith('flowfile-'))
    .map(name => join(root, name, 'bin', 'python'))
    .filter(existsSync)
}

export function findPython(options: { requirePolars?: boolean } = {}): string | null {
  const probe = options.requirePolars ? 'import polars' : ''
  const candidates = [process.env.FLOWFILE_TEST_PYTHON, 'python3', 'python', poetryPython(), ...poetryVenvPythons()]
  for (const candidate of candidates) {
    if (!candidate) continue
    try {
      execFileSync(candidate, ['-c', probe], { stdio: 'ignore' })
      return candidate
    } catch {
      /* try the next one */
    }
  }
  return null
}
