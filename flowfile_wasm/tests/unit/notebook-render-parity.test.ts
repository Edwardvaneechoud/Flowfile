/**
 * The notebook the browser renders must be the notebook flowfile_core renders.
 *
 * Every fixture is built in the real store and exported the way the download
 * does. That one flow file is then rendered twice in ONE Python process: by the
 * browser engine's `notebook_render` and by `flowfile_core.notebook.render`.
 * The cells must be identical: ids, node ids, status and text. A formula corpus
 * gets the same check: the same `ff.` code, or the same refusal to translate.
 *
 * What core renders is also committed as `tests/python/notebook_golden.json`, which
 * the engine's own pytest replays on the browser's pinned versions. Refresh it
 * with `UPDATE_NOTEBOOK_GOLDEN=1` after a deliberate change to the render.
 *
 * Skips cleanly without a Python that can import flowfile_core.
 */

import { describe, it, expect, beforeAll } from 'vitest'
import { execFileSync } from 'node:child_process'
import { mkdtempSync, readFileSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join, resolve } from 'node:path'
import yaml from 'js-yaml'
import { findPython } from '../helpers/python-runtime'
import { NOTEBOOK_FORMULAS, NOTEBOOK_RENDER_FIXTURES, buildInStore } from '../helpers/notebook'

const TIMEOUT = 900_000
const GOLDEN = resolve(__dirname, '../python/notebook_golden.json')

interface Cell {
  cell_id: string
  node_ids: number[]
  kind: string
  code: string
  status: string
  reason: string | null
  defines: string[]
  uses: string[]
}
interface Rendered {
  cells?: Cell[]
  error?: string
}
type Verdicts = Record<string, string | null>

let python: string | null = null
let flows: Record<string, { core: Rendered; browser: Rendered; schemas: Record<string, unknown> }> = {}
let formulas: { core: Verdicts; browser: Verdicts } = { core: {}, browser: {} }
const exported: Record<string, unknown> = {}

const DRIVER = `
import json, sys, traceback
from pathlib import Path

sys.path.insert(0, sys.argv[2])
sys.path.insert(0, sys.argv[3])
import engine
from engine.notebook_formulas import translate_to_ff_code
from engine.notebook_render import render_notebook
from engine_flow_runner import build_frames
from flowfile_core.flowfile.code_generator import code_generator as cg
from flowfile_core.flowfile.manage.io_flowfile import open_flow
from flowfile_core.notebook.compare import strip_outer_parens
from flowfile_core.notebook.render import render


def engine_schemas(steps):
    """Each node's output columns as the browser engine resolves them, where it can."""
    engine.clear_all()
    schemas = {}
    for count in range(1, len(steps) + 1):
        try:
            frames = build_frames({"steps": steps[:count]})
        except Exception:
            break
        for node_id, frame in frames.items():
            schemas[str(node_id)] = [{"name": n, "data_type": str(d)} for n, d in frame.collect_schema().items()]
    return schemas


def attempt(render_one):
    try:
        return {"cells": render_one()}
    except Exception as exc:
        return {"error": f"{type(exc).__name__}: {exc}", "trace": traceback.format_exc()[-2000:]}


def core_formula(formula):
    """The render's own three steps: translate, respell as ff, keep only what a cell reads back."""
    code = cg._try_translate_to_ff_code(strip_outer_parens(formula))
    if not code:
        return None
    code = cg._polars_code_to_flowframe(code, modules=("ff",))
    return code if cg._interprets_without_a_kernel(code) else None


work = json.load(open(sys.argv[1]))
flows = {}
for job in work["jobs"]:
    path = Path(job["path"])
    schemas = engine_schemas(job["steps"])
    flows[job["label"]] = {
        "core": attempt(lambda: [cell.model_dump() for cell in render(open_flow(path)).cells]),
        "browser": attempt(lambda: render_notebook(job["flow"], schemas, {})["cells"]),
        "schemas": schemas,
    }
formulas = {
    "core": {formula: core_formula(formula) for formula in work["formulas"]},
    "browser": {formula: translate_to_ff_code(formula) for formula in work["formulas"]},
}
print("@@@" + json.dumps({"flows": flows, "formulas": formulas}, default=str))
`

beforeAll(() => {
  const workdir = mkdtempSync(join(tmpdir(), 'flowfile-notebook-render-'))
  // Importing flowfile_core migrates a catalog DB — isolate it before the probe.
  const env = { ...process.env, FLOWFILE_STORAGE_DIR: join(workdir, 'storage'), FLOWFILE_MODE: 'package' }
  python = findPython({ probe: 'import flowfile_core.notebook.render', env })
  if (!python) return

  const jobs = NOTEBOOK_RENDER_FIXTURES.map((fixture, index) => {
    const { flow, ids } = buildInStore(fixture)
    const path = join(workdir, `flow_${index}.yaml`)
    writeFileSync(path, yaml.dump(JSON.parse(JSON.stringify(flow))))
    // The engine keys frames by the ids the store handed out.
    const steps = fixture.steps.map(step => ({
      ...step,
      id: ids.get(step.id),
      inputs: step.inputs?.map(id => ids.get(id)),
      left: step.left === undefined ? undefined : ids.get(step.left),
      right: step.right === undefined ? undefined : ids.get(step.right)
    }))
    // The id is the time of the export; nothing renders from it.
    exported[fixture.name] = { ...JSON.parse(JSON.stringify(flow)), flowfile_id: 1 }
    return { label: fixture.name, path, flow, steps }
  })
  writeFileSync(join(workdir, 'jobs.json'), JSON.stringify({ jobs, formulas: NOTEBOOK_FORMULAS }))
  writeFileSync(join(workdir, 'driver.py'), DRIVER)

  const out = execFileSync(
    python,
    [join(workdir, 'driver.py'), join(workdir, 'jobs.json'), resolve(__dirname, '../../src/pyodide'), resolve(__dirname, '../python')],
    { encoding: 'utf-8', env, timeout: TIMEOUT, maxBuffer: 64 * 1024 * 1024 }
  )
  const marker = out.lastIndexOf('@@@')
  if (marker === -1) throw new Error(`the render driver printed no result\n${out.slice(-4000)}`)
  ;({ flows, formulas } = JSON.parse(out.slice(marker + 3).split('\n')[0]))
}, TIMEOUT)

const SKIP_REASON = 'no Python that can import flowfile_core (set FLOWFILE_TEST_PYTHON)'

describe('the browser renders the notebook flowfile_core renders', () => {
  for (const fixture of NOTEBOOK_RENDER_FIXTURES) {
    it(fixture.name, ctx => {
      // Skipped, never silently passed: without core nothing was compared.
      if (!python) ctx.skip(SKIP_REASON)
      const { core, browser } = flows[fixture.name]
      expect(core.error, 'flowfile_core could not render the flow').toBeUndefined()
      expect(browser.error, 'the browser engine could not render the flow').toBeUndefined()
      expect(browser.cells).toEqual(core.cells)
    })
  }
})

describe('the browser reads a formula the way flowfile_core does', () => {
  for (const formula of NOTEBOOK_FORMULAS) {
    it(formula, ctx => {
      if (!python) ctx.skip(SKIP_REASON)
      expect(formulas.browser[formula]).toBe(formulas.core[formula])
    })
  }
})

describe('the golden file the pinned engine tests replay', () => {
  it('is what flowfile_core renders today', ctx => {
    if (!python) ctx.skip(SKIP_REASON)
    const golden = {
      flows: NOTEBOOK_RENDER_FIXTURES.map(fixture => ({
        name: fixture.name,
        flow: exported[fixture.name],
        schemas: flows[fixture.name].schemas,
        cells: flows[fixture.name].core.cells
      })),
      formulas: formulas.core
    }
    if (process.env.UPDATE_NOTEBOOK_GOLDEN) {
      // One flow per line keeps a refresh's diff to the flows that changed.
      const lines = golden.flows.map(flow => JSON.stringify(flow)).join(',\n')
      writeFileSync(GOLDEN, `{"flows": [\n${lines}\n],\n"formulas": ${JSON.stringify(golden.formulas, null, 1)}}\n`)
      return
    }
    expect(JSON.parse(readFileSync(GOLDEN, 'utf-8')), 'refresh with UPDATE_NOTEBOOK_GOLDEN=1').toEqual(golden)
  })
})
