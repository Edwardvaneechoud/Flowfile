/**
 * End-to-end: a flow built in the real store, exported through the real download
 * path, then opened and run by real flowfile_core must produce the rows the
 * browser engine produced.
 *
 * All flows run in ONE Python process — importing flowfile_core costs seconds.
 * Row order is not compared; neither engine promises the other's. Skips cleanly
 * without a Python that can import flowfile_core.
 */

import { describe, it, expect, beforeAll } from 'vitest'
import { setActivePinia, createPinia } from 'pinia'
import { execFileSync } from 'node:child_process'
import { mkdtempSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join, resolve } from 'node:path'
import yaml from 'js-yaml'
import { useFlowStore } from '../../src/stores/flow-store'
import { toCoreCompatibleFlow } from '../../src/utils/coreExport'
import { findPython } from '../helpers/python-runtime'
import { CORE_ONLY_FIXTURES, PARITY_FIXTURES, engineRows, normalise } from '../helpers/parity'
import type { Fixture } from '../helpers/parity'

/** Every flow core is asked to open and run; see CORE_ONLY_FIXTURES for the tail. */
const FIXTURES: Fixture[] = [...PARITY_FIXTURES, ...CORE_ONLY_FIXTURES]

/** A `bothRefuse` marker as the two sides it always means, whichever form it took. */
function refusalPhrases(marker: NonNullable<Fixture['bothRefuse']>): { core: string; engine: string } {
  return typeof marker === 'string' ? { core: marker, engine: marker } : marker
}

// Importing flowfile_core runs its migrations, and then 49 flows execute.
const TIMEOUT = 900_000

interface CoreResult {
  opened: boolean
  ran: boolean
  error?: string
  rows?: unknown[]
  /** The browser engine's own refusal, filled in only for `bothRefuse` fixtures. */
  engineError?: string | null
}

let python: string | null = null
let workdir = ''
let env: NodeJS.ProcessEnv = process.env
let coreResults: Record<string, CoreResult> = {}

/**
 * Build the fixture in the real store and export it the way the download does.
 * Settings *replace* the node defaults: a leftover default would change the flow.
 */
function buildInStore(fixture: Fixture): { flow: unknown; terminalId: number } {
  setActivePinia(createPinia())
  const store = useFlowStore()
  const ids = new Map<number, number>()

  for (const step of fixture.steps) {
    const id = store.addNode(step.type, 0, 0)
    ids.set(step.id, id)
    const node = store.getNode(id)!
    const { node_id, is_setup, cache_results, pos_x, pos_y, description } = node.settings as any
    node.settings = {
      node_id,
      is_setup,
      cache_results,
      pos_x,
      pos_y,
      description,
      ...(step.settings ?? {})
    } as any
  }

  for (const step of fixture.steps) {
    const target = ids.get(step.id)!
    const node = store.getNode(target)!
    const link = (from: number, handle: string) =>
      store.addEdge({
        id: `e${from}-${target}-${handle}`,
        source: String(from),
        target: String(target),
        sourceHandle: 'output-0',
        targetHandle: handle
      })

    if (step.left !== undefined) {
      node.inputIds = [ids.get(step.left)!]
      node.rightInputId = ids.get(step.right!)!
      link(ids.get(step.left)!, 'input-0')
      link(ids.get(step.right!)!, 'input-1')
    } else if (step.inputs?.length) {
      node.inputIds = step.inputs.map(id => ids.get(id)!)
      for (const input of step.inputs) link(ids.get(input)!, 'input-0')
    }
  }

  return { flow: toCoreCompatibleFlow(store.exportToFlowfile(fixture.name)), terminalId: ids.get(fixture.output)! }
}

const DRIVER = `
import json, sys
from pathlib import Path

sys.path.insert(0, sys.argv[2])
import engine
from flowfile_core.flowfile.manage.io_flowfile import open_flow


def engine_refusal(steps):
    """The browser engine's verdict: its error, or None if it ran."""
    engine.clear_all()
    for step in steps:
        node_id = int(step["id"])
        settings = step.get("settings", {})
        if step["type"] == "manual_input":
            result = engine.execute_manual_input(node_id, "", settings)
        elif step["type"] == "pivot":
            result = engine.execute_pivot(node_id, int(step["inputs"][0]), settings)
        else:
            raise ValueError(f"the refusal probe has no case for node type {step['type']!r}")
        if not result["success"]:
            return result["error"]
    return None


jobs = json.load(open(sys.argv[1]))
results = {}
for job in jobs:
    try:
        graph = open_flow(Path(job["path"]))
    except Exception as e:
        results[job["label"]] = {"opened": False, "ran": False, "error": f"{type(e).__name__}: {e}"}
        continue
    graph.flow_settings.execution_location = "local"
    try:
        info = graph.run_graph()
        if not info.success:
            failures = "; ".join(f"node {r.node_id}: {r.error}" for r in info.node_step_result if not r.success)
            results[job["label"]] = {"opened": True, "ran": False, "error": failures}
            continue
        rows = graph.get_node(int(job["node"])).get_resulting_data().collect().to_dicts()
        results[job["label"]] = {"opened": True, "ran": True, "rows": rows}
    except Exception as e:
        results[job["label"]] = {"opened": True, "ran": False, "error": f"{type(e).__name__}: {e}"}

for job in jobs:
    if job.get("engineSteps"):
        results[job["label"]]["engineError"] = engine_refusal(job["engineSteps"])
print("@@@" + json.dumps(results, default=str))
`

beforeAll(() => {
  workdir = mkdtempSync(join(tmpdir(), 'flowfile-core-run-'))
  // Importing flowfile_core migrates a catalog DB — isolate it before the probe.
  env = { ...process.env, FLOWFILE_STORAGE_DIR: join(workdir, 'storage'), FLOWFILE_MODE: 'package' }
  python = findPython({ probe: 'import flowfile_core.flowfile.manage.io_flowfile', env })
  if (!python) return

  const jobs = FIXTURES.map((fixture, index) => {
    const { flow, terminalId } = buildInStore(fixture)
    const path = join(workdir, `flow_${index}.yaml`)
    writeFileSync(path, yaml.dump(JSON.parse(JSON.stringify(flow))))
    // A flow neither engine runs is asked of both.
    const engineSteps = fixture.bothRefuse ? fixture.steps : undefined
    return { label: fixture.name, path, node: terminalId, engineSteps }
  })
  const jobsPath = join(workdir, 'jobs.json')
  writeFileSync(jobsPath, JSON.stringify(jobs))
  const script = join(workdir, 'driver.py')
  writeFileSync(script, DRIVER)

  const out = execFileSync(python, [script, jobsPath, resolve(__dirname, '../../src/pyodide')], {
    encoding: 'utf-8',
    env,
    timeout: TIMEOUT,
    maxBuffer: 64 * 1024 * 1024
  })
  const marker = out.lastIndexOf('@@@')
  if (marker === -1) throw new Error(`the core driver printed no result\n${out.slice(-4000)}`)
  coreResults = JSON.parse(out.slice(marker + 3).split('\n')[0])
}, TIMEOUT)

describe('flowfile_core runs the exported flow and gets the same rows', () => {
  for (const fixture of FIXTURES) {
    it(fixture.name, ctx => {
      // Skipped, never silently passed: without core nothing was proven.
      if (!python) ctx.skip('no Python that can import flowfile_core (set FLOWFILE_TEST_PYTHON)')
      const result = coreResults[fixture.name]
      expect(result, 'the core driver returned nothing for this flow').toBeTruthy()

      // The export contract, whatever else differs.
      expect(result.opened, `core could not open the flow: ${result.error}`).toBe(true)

      if (fixture.bothRefuse) {
        // Either side accepting it, or refusing for another reason, fails here.
        const phrase = refusalPhrases(fixture.bothRefuse)
        expect(result.ran, `core now runs this flow; drop bothRefuse: ${phrase.core}`).toBe(false)
        expect(result.error, 'core still refuses, but no longer for this reason').toContain(phrase.core)
        expect(
          result.engineError,
          `the browser engine now runs this flow; drop bothRefuse: ${phrase.engine}`
        ).toBeTruthy()
        expect(result.engineError, 'the engine still refuses, but no longer for this reason').toContain(phrase.engine)
        return
      }

      const expected = normalise(engineRows(python!, fixture), false)
      const matches = result.ran && JSON.stringify(normalise(result.rows!, false)) === JSON.stringify(expected)

      if (fixture.coreDivergence) {
        // If this fails core caught up: delete the marker, don't relax this.
        expect(matches, `core now matches; drop coreDivergence: ${fixture.coreDivergence}`).toBe(false)
        return
      }

      expect(result.ran, `core failed to run the flow: ${result.error}`).toBe(true)
      expect(normalise(result.rows!, false)).toEqual(expected)
    }, 60_000)
  }
})
