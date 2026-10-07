/**
 * A push in the browser must change the flow the way a push in flowfile_core does.
 *
 * Each case edits one cell of a fixture's notebook. The flow is built in the real store and
 * exported the way the download does; then, in ONE Python process, the same edited cells are
 * pushed twice: by the browser engine's `sync_notebook` (its patch laid over the flow, as the
 * editor lands it) and by core's own clean run of the cells (`NotebookRunner`). Core runs both
 * resulting flows, and the rows of every end node must match, unordered. Behaviour is compared,
 * not node structure: core may lower a call to other nodes than the browser does.
 *
 * Skips cleanly without a Python that can import flowfile_core.
 */

import { describe, it, expect, beforeAll } from 'vitest'
import { execFileSync } from 'node:child_process'
import { mkdtempSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join, resolve } from 'node:path'
import { findPython } from '../helpers/python-runtime'
import { NOTEBOOK_RENDER_FIXTURES, buildInStore } from '../helpers/notebook'

const TIMEOUT = 900_000

interface PushCase {
  name: string
  fixture: string
  /** Text of the cell to change, and what it becomes. `{name}` is the name the cell binds. */
  old: string
  new: string
}

const CHAIN = 'a longer chain: filter then group_by then sort'
const EVERY_AGG = 'group_by over every aggregation'

const CASES: PushCase[] = [
  {
    name: 'group by: aggregations added',
    fixture: CHAIN,
    old: 'ff.col("revenue").sum().alias("total"),',
    new: 'ff.col("revenue").sum().alias("total"),\n        ff.col("revenue").std().alias("spread"),\n        ff.col("revenue").var().alias("variance"),'
  },
  {
    name: 'group by: a renamed key and another aggregation',
    fixture: 'group_by renaming its grouping column',
    old: 'ff.col("product").alias("item")]).agg([\n        ff.col("revenue").sum()',
    new: 'ff.col("product").alias("name")]).agg([\n        ff.col("revenue").median()'
  },
  { name: 'group by: two keys', fixture: EVERY_AGG, old: "group_by(['product'])", new: "group_by(['product', 'region'])" },
  {
    name: 'group by: a new one, with a select below it',
    fixture: CHAIN,
    old: '\n)',
    new: '\n)\nper_total = {name}.group_by("total").agg(ff.col("product").n_unique().alias("products"))\nkept = per_total.select(["products"])'
  },
  { name: 'drop', fixture: EVERY_AGG, old: '\n)', new: '\n)\nslim = {name}.drop(["lowest", "highest"])' },
  { name: 'rename', fixture: CHAIN, old: '\n)', new: '\n)\nrenamed = {name}.rename({"total": "sum_revenue"})' },
  {
    name: 'drop, rename and select in a chain',
    fixture: 'group_by renaming its grouping column',
    old: '\n)',
    new: '\n)\nshort = {name}.drop("total").rename({"item": "name"}).select(["name"])'
  },
  { name: 'sort', fixture: 'sort on mixed directions', old: 'sort(["product", "revenue"], descending=[False, True])', new: 'sort(["revenue"], descending=[False])' },
  { name: 'head', fixture: 'head takes the first rows', old: '.head(3)', new: '.head(1)' },
  { name: 'unique', fixture: 'unique keep first on a subset', old: "keep='first'", new: "keep='none'" },
  { name: 'record id', fixture: 'record_id with an offset', old: 'offset=5', new: 'offset=0' },
  { name: 'filter', fixture: 'filter between', old: '(ff.col("revenue") >= 100) & (ff.col("revenue") <= 200)', new: 'ff.col("revenue") > 120' },
  { name: 'select', fixture: 'select keeps position order and renames', old: '.alias("item")', new: '.alias("name")' },
  { name: 'unpivot', fixture: 'unpivot goes column by column', old: "on=['q1', 'q2']", new: "on=['q1']" },
  {
    name: 'a step written over with another',
    fixture: 'sort on mixed directions',
    old: 'sort(["product", "revenue"], descending=[False, True])',
    new: 'select(["product"])'
  },
  { name: 'a step taken out of a chain', fixture: CHAIN, old: '\n    .filter(ff.col("revenue").is_not_null())', new: '' }
]

interface Outcome {
  error?: string
  leaves?: string[]
}

let python: string | null = null
let results: Record<string, { core: Outcome; browser: Outcome }> = {}

const DRIVER = `
import copy, json, sys, traceback
from pathlib import Path

import yaml

sys.path.insert(0, sys.argv[2])
from engine.notebook_cells import sync_notebook
from flowfile_core.flowfile.manage.io_flowfile import open_flow
from flowfile_core.notebook.bridge import CleanRunRequest
from flowfile_core.notebook.push import seed_snapshot
from flowfile_core.notebook.render import render
from flowfile_core.notebook.runner import NotebookRunner

work = Path(sys.argv[1]).parent


def merged(base, patch):
    if not isinstance(base, dict) or not isinstance(patch, dict):
        return copy.deepcopy(patch)
    return {**base, **{key: merged(base.get(key), value) for key, value in patch.items()}}


def applied(flow, answer):
    """The flow after a browser sync, the way the editor lands it."""
    flow = copy.deepcopy(flow)
    gone = {step["id"] for step in answer["removed"]}
    flow["nodes"] = [node for node in flow["nodes"] if node["id"] not in gone]
    for node in flow["nodes"]:
        node["input_ids"] = [source for source in node.get("input_ids") or [] if source not in gone]
        if node.get("right_input_id") in gone:
            node["right_input_id"] = None
    for new in answer["added"]:
        flow["nodes"].append({
            "id": new["id"], "type": new["type"], "is_start_node": False, "description": new["description"],
            "node_reference": new["node_reference"], "x_position": 0, "y_position": 0, "input_ids": [],
            "outputs": [], "setting_input": {"node_id": new["id"], "is_setup": True, **new["settings"]},
        })
    by_id = {node["id"]: node for node in flow["nodes"]}
    for node_id, change in answer["nodes"].items():
        target = by_id[int(node_id)]
        if "settings" in change:
            target["setting_input"] = merged(target["setting_input"], change["settings"])
        for key in ("description", "node_reference"):
            if key in change:
                target[key] = change[key]
    for node_id, ports in answer["inputs"].items():
        by_id[int(node_id)]["input_ids"] = ports["main"]
        by_id[int(node_id)]["right_input_id"] = ports["right"]
    for node in flow["nodes"]:
        node["outputs"] = [
            other["id"] for other in flow["nodes"]
            if node["id"] in (other.get("input_ids") or []) or node["id"] == other.get("right_input_id")
        ]
    return flow


def run(flow, name):
    """Core runs the flow; the rows of each node nothing reads, each table unordered."""
    path = work / f"{name}.yaml"
    path.write_text(yaml.dump(flow))
    graph = open_flow(path)
    graph.flow_settings.execution_location = "local"
    info = graph.run_graph()
    if not info.success:
        return graph, {"error": "; ".join(f"node {r.node_id}: {r.error}" for r in info.node_step_result if not r.success)}
    read = {source.node_id for node in graph.nodes for source in node.all_inputs}
    tables = [
        json.dumps(sorted(json.dumps(row, default=str, sort_keys=True) for row in node.get_resulting_data().collect().to_dicts()))
        for node in graph.nodes
        if node.node_id not in read
    ]
    return graph, {"leaves": sorted(tables)}


def attempt(push):
    try:
        return push()
    except Exception as exc:
        return {"error": f"{type(exc).__name__}: {exc}", "trace": traceback.format_exc()[-2000:]}


results = {}
for job in json.load(open(sys.argv[1])):
    # Both pushes start from a flow that has run, so each side knows the columns the canvas shows.
    graph, _ = run(job["flow"], f"{job['label']}.before")
    rendering = render(graph)
    cells = {cell.cell_id: cell.code for cell in rendering.cells}
    cell = next(cell for cell in rendering.cells if job["old"] in cell.code)
    draft = cell.code.replace(job["old"], job["new"].replace("{name}", (cell.defines or ["_"])[0]), 1)
    schemas = {
        str(node.node_id): [{"name": column.column_name, "data_type": column.data_type} for column in node.schema]
        for node in graph.nodes
    }
    next_id = max(node.node_id for node in graph.nodes) + 1

    def browser():
        answer = sync_notebook(copy.deepcopy(job["flow"]), schemas, {}, {cell.cell_id: draft}, next_id)
        if not answer["ok"]:
            return {"error": f"{answer['kind']}: {answer['message']} (line {answer['line']})"}
        return run(applied(job["flow"], answer), f"{job['label']}.browser")[1]

    def core():
        provenance = {
            each.cell_id: [(graph.get_node(node_id).node_type, node_id) for node_id in each.node_ids]
            for each in rendering.cells
            if each.node_ids
        }
        request = CleanRunRequest(
            cells=list({**cells, cell.cell_id: draft}.items()),
            provenance=provenance,
            ceiling=next_id - 1,
            snapshot=seed_snapshot(graph),
        )
        result = NotebookRunner().clean_run(1, graph.flow_id, request)
        if result.error is not None:
            return {"error": f"{result.kind}: {result.error} (line {result.line})"}
        return run(result.flowfile_data, f"{job['label']}.core")[1]

    results[job["label"]] = {"browser": attempt(browser), "core": attempt(core)}
print("@@@" + json.dumps(results, default=str))
`

beforeAll(() => {
  const workdir = mkdtempSync(join(tmpdir(), 'flowfile-notebook-push-'))
  // Importing flowfile_core migrates a catalog DB — isolate it before the probe.
  const env = { ...process.env, FLOWFILE_STORAGE_DIR: join(workdir, 'storage'), FLOWFILE_MODE: 'package' }
  python = findPython({ probe: 'import flowfile_core.notebook.runner', env })
  if (!python) return

  const fixtures = new Map(NOTEBOOK_RENDER_FIXTURES.map(fixture => [fixture.name, fixture]))
  const jobs = CASES.map((each, index) => {
    const fixture = fixtures.get(each.fixture)
    if (!fixture) throw new Error(`no notebook fixture named ${each.fixture}`)
    const { flow } = buildInStore(fixture)
    return { label: `case_${index}`, flow: JSON.parse(JSON.stringify(flow)), old: each.old, new: each.new }
  })
  const jobsPath = join(workdir, 'jobs.json')
  writeFileSync(jobsPath, JSON.stringify(jobs))
  writeFileSync(join(workdir, 'driver.py'), DRIVER)

  const out = execFileSync(python, [join(workdir, 'driver.py'), jobsPath, resolve(__dirname, '../../src/pyodide')], {
    encoding: 'utf-8',
    env,
    timeout: TIMEOUT,
    maxBuffer: 64 * 1024 * 1024
  })
  const marker = out.lastIndexOf('@@@')
  if (marker === -1) throw new Error(`the push driver printed no result\n${out.slice(-4000)}`)
  const byLabel = JSON.parse(out.slice(marker + 3).split('\n')[0])
  results = Object.fromEntries(CASES.map((each, index) => [each.name, byLabel[`case_${index}`]]))
}, TIMEOUT)

describe('a push in the browser changes the flow the way flowfile_core does', () => {
  for (const each of CASES) {
    it(each.name, ctx => {
      // Skipped, never silently passed: without core nothing was compared.
      if (!python) ctx.skip('no Python that can import flowfile_core (set FLOWFILE_TEST_PYTHON)')
      const { core, browser } = results[each.name]
      expect(core.error, 'flowfile_core could not push or run the edit').toBeUndefined()
      expect(browser.error, 'the browser engine could not push or run the edit').toBeUndefined()
      expect(browser.leaves).toEqual(core.leaves)
    })
  }
})
