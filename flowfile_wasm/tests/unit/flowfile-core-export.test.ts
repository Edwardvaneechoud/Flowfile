/**
 * The downloaded .yaml has to open in flowfile_core: a contract check that runs
 * anywhere (core's schemas are restated here, they are not importable from the
 * browser build) plus a real `open_flow` when the monorepo's Python is around.
 *
 * Whether core then produces the same rows is core-execution-parity.test.ts.
 */

import { describe, it, expect, beforeAll } from 'vitest'
import { setActivePinia, createPinia } from 'pinia'
import { execFileSync } from 'node:child_process'
import { mkdtempSync, readFileSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join, resolve } from 'node:path'
import yaml from 'js-yaml'
import { useFlowStore } from '../../src/stores/flow-store'
import { toCoreCompatibleFlow } from '../../src/utils/coreExport'
import { findPython } from '../helpers/python-runtime'
import type { FlowfileData } from '../../src/types'

const FIXTURE = resolve(__dirname, '../fixtures/wasm-export.yaml')

/** Every node type this editor can place, including the palette's own spellings. */
const EDITOR_NODE_TYPES = [
  'read',
  'manual_input',
  'external_data',
  'read_from_catalog',
  'filter',
  'select',
  'sort',
  'group_by',
  'unique',
  'formula',
  'record_id',
  'dynamic_rename',
  'head',
  'polars_code',
  'pivot',
  'unpivot',
  'join',
  'cross_join',
  'union',
  'explore_data',
  'output',
  'external_output',
  'write_to_catalog'
]

/** Node types in flowfile_core's NODE_TYPE_TO_SETTINGS_CLASS (schemas/schemas.py). */
const CORE_NODE_TYPES = new Set([
  'manual_input', 'filter', 'formula', 'dynamic_rename', 'select', 'sort', 'record_id', 'sample',
  'random_split', 'unique', 'group_by', 'window_functions', 'pivot', 'unpivot', 'text_to_rows',
  'graph_solver', 'python_script', 'polars_code', 'sql_query', 'join', 'cross_join', 'fuzzy_match',
  'record_count', 'explore_data', 'union', 'gate', 'output', 'api_response', 'read',
  'database_reader', 'database_writer', 'cloud_storage_reader', 'cloud_storage_writer',
  'catalog_reader', 'catalog_writer', 'kafka_source', 'google_analytics_reader', 'rest_api_reader',
  'external_source', 'promise', 'user_defined', 'train_model', 'apply_model', 'evaluate_model',
  'wait_for', 'flow_input', 'flow_output', 'run_flow'
])

function loadFixture(): FlowfileData {
  return yaml.load(readFileSync(FIXTURE, 'utf-8')) as FlowfileData
}

/** The fixture opened in the editor and exported again, the way the download does. */
function reExported(): FlowfileData {
  setActivePinia(createPinia())
  const store = useFlowStore()
  expect(store.importFromFlowfile(loadFixture())).toBe(true)
  return toCoreCompatibleFlow(store.exportToFlowfile('Untitled Flow 2'))
}

function everyNodeTypeExported(): FlowfileData {
  setActivePinia(createPinia())
  const store = useFlowStore()
  for (const type of EDITOR_NODE_TYPES) store.addNode(type, 0, 0)
  return toCoreCompatibleFlow(store.exportToFlowfile('Every Node Type'))
}

function nodeOfType(data: FlowfileData, type: string) {
  const node = data.nodes.find(n => n.type === type)
  expect(node, `no ${type} node in the export`).toBeTruthy()
  return node!
}

/** A manual input holding one row of the given columns. */
function manualInput(columns: string[]) {
  return {
    raw_data_format: {
      columns: columns.map(name => ({ name, data_type: 'String' })),
      data: columns.map(name => [name === 'id' ? '1' : `${name} value`])
    }
  }
}

/** Two sources with a colliding `name`; `rightSource: 'read'` hides the right schema. */
function suffixedJoin(options: {
  suffix: string
  type?: 'join' | 'cross_join'
  how?: string
  rightSource?: 'manual_input' | 'read'
}): { flow: FlowfileData; terminalId: number } {
  const type = options.type ?? 'join'
  const rightSource = options.rightSource ?? 'manual_input'

  setActivePinia(createPinia())
  const store = useFlowStore()
  const left = store.addNode('manual_input', 0, 0)
  Object.assign(store.getNode(left)!.settings, manualInput(['id', 'name']))
  const right = store.addNode(rightSource, 0, 0)
  if (rightSource === 'manual_input') {
    Object.assign(store.getNode(right)!.settings, manualInput(['id', 'name']))
  }

  const combined = store.addNode(type, 0, 0)
  const settings = store.getNode(combined)!.settings as any
  if (type === 'join') {
    const how = options.how ?? 'left'
    settings.join_input = {
      join_type: how,
      how,
      join_mapping: [{ left_col: 'id', right_col: 'id' }],
      left_suffix: '',
      right_suffix: options.suffix
    }
  } else {
    settings.cross_join_input = { right_suffix: options.suffix }
  }

  const node = store.getNode(combined)!
  node.inputIds = [left]
  node.rightInputId = right
  for (const [source, handle] of [
    [left, 'input-0'],
    [right, 'input-1']
  ] as const) {
    store.addEdge({
      id: `e${source}-${combined}`,
      source: String(source),
      target: String(combined),
      sourceHandle: 'output-0',
      targetHandle: handle
    })
  }

  return { flow: toCoreCompatibleFlow(store.exportToFlowfile('Suffixed join')), terminalId: combined }
}

const rightSelectOf = (flow: FlowfileData, type: string) =>
  nodeOfType(flow, type).setting_input[type === 'join' ? 'join_input' : 'cross_join_input'].right_select

describe('the downloaded flow file speaks flowfile_core', () => {
  it('only uses node types core knows', () => {
    for (const data of [reExported(), everyNodeTypeExported()]) {
      for (const node of data.nodes) {
        expect(CORE_NODE_TYPES, `node type ${node.type}`).toContain(node.type)
      }
    }
  })

  it('gives joins the selections core requires', () => {
    const data = everyNodeTypeExported()
    const join = nodeOfType(data, 'join').setting_input.join_input
    expect(join.left_select).toEqual({ select: [] })
    expect(join.right_select).toEqual({ select: [] })
    expect(join.how).toBe('inner')

    const crossJoin = nodeOfType(data, 'cross_join').setting_input.cross_join_input
    expect(crossJoin.left_select).toEqual({ select: [] })
    expect(crossJoin.right_select).toEqual({ select: [] })
  })

  it('spells a custom join suffix out as renames core can act on', () => {
    // core has no suffix field, so the colliding column is named explicitly.
    const { flow } = suffixedJoin({ suffix: '_from_right' })
    expect(rightSelectOf(flow, 'join')).toEqual({
      select: [
        { old_name: 'id', new_name: 'id', keep: false },
        { old_name: 'name', new_name: 'name_from_right', keep: true }
      ]
    })
  })

  it('leaves the default join suffix to core', () => {
    // _right is what core does anyway: the ordinary export is untouched.
    const { flow } = suffixedJoin({ suffix: '_right' })
    expect(rightSelectOf(flow, 'join')).toEqual({
      select: [{ old_name: 'id', new_name: 'id', keep: false }]
    })
  })

  it('does not guess the suffixed columns when the schema is unknowable', () => {
    // A CSV read only says what its columns are once it has run. Rather than
    // invent a collision, fall back to the _right core picks by itself.
    const { flow } = suffixedJoin({ suffix: '_from_right', rightSource: 'read' })
    expect(rightSelectOf(flow, 'join')).toEqual({
      select: [{ old_name: 'id', new_name: 'id', keep: false }]
    })
  })

  it('suffixes nothing for semi and anti joins, which keep no right column', () => {
    for (const how of ['semi', 'anti']) {
      const { flow } = suffixedJoin({ suffix: '_from_right', how })
      expect(rightSelectOf(flow, 'join'), how).toEqual({
        select: [{ old_name: 'id', new_name: 'id', keep: false }]
      })
    }
  })

  it('suffixes both keys of a full join, which drops neither', () => {
    const { flow } = suffixedJoin({ suffix: '_from_right', how: 'full' })
    expect(rightSelectOf(flow, 'join')).toEqual({
      select: [
        { old_name: 'id', new_name: 'id_from_right', keep: true },
        { old_name: 'name', new_name: 'name_from_right', keep: true }
      ]
    })
  })

  it('spells a custom cross join suffix out too', () => {
    // Unlike the join panel, the cross join panel does offer the field.
    const { flow } = suffixedJoin({ suffix: '_from_right', type: 'cross_join' })
    expect(rightSelectOf(flow, 'cross_join')).toEqual({
      select: [
        { old_name: 'id', new_name: 'id_from_right', keep: true },
        { old_name: 'name', new_name: 'name_from_right', keep: true }
      ]
    })
  })

  it('carries the join strategy core reads', () => {
    const join = nodeOfType(reExported(), 'join').setting_input.join_input
    expect(join.how).toBe('inner')
    expect(join.join_mapping).toEqual([{ left_col: 'record_id', right_col: 'record_id' }])
  })

  it('restates the settings panel unique as columns/strategy', () => {
    const unique = nodeOfType(reExported(), 'unique').setting_input.unique_input
    // The panel writes subset/keep; core only looks at columns/strategy.
    expect(unique.columns).toEqual(['e_contracts', 'row_labels'])
    expect(unique.strategy).toBe('last')
    expect(unique.subset).toEqual(['e_contracts', 'row_labels'])
    expect(unique.keep).toBe('last')
  })

  it('exports a unique with no chosen columns as null, not an empty list', () => {
    // core reads `columns is None` as "every column"; [] would reach
    // DataFrame.unique with an empty subset instead.
    const unique = nodeOfType(everyNodeTypeExported(), 'unique').setting_input.unique_input
    expect(unique.columns).toBeNull()
  })

  it('exports Take Sample as core sample with a row count', () => {
    const sample = nodeOfType(reExported(), 'sample').setting_input
    expect(sample.sample_method).toBe('random_fraction')
    expect(sample.sample_size).toBe(10)
    expect(sample.fraction).toBe(10)
    expect(sample.seed).toBe(42)
  })

  it('exports a sample whose count only lives in head_input', () => {
    setActivePinia(createPinia())
    const store = useFlowStore()
    const id = store.addNode('head', 0, 0)
    const settings = store.getNode(id)!.settings as any
    delete settings.sample_size
    settings.head_input = { n: 3 }
    expect(nodeOfType(toCoreCompatibleFlow(store.exportToFlowfile('S')), 'sample').setting_input.sample_size).toBe(3)
  })

  it('narrows the union mode to core enum', () => {
    const union = nodeOfType(everyNodeTypeExported(), 'union').setting_input.union_input
    expect(union.mode).toBe('relaxed')
  })

  it('maps the host and catalog nodes onto their core counterparts', () => {
    const data = everyNodeTypeExported()
    expect(nodeOfType(data, 'flow_input').setting_input.raw_data_format).toEqual({ columns: [], data: [] })
    expect(nodeOfType(data, 'flow_output').setting_input.output_name).toBe('result')
    expect(nodeOfType(data, 'catalog_reader').setting_input).toHaveProperty('catalog_table_name')
    expect(nodeOfType(data, 'catalog_writer').setting_input.catalog_write_settings).toHaveProperty('table_name')
  })

  it('leaves the in-browser export in the editor vocabulary', () => {
    // Library entries, share links and tab snapshots go through
    // exportToFlowfile directly — a lossy rewrite there would change the flow
    // every time it is saved.
    setActivePinia(createPinia())
    const store = useFlowStore()
    for (const type of EDITOR_NODE_TYPES) store.addNode(type, 0, 0)
    expect(store.exportToFlowfile('Every Node Type').nodes.map(n => n.type)).toEqual(EDITOR_NODE_TYPES)
  })

  it('reads its own core-shaped export back as the editor node types', () => {
    const exported = everyNodeTypeExported()
    setActivePinia(createPinia())
    const store = useFlowStore()
    expect(store.importFromFlowfile(exported)).toBe(true)
    const types = [...store.nodes.values()].map(n => n.type)
    expect(types).toEqual(EDITOR_NODE_TYPES)
  })
})

describe('flowfile_core opens the downloaded flow file', () => {
  // Importing flowfile_core runs its migrations, which is far past vitest's
  // 5s default.
  const TIMEOUT = 120_000

  let python: string | null = null
  let workdir = ''
  let env: NodeJS.ProcessEnv = process.env

  beforeAll(() => {
    workdir = mkdtempSync(join(tmpdir(), 'flowfile-core-import-'))
    // Importing flowfile_core creates a catalog DB and runs its migrations, so
    // point it at a throwaway storage dir before the probe, not after.
    env = { ...process.env, FLOWFILE_STORAGE_DIR: join(workdir, 'storage'), FLOWFILE_MODE: 'package' }
    python = findPython({ probe: 'import flowfile_core.flowfile.manage.io_flowfile', env })
  }, TIMEOUT)


  function openInCore(data: FlowfileData, label: string): string {
    const driver = `
import json, sys
from pathlib import Path
from flowfile_core.flowfile.manage.io_flowfile import open_flow

graph = open_flow(Path(sys.argv[1]))
print("@@@" + json.dumps(sorted(n.node_type for n in graph.nodes)))
`
    return runDriver(driver, data, label, [])
  }

  function runDriver(driver: string, data: FlowfileData, label: string, args: string[]): string {
    const path = join(workdir, `${label}.yaml`)
    writeFileSync(path, yaml.dump(JSON.parse(JSON.stringify(data))))
    const script = join(workdir, `${label}.py`)
    writeFileSync(script, driver)
    const out = execFileSync(python!, [script, path, ...args], { encoding: 'utf-8', env, timeout: 300_000 })
    const marker = out.lastIndexOf('@@@')
    if (marker === -1) throw new Error(`${label}: driver printed no result\n${out}`)
    // core keeps logging after the marker line, so take just that line.
    return out.slice(marker + 3).split('\n')[0]
  }

  it('opens the reported flow', ctx => {
    // Skipped, never silently passed: without flowfile_core nothing was proven.
    if (!python) ctx.skip('no Python that can import flowfile_core (set FLOWFILE_TEST_PYTHON)')
    const types = JSON.parse(openInCore(reExported(), 'reported')) as string[]
    expect(types).toContain('join')
    expect(types).toContain('sample')
    expect(types).toHaveLength(12)
  }, TIMEOUT)

  it('opens a flow using every node type', ctx => {
    if (!python) ctx.skip('no Python that can import flowfile_core (set FLOWFILE_TEST_PYTHON)')
    const types = JSON.parse(openInCore(everyNodeTypeExported(), 'every_type')) as string[]
    expect(types).toHaveLength(EDITOR_NODE_TYPES.length)
  }, TIMEOUT)

  const RUN_DRIVER = `
import json, sys
from pathlib import Path
from flowfile_core.flowfile.manage.io_flowfile import open_flow

graph = open_flow(Path(sys.argv[1]))
graph.flow_settings.execution_location = "local"
info = graph.run_graph()
if not info.success:
    raise SystemExit("; ".join(f"node {r.node_id}: {r.error}" for r in info.node_step_result if not r.success))
rows = graph.get_node(int(sys.argv[2])).get_resulting_data().collect().to_dicts()
print("@@@" + json.dumps(rows, default=str))
`

  /** The columns core ends up with, which is the only proof the renames worked. */
  function columnsFromCore(
    combined: { flow: FlowfileData; terminalId: number },
    label: string
  ): string[] {
    const rows = JSON.parse(
      runDriver(RUN_DRIVER, combined.flow, label, [String(combined.terminalId)])
    ) as Array<Record<string, unknown>>
    expect(rows.length, 'core produced no rows').toBeGreaterThan(0)
    return Object.keys(rows[0])
  }

  it('makes core name the colliding column the way the browser engine did', ctx => {
    if (!python) ctx.skip('no Python that can import flowfile_core (set FLOWFILE_TEST_PYTHON)')
    // Left join of two frames sharing id + name: polars would call the right
    // one name_from_right, and so must core, from the renames alone.
    const columns = columnsFromCore(suffixedJoin({ suffix: '_from_right' }), 'suffix_join')
    expect(columns).toEqual(['id', 'name', 'name_from_right'])
  }, TIMEOUT)

  it('makes core honour a custom cross join suffix', ctx => {
    if (!python) ctx.skip('no Python that can import flowfile_core (set FLOWFILE_TEST_PYTHON)')
    const combined = suffixedJoin({ suffix: '_from_right', type: 'cross_join' })
    expect(columnsFromCore(combined, 'suffix_cross_join')).toEqual([
      'id',
      'name',
      'id_from_right',
      'name_from_right'
    ])
  }, TIMEOUT)

  it('still lets core pick _right when nothing else was asked for', ctx => {
    if (!python) ctx.skip('no Python that can import flowfile_core (set FLOWFILE_TEST_PYTHON)')
    const columns = columnsFromCore(suffixedJoin({ suffix: '_right' }), 'default_suffix_join')
    expect(columns).toEqual(['id', 'name', 'name_right'])
  }, TIMEOUT)


})
