/**
 * The flow file in flowfile_core's dialect, for the download path only.
 *
 * core rejects the whole flow on an unknown node type or a missing required
 * field, so the rewrite has to be complete. It stays a pure function of the
 * file — schemas included — so downloading twice gives the same bytes.
 */

import type { FlowfileData, FlowfileNode } from '../types'
import { columnNames, inferFlowSchemas } from './flowSchemas'

/** Browser node type -> the flowfile_core type that means the same thing. */
const TYPE_TO_CORE: Record<string, string> = {
  head: 'sample',
  external_data: 'flow_input',
  external_output: 'flow_output',
  read_from_catalog: 'catalog_reader',
  write_to_catalog: 'catalog_writer'
}

/** The inverse, plus the legacy core spellings this editor renamed. */
const TYPE_FROM_CORE: Record<string, string> = {
  sample: 'head',
  flow_input: 'external_data',
  flow_output: 'external_output',
  catalog_reader: 'read_from_catalog',
  catalog_writer: 'write_to_catalog',
  read_csv: 'read',
  preview: 'explore_data',
  rename: 'dynamic_rename'
}

function clone<T>(value: T): T {
  return JSON.parse(JSON.stringify(value ?? null))
}

/** The suffix core appends by itself; the only one the join panel writes. */
const CORE_RIGHT_SUFFIX = '_right'

/** One renamed right-hand column, as core's SelectInput spells it. */
interface Rename {
  old_name: string
  new_name: string
}

/** Listing nothing is normal: core auto-keeps the rest. Name only the exceptions. */
function joinSelect(droppedKeys: string[] = [], renames: Rename[] = []) {
  return {
    select: [
      ...droppedKeys.map(old_name => ({ old_name, new_name: old_name, keep: false })),
      ...renames.map(rename => ({ ...rename, keep: true }))
    ]
  }
}

/** Which side's join key polars removes from the output, for a given strategy. */
function droppedKeySide(how: string): 'left' | 'right' | null {
  if (how === 'right') return 'left'
  if (how === 'full' || how === 'outer' || how === 'cross') return null
  return 'right'
}

/** The suffix a join carries only if it is one core would not have picked. */
function customSuffix(value: unknown): string | null {
  return typeof value === 'string' && value !== '' && value !== CORE_RIGHT_SUFFIX ? value : null
}

/** core has no suffix field, so a non-default suffix becomes explicit renames. */
function suffixedRightColumns(
  survivingLeft: string[] | null,
  survivingRight: string[] | null,
  suffix: string
): Rename[] {
  if (!survivingLeft || !survivingRight) return []
  const left = new Set(survivingLeft)
  return survivingRight.filter(name => left.has(name)).map(name => ({ old_name: name, new_name: name + suffix }))
}

/** The columns of one side that survive the join — the join key can be dropped. */
function survivors(columns: string[] | null, droppedKeys: string[]): string[] | null {
  if (!columns) return null
  const dropped = new Set(droppedKeys)
  return columns.filter(name => !dropped.has(name))
}

/** What the flow file says reaches a node, where it says anything at all. */
interface Inputs {
  left: string[] | null
  right: string[] | null
}

function toCoreSettings(type: string, settings: any, inputs: Inputs): any {
  if (!settings) return settings
  const s = clone(settings)

  switch (type) {
    case 'cross_join': {
      const input = s.cross_join_input || (s.cross_join_input = {})
      const suffix = customSuffix(input.right_suffix)
      const renames = suffix ? suffixedRightColumns(inputs.left, inputs.right, suffix) : []
      if (!input.left_select) input.left_select = joinSelect()
      if (!input.right_select) input.right_select = joinSelect([], renames)
      return s
    }
    case 'join': {
      const input = s.join_input || (s.join_input = {})
      const how = input.join_type || input.how || 'inner'
      input.how = how
      const mapping: Array<{ left_col?: string; right_col?: string }> = input.join_mapping || []
      const side = droppedKeySide(how)
      const dropped =
        side === null
          ? []
          : [
              ...new Set(
                mapping
                  .map(m => (side === 'left' ? m.left_col : (m.right_col ?? m.left_col)))
                  .filter((name): name is string => !!name)
              )
            ]
      const suffix = customSuffix(input.right_suffix)
      // semi and anti keep no right-hand column at all, so nothing can collide.
      const renames =
        suffix && how !== 'semi' && how !== 'anti'
          ? suffixedRightColumns(
              survivors(inputs.left, side === 'left' ? dropped : []),
              survivors(inputs.right, side === 'right' ? dropped : []),
              suffix
            )
          : []
      if (!input.left_select) input.left_select = joinSelect(side === 'left' ? dropped : [])
      if (!input.right_select) input.right_select = joinSelect(side === 'right' ? dropped : [], renames)
      return s
    }
    case 'unique': {
      // core means "every column" by null, not by [].
      const u = s.unique_input || (s.unique_input = {})
      const subset = u.subset?.length ? u.subset : u.columns
      u.columns = subset?.length ? subset : null
      u.strategy = u.keep || u.strategy || 'first'
      return s
    }
    case 'union': {
      // core only knows selective/relaxed, and concatenates diagonally either way.
      s.union_input = { ...(s.union_input || {}), mode: 'relaxed' }
      return s
    }
    case 'head':
    case 'sample': {
      s.sample_size = s.sample_size ?? s.head_input?.n ?? 10
      return s
    }
    case 'select': {
      // core keeps unmentioned columns by default; build_select drops them.
      s.keep_missing = false
      return s
    }
    case 'record_id': {
      // core's RecordIdInput calls the new column output_column_name.
      const r = s.record_id_input || (s.record_id_input = {})
      r.output_column_name = r.name || r.output_column_name || 'record_id'
      return s
    }
    case 'external_data': {
      const columns = (s.schema_snapshot || []).map((col: any) => ({
        name: col.name,
        data_type: col.data_type
      }))
      return {
        ...s,
        input_name: s.dataset_name || 'input',
        raw_data_format: { columns, data: columns.map(() => []) }
      }
    }
    case 'external_output':
      return { ...s, output_name: s.output_name || 'output' }
    case 'read_from_catalog':
      return { ...s, catalog_table_name: s.dataset_name || '' }
    case 'write_to_catalog':
      return { ...s, catalog_write_settings: { table_name: s.dataset_name || '' } }
    default:
      return s
  }
}

function fromCoreSettings(coreType: string, settings: any): any {
  if (!settings) return settings
  switch (coreType) {
    case 'record_id': {
      const r = settings.record_id_input
      if (!r || r.name) return settings
      return { ...settings, record_id_input: { ...r, name: r.output_column_name || 'record_id' } }
    }
    case 'flow_input':
      return { ...settings, dataset_name: settings.dataset_name || settings.input_name || '' }
    case 'flow_output':
      return { ...settings, output_name: settings.output_name || 'output' }
    case 'catalog_reader':
      return { ...settings, dataset_name: settings.dataset_name || settings.catalog_table_name || '' }
    case 'catalog_writer':
      return {
        ...settings,
        dataset_name: settings.dataset_name || settings.catalog_write_settings?.table_name || ''
      }
    default:
      return settings
  }
}

/** Rewrite a flow into the node types and setting shapes core's importer accepts. */
export function toCoreCompatibleFlow(data: FlowfileData): FlowfileData {
  const schemas = inferFlowSchemas(data.nodes)
  const columnsOf = (id: number | null | undefined): string[] | null =>
    id === null || id === undefined ? null : columnNames(schemas.get(id))
  const inputsOf = (node: FlowfileNode): Inputs => ({
    left: columnsOf(node.left_input_id ?? node.input_ids?.[0]),
    right: columnsOf(node.right_input_id)
  })

  return {
    ...data,
    nodes: data.nodes.map(
      (node): FlowfileNode => ({
        ...node,
        type: TYPE_TO_CORE[node.type] || node.type,
        setting_input: toCoreSettings(node.type, node.setting_input, inputsOf(node))
      })
    )
  }
}

/** The type this editor uses for a node saved under `type` (its own or core's). */
export function editorNodeType(type: string): string {
  return TYPE_FROM_CORE[type] || type
}

/** Undo the export rewrite for one node's settings. */
export function editorNodeSettings(type: string, settings: any): any {
  return fromCoreSettings(type, settings)
}
