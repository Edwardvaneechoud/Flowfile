/**
 * The columns each node produces, from the flow file alone — never from run
 * results, so the export it feeds stays deterministic. A node only the data can
 * settle (a CSV read, a formula, a pivot) is absent, as is everything after it;
 * callers must read "not known" as "leave it to core".
 */

import { inferOutputSchema, inferSchemaFromRawData, isSourceNode } from '../stores/schema-inference'
import type { ColumnSchema, FlowfileNode, NodeSettings } from '../types'

/** A source's columns, where the flow file carries them (it often does not). */
function sourceSchema(node: FlowfileNode): ColumnSchema[] | null {
  const settings = node.setting_input || {}
  if (node.type === 'manual_input') return inferSchemaFromRawData(settings.raw_data_format?.columns || [])
  if (node.type === 'external_data') return inferSchemaFromRawData(settings.schema_snapshot || [])
  return null  // read / read_from_catalog: only the data says
}

/** Schemas by node id, holding only the nodes whose columns the file settles. */
export function inferFlowSchemas(nodes: FlowfileNode[]): Map<number, ColumnSchema[]> {
  const byId = new Map(nodes.map(node => [node.id, node]))
  const known = new Map<number, ColumnSchema[]>()
  const settled = new Set<number>()
  const resolving = new Set<number>()

  const schemaOf = (id: number | null | undefined): ColumnSchema[] | null => {
    if (id === null || id === undefined) return null
    if (settled.has(id)) return known.get(id) || null
    const node = byId.get(id)
    if (!node || resolving.has(id)) return null  // missing input, or a cycle

    resolving.add(id)
    const schema = computeSchema(node)
    resolving.delete(id)
    settled.add(id)
    if (schema?.length) known.set(id, schema)
    return schema?.length ? schema : null
  }

  const computeSchema = (node: FlowfileNode): ColumnSchema[] | null => {
    if (isSourceNode(node.type)) return sourceSchema(node)
    const input = schemaOf(node.left_input_id ?? node.input_ids?.[0])
    if (!input) return null
    const settings = (node.setting_input || {}) as NodeSettings
    return inferOutputSchema(node.type, input, settings, schemaOf(node.right_input_id))
  }

  for (const node of nodes) schemaOf(node.id)
  return known
}

/** Just the names, in order — what a select or a rename is written against. */
export function columnNames(schema: ColumnSchema[] | null | undefined): string[] | null {
  return schema?.length ? schema.map(column => column.name) : null
}
