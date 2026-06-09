/**
 * Built-in templates validation.
 *
 * Without booting Pyodide we can still catch the most likely authoring bugs:
 * - the flow YAML parses and has nodes
 * - every read node's CSV exists and has a header row
 * - every column referenced by a transform node resolves to either a source
 *   column (from a read CSV in the flow) or a column produced upstream
 *   (a select/group_by new_name, a pivot index column). This catches typos in
 *   field names — the #1 reason a template would fail to execute.
 */

import { describe, it, expect } from 'vitest'
import { readFileSync } from 'node:fs'
import { resolve } from 'node:path'
import yaml from 'js-yaml'
import { FLOW_TEMPLATES } from '../../src/config/templates'

const publicUrl = (rel: string) => resolve(process.cwd(), 'public', rel)

function readText(rel: string): string {
  return readFileSync(publicUrl(rel), 'utf-8')
}

function csvHeaders(rel: string): string[] {
  const text = readText(rel)
  const firstLine = text.split(/\r?\n/).find((l) => l.length > 0) ?? ''
  return firstLine.split(',').map((c) => c.trim())
}

interface Node {
  id: number
  type: string
  setting_input?: Record<string, any>
}

/** Columns a node references (that must exist in its input). */
function referencedColumns(node: Node): string[] {
  const s = node.setting_input ?? {}
  const cols: string[] = []
  switch (node.type) {
    case 'filter':
      if (s.filter_input?.mode === 'basic' && s.filter_input?.basic_filter?.field) {
        cols.push(s.filter_input.basic_filter.field)
      }
      break
    case 'sort':
      for (const sc of s.sort_input ?? []) cols.push(sc.column)
      break
    case 'select':
      for (const si of s.select_input ?? []) cols.push(si.old_name)
      break
    case 'unique':
      for (const c of s.unique_input?.columns ?? s.unique_input?.subset ?? []) cols.push(c)
      break
    case 'group_by':
      for (const ac of s.groupby_input?.agg_cols ?? []) cols.push(ac.old_name)
      break
    case 'pivot':
      for (const c of s.pivot_input?.index_columns ?? []) cols.push(c)
      if (s.pivot_input?.pivot_column) cols.push(s.pivot_input.pivot_column)
      if (s.pivot_input?.value_col) cols.push(s.pivot_input.value_col)
      break
    case 'join':
      for (const m of s.join_input?.join_mapping ?? []) {
        cols.push(m.left_col, m.right_col)
      }
      break
  }
  return cols.filter(Boolean)
}

/** Column names a node produces (renames / aggregation outputs). */
function producedColumns(node: Node): string[] {
  const s = node.setting_input ?? {}
  const cols: string[] = []
  if (node.type === 'select') {
    for (const si of s.select_input ?? []) if (si.keep !== false) cols.push(si.new_name)
  } else if (node.type === 'group_by') {
    for (const ac of s.groupby_input?.agg_cols ?? []) cols.push(ac.new_name)
  } else if (node.type === 'pivot') {
    for (const c of s.pivot_input?.index_columns ?? []) cols.push(c)
  }
  return cols.filter(Boolean)
}

describe('Built-in flow templates', () => {
  it('exposes a non-empty, unique-id template set', () => {
    expect(FLOW_TEMPLATES.length).toBeGreaterThanOrEqual(5)
    const ids = FLOW_TEMPLATES.map((t) => t.id)
    expect(new Set(ids).size).toBe(ids.length)
  })

  for (const template of FLOW_TEMPLATES) {
    describe(template.name, () => {
      const flow = yaml.load(readText(template.flowPath)) as { nodes: Node[] }

      it('parses to a flow with nodes', () => {
        expect(Array.isArray(flow.nodes)).toBe(true)
        expect(flow.nodes.length).toBe(template.nodeCount)
      })

      it('every read node has an existing, non-empty CSV', () => {
        const reads = flow.nodes.filter((n) => n.type === 'read')
        expect(reads.length).toBeGreaterThan(0)
        for (const r of reads) {
          const fileName = r.setting_input?.file_name || r.setting_input?.received_file?.name
          expect(fileName, `read node #${r.id} has a file_name`).toBeTruthy()
          const headers = csvHeaders(template.dataDir + fileName)
          expect(headers.length, `${fileName} has headers`).toBeGreaterThan(0)
        }
      })

      it('references only known columns (source or produced upstream)', () => {
        // Union of every read CSV's columns + every column produced in the flow.
        const allowed = new Set<string>()
        for (const n of flow.nodes) {
          if (n.type === 'read') {
            const fileName = n.setting_input?.file_name || n.setting_input?.received_file?.name
            for (const h of csvHeaders(template.dataDir + fileName)) allowed.add(h)
          }
          for (const p of producedColumns(n)) allowed.add(p)
        }
        for (const n of flow.nodes) {
          // explore_data references Graphic Walker field ids (incl. synthetic), not table columns.
          if (n.type === 'explore_data') continue
          for (const col of referencedColumns(n)) {
            expect(allowed.has(col), `node #${n.id} (${n.type}) references unknown column "${col}"`).toBe(true)
          }
        }
      })
    })
  }
})
