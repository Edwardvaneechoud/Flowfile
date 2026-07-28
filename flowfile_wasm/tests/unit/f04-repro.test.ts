import { describe, it, expect } from 'vitest'
import { useCodeGeneration } from '../../src/composables/useCodeGeneration'
import type { FlowNode } from '../../src/types'

describe('F04 repro: group_by with no groupby-tagged column', () => {
  const { generateCode } = useCodeGeneration()

  function createNode(id: number, type: string, settings: any, inputIds: number[] = []): FlowNode {
    return {
      id,
      type,
      x: 0,
      y: 0,
      settings: {
        node_id: id,
        is_setup: true,
        cache_results: true,
        pos_x: 0,
        pos_y: 0,
        description: '',
        ...settings
      },
      inputIds,
      leftInputId: undefined,
      rightInputId: undefined
    } as FlowNode
  }

  it('emits group_by([])', () => {
    const nodes = new Map<number, FlowNode>()
    nodes.set(1, createNode(1, 'manual_input', {
      raw_data_format: { columns: [{ name: 'a', data_type: 'Int64' }], data: [[1], [2], [3]] }
    }))
    nodes.set(2, createNode(2, 'group_by', {
      groupby_input: { agg_cols: [{ old_name: 'a', new_name: 'a_sum', agg: 'sum' }] }
    }, [1]))

    const code = generateCode({
      nodes,
      edges: [{ id: 'e1-2', source: '1', target: '2', sourceHandle: 'output-0', targetHandle: 'input-0' }] as any
    })
    console.log(code)
    expect(code).toContain('.group_by([]).agg([')
  })
})
