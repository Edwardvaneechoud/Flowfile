/**
 * The drawer's "How would I write this myself?" section: the plain loop, and
 * beneath it the same step in Polars — except where there is nothing to bridge.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest'
import { mount } from '@vue/test-utils'
import NodeExplainer from '../../src/components/nodes/NodeExplainer.vue'
import { makeNode, flowWith } from '../helpers/flow-builder'
import type { FlowNode, FlowEdge } from '../../src/types'

let flow: { nodes: Map<number, FlowNode>; edges: FlowEdge[] } = { nodes: new Map(), edges: [] }

vi.mock('../../src/stores/flow-store', () => ({
  useFlowStore: () => ({
    get nodes() {
      return flow.nodes
    },
    get edges() {
      return flow.edges
    }
  })
}))

vi.mock('../../src/stores/pyodide-store', () => ({
  usePyodideStore: () => ({ isReady: false })
}))

vi.mock('vue-codemirror', () => ({
  Codemirror: {
    props: ['modelValue'],
    template: '<pre class="cm-stub">{{ modelValue }}</pre>'
  }
}))

const SOURCE = makeNode(1, 'manual_input', {
  raw_data_format: { columns: [{ name: 'a' }, { name: 'b' }], data: [[1, 2, 3], ['x', 'y', 'z']] }
})

function mountExplainer(nodeId: number) {
  return mount(NodeExplainer, { props: { nodeId } })
}

describe('NodeExplainer Polars bridge', () => {
  beforeEach(() => {
    localStorage.clear()
    localStorage.setItem('flowfile-node-explainer-open', '1')
  })

  it('shows the same step in Polars under the plain loop', () => {
    flow = flowWith(
      SOURCE,
      makeNode(
        2,
        'filter',
        { filter_input: { mode: 'basic', basic_filter: { field: 'a', operator: 'greater_than', value: '1' } } },
        [1]
      )
    )
    const wrapper = mountExplainer(2)
    const blocks = wrapper.findAll('.cm-stub')
    expect(blocks).toHaveLength(2)
    expect(blocks[0].text()).toContain('for row in source:')
    expect(blocks[1].text()).toContain('filtered = source.filter(')
    expect(wrapper.find('.explainer-bridge').text()).toContain('The same step in Polars')
  })

  it('hides the plain block for a stubbed node and shows only the Polars step', () => {
    flow = flowWith(
      SOURCE,
      makeNode(2, 'formula', { function: { field: { name: 'c' }, function: '[a] * 2' } }, [1])
    )
    const wrapper = mountExplainer(2)
    // No fake plain implementation — the Polars snippet is the code for this step.
    const blocks = wrapper.findAll('.cm-stub')
    expect(blocks).toHaveLength(1)
    expect(blocks[0].text()).toContain('simple_function_to_expr')
    expect(wrapper.find('.explainer-bridge').text()).toContain('This step in Polars')
  })

  it('offers no bridge on a Polars Code node — it already is Polars', () => {
    flow = flowWith(
      SOURCE,
      makeNode(2, 'polars_code', { polars_code_input: { polars_code: 'input_df.head(1)' } }, [1])
    )
    const wrapper = mountExplainer(2)
    // Stubbed (no plain form) AND nothing to bridge: prose only.
    expect(wrapper.find('.explainer-bridge').exists()).toBe(false)
    expect(wrapper.findAll('.cm-stub')).toHaveLength(0)
    expect(wrapper.find('.explainer-prose').text()).toContain('nothing to translate')
  })

  it('collapses each snippet on its own, and remembers it', async () => {
    flow = flowWith(
      SOURCE,
      makeNode(
        2,
        'filter',
        { filter_input: { mode: 'basic', basic_filter: { field: 'a', operator: 'greater_than', value: '1' } } },
        [1]
      )
    )
    const wrapper = mountExplainer(2)
    const [plain, polars] = wrapper.findAll('.block-toggle')
    expect(plain.text()).toContain('Plain Python')
    expect(polars.text()).toContain('1 line')

    await plain.trigger('click')
    // Only the plain editor goes; the Polars one and both Copy buttons stay.
    expect(wrapper.findAll('.cm-stub')).toHaveLength(1)
    expect(wrapper.findAll('.explainer-copy')).toHaveLength(2)
    expect(localStorage.getItem('flowfile-node-explainer-plain-open')).toBe('0')

    expect(mountExplainer(2).findAll('.cm-stub')).toHaveLength(1)
    await plain.trigger('click')
    expect(wrapper.findAll('.cm-stub')).toHaveLength(2)
  })

  it('shows only the connect hint when the node emits nothing', () => {
    flow = flowWith(SOURCE, makeNode(2, 'explore_data', {}, [1]))
    const wrapper = mountExplainer(2)
    expect(wrapper.find('.explainer-hint').text()).toContain('Connect this node')
    expect(wrapper.find('.explainer-bridge').exists()).toBe(false)
  })
})
