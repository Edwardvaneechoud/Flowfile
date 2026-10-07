/**
 * FormulaSettings Component Tests
 *
 * The panel edits an ordered list of formulas and writes them in flowfile_core's
 * shape: `functions` always, `function` mirrored only for exactly one entry.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest'
import { mount } from '@vue/test-utils'
import { createPinia, setActivePinia } from 'pinia'
import FormulaSettings from '../../src/components/nodes/FormulaSettings.vue'
import ExpressionEditor from '../../src/components/common/ExpressionEditor.vue'
import type { FunctionInput, NodeFormulaSettings } from '../../src/types'

vi.mock('../../src/stores/flow-store', () => ({
  useFlowStore: () => ({
    getNodeInputSchema: vi.fn(() => [
      { name: 'price', data_type: 'Float64' },
      { name: 'quantity', data_type: 'Int64' }
    ])
  })
}))

vi.mock('../../src/stores/pyodide-store', () => ({
  usePyodideStore: () => ({ isReady: false })
}))

vi.mock('vue-codemirror', () => ({
  Codemirror: {
    props: ['modelValue', 'placeholder'],
    template: '<pre class="cm-stub">{{ modelValue }}</pre>'
  }
}))

const entry = (name: string, formula: string, dataType = 'Auto'): FunctionInput => ({
  field: { name, data_type: dataType },
  function: formula
})

const base = { node_id: 1, is_setup: true, cache_results: true, pos_x: 0, pos_y: 0, description: '' }

const mountPanel = (settings: Partial<NodeFormulaSettings>) =>
  mount(FormulaSettings, { props: { nodeId: 1, settings: { ...base, ...settings } as NodeFormulaSettings } })

const lastEmitted = (wrapper: ReturnType<typeof mountPanel>): NodeFormulaSettings => {
  const events = wrapper.emitted('update:settings')!
  return events[events.length - 1][0] as NodeFormulaSettings
}

const entryNames = (wrapper: ReturnType<typeof mountPanel>) =>
  wrapper.findAll('.entry-name').map(el => el.text())

describe('FormulaSettings', () => {
  beforeEach(() => {
    setActivePinia(createPinia())
  })

  it('shows a single formula without the entry list and mirrors it onto `function`', async () => {
    const wrapper = mountPanel({ function: entry('total', '[price] * [quantity]') })

    expect(wrapper.find('.entry-list').exists()).toBe(false)
    expect(wrapper.findComponent(ExpressionEditor).props('modelValue')).toBe('[price] * [quantity]')

    wrapper.findComponent(ExpressionEditor).vm.$emit('update:modelValue', '[price] * 2')
    await wrapper.vm.$nextTick()

    const emitted = lastEmitted(wrapper)
    expect(emitted.functions).toEqual([entry('total', '[price] * 2')])
    expect(emitted.function).toEqual(entry('total', '[price] * 2'))
    expect(emitted.is_setup).toBe(true)
  })

  it('opens a flowfile_core multi-entry node, which carries `functions` and no `function`', () => {
    const wrapper = mountPanel({
      functions: [entry('total', '[price] * [quantity]'), entry('with_tax', '[total] * 1.21', 'Float64')]
    })

    expect(entryNames(wrapper)).toEqual(['total', 'with_tax'])
    expect(wrapper.findComponent(ExpressionEditor).props('modelValue')).toBe('[price] * [quantity]')
  })

  it('offers the columns earlier formulas make to the one being edited', async () => {
    const wrapper = mountPanel({
      functions: [entry('total', '[price] * [quantity]'), entry('with_tax', '[total] * 1.21')]
    })
    expect(wrapper.findComponent(ExpressionEditor).props('extraColumns')).toEqual([])

    await wrapper.findAll('.entry-select')[1].trigger('click')

    expect(wrapper.findComponent(ExpressionEditor).props('modelValue')).toBe('[total] * 1.21')
    expect(wrapper.findComponent(ExpressionEditor).props('extraColumns')).toEqual([
      { name: 'total', data_type: 'Auto' }
    ])
  })

  it('adds a formula and drops the `function` mirror once there are two', async () => {
    const wrapper = mountPanel({ function: entry('total', '[price] * [quantity]') })

    await wrapper.find('.add-entry').trigger('click')

    const emitted = lastEmitted(wrapper)
    expect(emitted.functions).toEqual([entry('total', '[price] * [quantity]'), entry('', '')])
    expect('function' in emitted).toBe(false)
    // The blank row produces no column, so it does not make the node unconfigured.
    expect(emitted.is_setup).toBe(true)
    expect(wrapper.findAll('.entry-item')).toHaveLength(2)
    expect(wrapper.findAll('.entry-item')[1].classes()).toContain('active')
  })

  it('reorders formulas, and the order is what gets saved', async () => {
    const wrapper = mountPanel({
      functions: [entry('a', '[price]'), entry('b', '[a] + 1'), entry('c', '[b] + 1')]
    })

    await wrapper.find('[aria-label="Move formula 3 up"]').trigger('click')
    expect(entryNames(wrapper)).toEqual(['a', 'c', 'b'])
    expect(lastEmitted(wrapper).functions!.map(fn => fn.field.name)).toEqual(['a', 'c', 'b'])

    await wrapper.find('[aria-label="Move formula 1 down"]').trigger('click')
    expect(lastEmitted(wrapper).functions!.map(fn => fn.field.name)).toEqual(['c', 'a', 'b'])

    expect(wrapper.find('[aria-label="Move formula 1 up"]').attributes('disabled')).toBeDefined()
    expect(wrapper.find('[aria-label="Move formula 3 down"]').attributes('disabled')).toBeDefined()
  })

  it('removes a formula and mirrors `function` again when one is left', async () => {
    const wrapper = mountPanel({ functions: [entry('a', '[price]'), entry('b', '[a] + 1')] })

    await wrapper.find('[aria-label="Remove formula 1"]').trigger('click')

    const emitted = lastEmitted(wrapper)
    expect(emitted.functions).toEqual([entry('b', '[a] + 1')])
    expect(emitted.function).toEqual(entry('b', '[a] + 1'))
    expect(wrapper.find('.entry-list').exists()).toBe(false)
    expect(wrapper.findComponent(ExpressionEditor).props('modelValue')).toBe('[a] + 1')
  })

  it('is not set up while a formula with an expression has no output field', async () => {
    const wrapper = mountPanel({ functions: [entry('a', '[price]'), entry('', '[a] + 1')] })

    await wrapper.findAll('.entry-select')[1].trigger('click')
    wrapper.findComponent(ExpressionEditor).vm.$emit('update:modelValue', '[a] + 2')
    await wrapper.vm.$nextTick()
    expect(lastEmitted(wrapper).is_setup).toBe(false)

    await wrapper.find('input[type="text"]').setValue('b')
    expect(lastEmitted(wrapper).is_setup).toBe(true)
    expect(lastEmitted(wrapper).functions![1]).toEqual(entry('b', '[a] + 2'))
  })
})
