/**
 * The shared flowfile-formula editor used by the Formula node and the Filter
 * node's advanced mode.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest'
import { mount } from '@vue/test-utils'
import { createPinia, setActivePinia } from 'pinia'
import ExpressionEditor from '../../src/components/common/ExpressionEditor.vue'

vi.mock('../../src/stores/flow-store', () => ({
  useFlowStore: () => ({
    getNodeInputSchema: vi.fn(() => [
      { name: 'quantity', data_type: 'Int64' },
      { name: 'city', data_type: 'String' },
      { name: 'ordered_at', data_type: 'Date' }
    ])
  })
}))

vi.mock('../../src/stores/pyodide-store', () => ({
  usePyodideStore: () => ({ isReady: false })
}))

vi.mock('vue-codemirror', () => ({
  Codemirror: {
    props: ['modelValue', 'placeholder'],
    template: '<pre class="cm-stub" :data-placeholder="placeholder">{{ modelValue }}</pre>'
  }
}))

function mountEditor(props: Record<string, unknown> = {}) {
  return mount(ExpressionEditor, { props: { nodeId: 1, modelValue: '', ...props } })
}

describe('ExpressionEditor', () => {
  beforeEach(() => {
    setActivePinia(createPinia())
  })

  it('lists the input columns with their data-type badges', () => {
    const wrapper = mountEditor()
    const items = wrapper.findAll('.item')

    expect(items).toHaveLength(3)
    expect(items[0].find('.item-name').text()).toBe('quantity')
    expect(items[0].find('.type-badge').text()).toBe('Int64')
    expect(items[0].find('.type-badge').classes()).toContain('badge-numeric')
    expect(items[1].find('.type-badge').classes()).toContain('badge-string')
    expect(items[2].find('.type-badge').classes()).toContain('badge-date')
  })

  it('filters the field list from the search box', async () => {
    const wrapper = mountEditor()
    await wrapper.find('.search-input').setValue('cit')

    const items = wrapper.findAll('.item')
    expect(items).toHaveLength(1)
    expect(items[0].find('.item-name').text()).toBe('city')

    await wrapper.find('.search-input').setValue('nothing')
    expect(wrapper.find('.item-empty').text()).toBe('No input columns')
  })

  it('shows the loading state on the Functions tab until the package arrives', async () => {
    const wrapper = mountEditor()
    await wrapper.findAll('.tab')[1].trigger('click')

    expect(wrapper.find('.fn-tree').exists()).toBe(true)
    expect(wrapper.find('.item-empty').text()).toContain('Loading functions')
  })

  it('derives the placeholder from the columns, unless one is given', () => {
    expect(mountEditor().find('.cm-stub').attributes('data-placeholder')).toBe(
      'e.g. [quantity] + [city]'
    )
    expect(
      mountEditor({ placeholder: 'e.g. [quantity] > 7' })
        .find('.cm-stub')
        .attributes('data-placeholder')
    ).toBe('e.g. [quantity] > 7')
  })
})
