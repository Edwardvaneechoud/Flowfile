/**
 * ManualInputSettings Component Tests
 *
 * Regression guard: raw_data_format.data is columnar (data[colIdx][rowIdx],
 * matching flowfile_core RawData). The grid must load it row-wise, not transpose.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest'
import { nextTick } from 'vue'
import { mount } from '@vue/test-utils'
import { createPinia, setActivePinia } from 'pinia'
import ManualInputSettings from '../../src/components/nodes/ManualInputSettings.vue'

vi.mock('../../src/stores/flow-store', () => ({
  useFlowStore: () => ({
    getTextContent: vi.fn(() => null),
    setFileContent: vi.fn(),
    setSourceNodeSchema: vi.fn()
  })
}))

function makeSettings(raw_data_format: unknown) {
  return {
    node_id: 1,
    is_setup: true,
    cache_results: true,
    pos_x: 0,
    pos_y: 0,
    description: '',
    raw_data_format
  } as any
}

function cellValues(wrapper: ReturnType<typeof mount>) {
  return wrapper.findAll('tr.data-row').map(row =>
    row.findAll('input.input-cell').map(i => (i.element as HTMLInputElement).value)
  )
}

describe('ManualInputSettings', () => {
  beforeEach(() => {
    setActivePinia(createPinia())
  })

  it('loads columnar raw_data_format into rows without transposing', async () => {
    const wrapper = mount(ManualInputSettings, {
      props: {
        nodeId: 1,
        settings: makeSettings({
          columns: [
            { name: 'a', data_type: 'String' },
            { name: 'b', data_type: 'String' },
            { name: 'c', data_type: 'String' }
          ],
          // columnar: a=[a1,a2], b=[b1,b2], c=[c1,c2]
          data: [
            ['a1', 'a2'],
            ['b1', 'b2'],
            ['c1', 'c2']
          ]
        })
      }
    })
    await nextTick()

    expect(cellValues(wrapper)).toEqual([
      ['a1', 'b1', 'c1'],
      ['a2', 'b2', 'c2']
    ])
  })

  it('loads the reported 3-col/6-row case as 6 rows, not 3 transposed rows', async () => {
    const wrapper = mount(ManualInputSettings, {
      props: {
        nodeId: 1,
        settings: makeSettings({
          columns: [
            { name: 'product_line', data_type: 'String' },
            { name: 'department', data_type: 'String' },
            { name: 'annual_target', data_type: 'Int64' }
          ],
          data: [
            ['Home and lifestyle', 'Fashion accessories', 'Food and beverages', 'Sports and travel', 'Health and beauty', 'Electronic accessories'],
            ['Homeware', 'Apparel', 'Grocery', 'Leisure', 'Personal care', 'Electronics'],
            [55000, 50000, 52000, 50000, 48000, 45000]
          ]
        })
      }
    })
    await nextTick()

    const cells = cellValues(wrapper)
    expect(cells.length).toBe(6)
    expect(cells[0]).toEqual(['Home and lifestyle', 'Homeware', '55000'])
    expect(cells[5]).toEqual(['Electronic accessories', 'Electronics', '45000'])
  })
})
