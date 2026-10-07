import { describe, it, expect } from 'vitest'
import { mount } from '@vue/test-utils'
import CellOutput from '../../src/components/notebook/CellOutput.vue'

const PIVOTED = {
  state: 'rows' as const,
  nodeId: 3,
  columns: ['region', '2021', '2022'],
  dtypes: { region: 'String', '2021': 'Int64', '2022': 'Int64' },
  rows: [{ region: 'EU', '2021': 1, '2022': 2 }],
  total: 1
}

describe('CellOutput', () => {
  it('keeps the columns in the order the step gives them, each with its type', () => {
    const wrapper = mount(CellOutput, { props: { output: PIVOTED } })
    const headers = wrapper.findAll('th').map(header => header.text())
    expect(headers).toEqual(['regionString', '2021Int64', '2022Int64'])
    expect(wrapper.findAll('td').map(cell => cell.text())).toEqual(['EU', '1', '2'])
  })

  it('says when what it shows is out of date', () => {
    expect(mount(CellOutput, { props: { output: PIVOTED } }).find('[data-stale]').exists()).toBe(false)
    const stale = mount(CellOutput, { props: { output: PIVOTED, stale: true } })
    expect(stale.find('[data-stale]').exists()).toBe(true)
    expect(stale.text()).toContain('Out of date')
  })
})
