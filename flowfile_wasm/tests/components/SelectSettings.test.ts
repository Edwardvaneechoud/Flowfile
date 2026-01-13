/**
 * SelectSettings Component Tests
 */

import { describe, it, expect, vi, beforeEach } from 'vitest'
import { mount } from '@vue/test-utils'
import { createPinia, setActivePinia } from 'pinia'
import SelectSettings from '../../src/components/nodes/SelectSettings.vue'
import type { SelectSettings as SelectSettingsType } from '../../src/types'

// Mock the flow store
const mockGetNode = vi.fn()
const mockGetNodeInputSchema = vi.fn()

vi.mock('../../src/stores/flow-store', () => ({
  useFlowStore: () => ({
    getNode: mockGetNode,
    getNodeInputSchema: mockGetNodeInputSchema
  })
}))

describe('SelectSettings', () => {
  beforeEach(() => {
    setActivePinia(createPinia())
    mockGetNode.mockReturnValue({
      id: 1,
      type: 'select',
      inputIds: [0],
      leftInputId: undefined
    })
    mockGetNodeInputSchema.mockReturnValue([
      { name: 'id', data_type: 'Int64' },
      { name: 'name', data_type: 'String' },
      { name: 'value', data_type: 'Float64' }
    ])
  })

  const defaultSettings: SelectSettingsType = {
    node_id: 1,
    is_setup: false,
    cache_results: true,
    pos_x: 0,
    pos_y: 0,
    description: '',
    select_input: [
      { old_name: 'id', new_name: 'id', keep: true, position: 0, data_type: 'Int64' },
      { old_name: 'name', new_name: 'name', keep: true, position: 1, data_type: 'String' },
      { old_name: 'value', new_name: 'value', keep: true, position: 2, data_type: 'Float64' }
    ]
  }

  it('should render columns table with all columns', () => {
    const wrapper = mount(SelectSettings, {
      props: {
        nodeId: 1,
        settings: defaultSettings
      }
    })

    const rows = wrapper.findAll('tbody tr')
    expect(rows.length).toBe(3)
  })

  it('should show column names in the table', () => {
    const wrapper = mount(SelectSettings, {
      props: {
        nodeId: 1,
        settings: defaultSettings
      }
    })

    expect(wrapper.text()).toContain('id')
    expect(wrapper.text()).toContain('name')
    expect(wrapper.text()).toContain('value')
  })

  it('should render checkboxes for keep/remove columns', () => {
    const wrapper = mount(SelectSettings, {
      props: {
        nodeId: 1,
        settings: defaultSettings
      }
    })

    const checkboxes = wrapper.findAll('input[type="checkbox"]')
    expect(checkboxes.length).toBe(3) // One for each column
    expect(checkboxes.every(cb => (cb.element as HTMLInputElement).checked)).toBe(true)
  })

  it('should emit update when checkbox is toggled', async () => {
    const wrapper = mount(SelectSettings, {
      props: {
        nodeId: 1,
        settings: defaultSettings
      }
    })

    const checkboxes = wrapper.findAll('input[type="checkbox"]')
    await checkboxes[0].setValue(false)

    const emitted = wrapper.emitted('update:settings')
    expect(emitted).toBeTruthy()

    const updatedSettings = emitted![0][0] as SelectSettingsType
    const updatedColumn = updatedSettings.select_input.find(c => c.old_name === 'id')
    expect(updatedColumn?.keep).toBe(false)
  })

  it('should render rename input fields', () => {
    const wrapper = mount(SelectSettings, {
      props: {
        nodeId: 1,
        settings: defaultSettings
      }
    })

    const inputs = wrapper.findAll('input[type="text"]')
    expect(inputs.length).toBe(3) // One for each column

    // Check initial values
    expect((inputs[0].element as HTMLInputElement).value).toBe('id')
    expect((inputs[1].element as HTMLInputElement).value).toBe('name')
    expect((inputs[2].element as HTMLInputElement).value).toBe('value')
  })

  it('should emit update when column is renamed', async () => {
    const wrapper = mount(SelectSettings, {
      props: {
        nodeId: 1,
        settings: defaultSettings
      }
    })

    const inputs = wrapper.findAll('input[type="text"]')
    await inputs[0].setValue('user_id')

    const emitted = wrapper.emitted('update:settings')
    expect(emitted).toBeTruthy()

    const updatedSettings = emitted![0][0] as SelectSettingsType
    const updatedColumn = updatedSettings.select_input.find(c => c.old_name === 'id')
    expect(updatedColumn?.new_name).toBe('user_id')
  })

  it('should show "No input connected" when no input', () => {
    mockGetNode.mockReturnValue({
      id: 1,
      type: 'select',
      inputIds: [],
      leftInputId: undefined
    })

    const wrapper = mount(SelectSettings, {
      props: {
        nodeId: 1,
        settings: {
          ...defaultSettings,
          select_input: []
        }
      }
    })

    expect(wrapper.text()).toContain('No input connected')
  })

  it('should render Select All button', () => {
    const wrapper = mount(SelectSettings, {
      props: {
        nodeId: 1,
        settings: defaultSettings
      }
    })

    const buttons = wrapper.findAll('button')
    expect(buttons.some(b => b.text() === 'Select All')).toBe(true)
  })

  it('should render Deselect All button', () => {
    const wrapper = mount(SelectSettings, {
      props: {
        nodeId: 1,
        settings: defaultSettings
      }
    })

    const buttons = wrapper.findAll('button')
    expect(buttons.some(b => b.text() === 'Deselect All')).toBe(true)
  })

  it('should select all columns when Select All clicked', async () => {
    const settingsWithDeselected: SelectSettingsType = {
      ...defaultSettings,
      select_input: [
        { old_name: 'id', new_name: 'id', keep: false, position: 0, data_type: 'Int64' },
        { old_name: 'name', new_name: 'name', keep: true, position: 1, data_type: 'String' },
        { old_name: 'value', new_name: 'value', keep: false, position: 2, data_type: 'Float64' }
      ]
    }

    const wrapper = mount(SelectSettings, {
      props: {
        nodeId: 1,
        settings: settingsWithDeselected
      }
    })

    const selectAllBtn = wrapper.findAll('button').find(b => b.text() === 'Select All')
    await selectAllBtn!.trigger('click')

    const emitted = wrapper.emitted('update:settings')
    expect(emitted).toBeTruthy()

    const updatedSettings = emitted![0][0] as SelectSettingsType
    expect(updatedSettings.select_input.every(c => c.keep)).toBe(true)
  })

  it('should deselect all columns when Deselect All clicked', async () => {
    const wrapper = mount(SelectSettings, {
      props: {
        nodeId: 1,
        settings: defaultSettings
      }
    })

    const deselectAllBtn = wrapper.findAll('button').find(b => b.text() === 'Deselect All')
    await deselectAllBtn!.trigger('click')

    const emitted = wrapper.emitted('update:settings')
    expect(emitted).toBeTruthy()

    const updatedSettings = emitted![0][0] as SelectSettingsType
    expect(updatedSettings.select_input.every(c => !c.keep)).toBe(true)
  })

  it('should apply disabled styling to unchecked rows', () => {
    const settingsWithDeselected: SelectSettingsType = {
      ...defaultSettings,
      select_input: [
        { old_name: 'id', new_name: 'id', keep: false, position: 0, data_type: 'Int64' },
        { old_name: 'name', new_name: 'name', keep: true, position: 1, data_type: 'String' }
      ]
    }

    const wrapper = mount(SelectSettings, {
      props: {
        nodeId: 1,
        settings: settingsWithDeselected
      }
    })

    const rows = wrapper.findAll('tbody tr')
    expect(rows[0].classes()).toContain('row-disabled')
    expect(rows[1].classes()).not.toContain('row-disabled')
  })

  it('should support drag functionality', () => {
    const wrapper = mount(SelectSettings, {
      props: {
        nodeId: 1,
        settings: defaultSettings
      }
    })

    const rows = wrapper.findAll('tbody tr')
    // All rows should be draggable
    rows.forEach(row => {
      expect(row.attributes('draggable')).toBe('true')
    })
  })

  it('should hide unavailable columns from display', () => {
    const settingsWithUnavailable: SelectSettingsType = {
      ...defaultSettings,
      select_input: [
        { old_name: 'id', new_name: 'id', keep: true, position: 0, data_type: 'Int64', is_available: true },
        { old_name: 'old_col', new_name: 'old_col', keep: true, position: 1, data_type: 'String', is_available: false }
      ]
    }

    const wrapper = mount(SelectSettings, {
      props: {
        nodeId: 1,
        settings: settingsWithUnavailable
      }
    })

    const rows = wrapper.findAll('tbody tr')
    expect(rows.length).toBe(1) // Only available column shown
    expect(wrapper.text()).toContain('id')
    expect(wrapper.text()).not.toContain('old_col')
  })
})
