/**
 * SelectSettings Component Tests
 */

import { describe, it, expect, vi, beforeEach } from 'vitest'
import { mount } from '@vue/test-utils'
import { createPinia, setActivePinia } from 'pinia'
import SelectSettings from '../../src/components/nodes/SelectSettings.vue'
import type { SelectSettings as SelectSettingsType } from '../../src/types'

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
    expect(checkboxes.length).toBe(3)
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
    expect(inputs.length).toBe(3)

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

  it('should render the keep-all button', () => {
    const wrapper = mount(SelectSettings, {
      props: {
        nodeId: 1,
        settings: defaultSettings
      }
    })

    expect(wrapper.find('button[aria-label="Keep all columns"]').exists()).toBe(true)
  })

  it('should render the keep-none button', () => {
    const wrapper = mount(SelectSettings, {
      props: {
        nodeId: 1,
        settings: defaultSettings
      }
    })

    expect(wrapper.find('button[aria-label="Keep no columns"]').exists()).toBe(true)
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

    await wrapper.find('button[aria-label="Keep all columns"]').trigger('click')

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

    await wrapper.find('button[aria-label="Keep no columns"]').trigger('click')

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

  it('should put the drag affordance on the handle cell, not the row', () => {
    // A draggable row hijacks mousedown inside the rename input.
    const wrapper = mount(SelectSettings, {
      props: {
        nodeId: 1,
        settings: defaultSettings
      }
    })

    const rows = wrapper.findAll('tbody tr')
    expect(rows.length).toBe(3)
    rows.forEach(row => {
      expect(row.attributes('draggable')).toBeUndefined()
      expect(row.find('td.drag-handle-cell').attributes('draggable')).toBe('true')
    })
  })

  it('should render a data type badge per row, with the full dtype in the title', () => {
    const wrapper = mount(SelectSettings, {
      props: {
        nodeId: 1,
        settings: {
          ...defaultSettings,
          select_input: [
            {
              old_name: 'created',
              new_name: 'created',
              keep: true,
              position: 0,
              data_type: "Datetime(time_unit='us', time_zone=None)"
            }
          ]
        }
      }
    })

    const badge = wrapper.find('.type-badge')
    expect(badge.exists()).toBe(true)
    expect(badge.text()).toBe('Datetime')
    expect(badge.classes()).toContain('badge-date')
    expect(badge.attributes('title')).toBe("Datetime(time_unit='us', time_zone=None)")
  })

  it('should narrow the rows to columns matching the filter', async () => {
    const wrapper = mount(SelectSettings, {
      props: {
        nodeId: 1,
        settings: defaultSettings
      }
    })

    expect(wrapper.findAll('tbody tr').length).toBe(3)

    await wrapper.find('.column-picker-search input').setValue('nam')

    const rows = wrapper.findAll('tbody tr')
    expect(rows.length).toBe(1)
    expect(rows[0].text()).toContain('name')
  })

  it('should report how many columns are kept', async () => {
    const wrapper = mount(SelectSettings, {
      props: {
        nodeId: 1,
        settings: defaultSettings
      }
    })

    expect(wrapper.find('.column-picker-count').text()).toBe('3 of 3 kept')

    await wrapper.findAll('input[type="checkbox"]')[0].setValue(false)

    expect(wrapper.find('.column-picker-count').text()).toBe('2 of 3 kept')
  })

  it('should opt every free-text field out of password-manager autofill', () => {
    // A column named name/city/email reads as a contact form to LastPass.
    const wrapper = mount(SelectSettings, {
      props: {
        nodeId: 1,
        settings: defaultSettings
      }
    })

    const fields = [
      ...wrapper.findAll('input.inline-input'),
      wrapper.find('.column-picker-search input')
    ]
    expect(fields.length).toBe(4)

    for (const field of fields) {
      expect(field.attributes('autocomplete')).toBe('off')
      expect(field.attributes('data-lpignore')).toBe('true')
      expect(field.attributes('data-1p-ignore')).toBe('true')
      expect(field.attributes('data-bwignore')).toBe('true')
      expect(field.attributes('data-form-type')).toBe('other')
    }
  })

  it('should mark the row under the pointer while dragging, and clear it after', async () => {
    const wrapper = mount(SelectSettings, {
      props: {
        nodeId: 1,
        settings: defaultSettings
      }
    })

    const rows = wrapper.findAll('tbody tr')
    await rows[0].find('td.drag-handle-cell').trigger('dragstart')
    await rows[2].trigger('dragover')
    expect(rows[2].classes()).toContain('is-drop-target')
    // never on the row being dragged
    expect(rows[0].classes()).not.toContain('is-drop-target')

    // an Escape-cancelled drag must not leave the highlight behind
    await wrapper.find('tbody').trigger('dragend')
    expect(wrapper.find('.is-drop-target').exists()).toBe(false)
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
    expect(rows.length).toBe(1)
    expect(wrapper.text()).toContain('id')
    expect(wrapper.text()).not.toContain('old_col')
  })
})
