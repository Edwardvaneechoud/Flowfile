/**
 * FilterSettings Component Tests
 */

import { describe, it, expect, vi, beforeEach } from 'vitest'
import { mount } from '@vue/test-utils'
import { createPinia, setActivePinia } from 'pinia'
import FilterSettings from '../../src/components/nodes/FilterSettings.vue'
import type { FilterSettings as FilterSettingsType } from '../../src/types'

// Mock the flow store
vi.mock('../../src/stores/flow-store', () => ({
  useFlowStore: () => ({
    getNodeInputSchema: vi.fn(() => [
      { name: 'id', data_type: 'Int64' },
      { name: 'name', data_type: 'String' },
      { name: 'value', data_type: 'Float64' },
      { name: 'active', data_type: 'Boolean' }
    ])
  })
}))

describe('FilterSettings', () => {
  beforeEach(() => {
    setActivePinia(createPinia())
  })

  const defaultSettings: FilterSettingsType = {
    node_id: 1,
    is_setup: false,
    cache_results: true,
    pos_x: 0,
    pos_y: 0,
    description: '',
    filter_input: {
      mode: 'basic',
      basic_filter: {
        field: '',
        operator: 'equals',
        value: '',
        value2: ''
      },
      advanced_filter: ''
    }
  }

  it('should render column selector with available columns', () => {
    const wrapper = mount(FilterSettings, {
      props: {
        nodeId: 1,
        settings: defaultSettings
      }
    })

    const select = wrapper.find('select')
    const options = select.findAll('option')

    // Should have "Select column..." + 4 columns
    expect(options.length).toBe(5)
    expect(options[1].text()).toContain('id')
    expect(options[2].text()).toContain('name')
    expect(options[3].text()).toContain('value')
    expect(options[4].text()).toContain('active')
  })

  it('should render all filter operators', () => {
    const wrapper = mount(FilterSettings, {
      props: {
        nodeId: 1,
        settings: defaultSettings
      }
    })

    const selects = wrapper.findAll('select')
    const operatorSelect = selects[1] // Second select is operator
    const options = operatorSelect.findAll('option')

    expect(options.some(o => o.text() === 'Equals')).toBe(true)
    expect(options.some(o => o.text() === 'Not Equals')).toBe(true)
    expect(options.some(o => o.text() === 'Greater Than')).toBe(true)
    expect(options.some(o => o.text() === 'Contains')).toBe(true)
    expect(options.some(o => o.text() === 'Is Null')).toBe(true)
    expect(options.some(o => o.text() === 'Between')).toBe(true)
  })

  it('should show value input for standard operators', () => {
    const wrapper = mount(FilterSettings, {
      props: {
        nodeId: 1,
        settings: {
          ...defaultSettings,
          filter_input: {
            mode: 'basic',
            basic_filter: {
              field: 'name',
              operator: 'equals',
              value: 'test',
              value2: ''
            },
            advanced_filter: ''
          }
        }
      }
    })

    const inputs = wrapper.findAll('input[type="text"]')
    expect(inputs.length).toBe(1) // Only value input, no value2
  })

  it('should show second value input for BETWEEN operator', () => {
    const wrapper = mount(FilterSettings, {
      props: {
        nodeId: 1,
        settings: {
          ...defaultSettings,
          filter_input: {
            mode: 'basic',
            basic_filter: {
              field: 'value',
              operator: 'between',
              value: '10',
              value2: '100'
            },
            advanced_filter: ''
          }
        }
      }
    })

    const inputs = wrapper.findAll('input[type="text"]')
    expect(inputs.length).toBe(2) // value and value2 inputs
  })

  it('should hide value input for IS_NULL operator', () => {
    const wrapper = mount(FilterSettings, {
      props: {
        nodeId: 1,
        settings: {
          ...defaultSettings,
          filter_input: {
            mode: 'basic',
            basic_filter: {
              field: 'name',
              operator: 'is_null',
              value: '',
              value2: ''
            },
            advanced_filter: ''
          }
        }
      }
    })

    const inputs = wrapper.findAll('input[type="text"]')
    expect(inputs.length).toBe(0) // No value inputs for is_null
  })

  it('should emit update:settings when field changes', async () => {
    const wrapper = mount(FilterSettings, {
      props: {
        nodeId: 1,
        settings: defaultSettings
      }
    })

    const select = wrapper.find('select')
    await select.setValue('name')

    const emitted = wrapper.emitted('update:settings')
    expect(emitted).toBeTruthy()
    expect(emitted![0][0]).toMatchObject({
      filter_input: {
        mode: 'basic',
        basic_filter: {
          field: 'name'
        }
      }
    })
  })

  it('should emit update:settings when operator changes', async () => {
    const wrapper = mount(FilterSettings, {
      props: {
        nodeId: 1,
        settings: {
          ...defaultSettings,
          filter_input: {
            mode: 'basic',
            basic_filter: {
              field: 'name',
              operator: 'equals',
              value: '',
              value2: ''
            },
            advanced_filter: ''
          }
        }
      }
    })

    const selects = wrapper.findAll('select')
    await selects[1].setValue('contains')

    const emitted = wrapper.emitted('update:settings')
    expect(emitted).toBeTruthy()
    expect(emitted![0][0]).toMatchObject({
      filter_input: {
        basic_filter: {
          operator: 'contains'
        }
      }
    })
  })

  it('should emit update:settings when value changes', async () => {
    const wrapper = mount(FilterSettings, {
      props: {
        nodeId: 1,
        settings: {
          ...defaultSettings,
          filter_input: {
            mode: 'basic',
            basic_filter: {
              field: 'name',
              operator: 'equals',
              value: '',
              value2: ''
            },
            advanced_filter: ''
          }
        }
      }
    })

    const input = wrapper.find('input[type="text"]')
    await input.setValue('test value')

    const emitted = wrapper.emitted('update:settings')
    expect(emitted).toBeTruthy()
    expect(emitted![0][0]).toMatchObject({
      filter_input: {
        basic_filter: {
          value: 'test value'
        }
      }
    })
  })

  it('should show help text for IN operator', () => {
    const wrapper = mount(FilterSettings, {
      props: {
        nodeId: 1,
        settings: {
          ...defaultSettings,
          filter_input: {
            mode: 'basic',
            basic_filter: {
              field: 'name',
              operator: 'in',
              value: '',
              value2: ''
            },
            advanced_filter: ''
          }
        }
      }
    })

    expect(wrapper.text()).toContain('comma-separated')
  })

  it('should show help text for BETWEEN operator', () => {
    const wrapper = mount(FilterSettings, {
      props: {
        nodeId: 1,
        settings: {
          ...defaultSettings,
          filter_input: {
            mode: 'basic',
            basic_filter: {
              field: 'value',
              operator: 'between',
              value: '',
              value2: ''
            },
            advanced_filter: ''
          }
        }
      }
    })

    expect(wrapper.text()).toContain('boundaries')
  })

  it('should display data type in column options', () => {
    const wrapper = mount(FilterSettings, {
      props: {
        nodeId: 1,
        settings: defaultSettings
      }
    })

    const select = wrapper.find('select')
    const options = select.findAll('option')

    expect(options[1].text()).toContain('Int64')
    expect(options[2].text()).toContain('String')
    expect(options[3].text()).toContain('Float64')
    expect(options[4].text()).toContain('Boolean')
  })
})
