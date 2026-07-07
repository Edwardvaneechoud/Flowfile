/**
 * manual_input execution wiring.
 * A core-imported flow carries data as raw_data_format (columnar) with no CSV
 * text content. executeNode must run execute_manual_input off raw_data_format
 * instead of bailing early with "No data entered". We mock the Pyodide call.
 */

import { describe, it, expect, beforeEach, vi } from 'vitest'
import { createPinia, setActivePinia } from 'pinia'

const mocks = vi.hoisted(() => ({
  runPythonWithResult: vi.fn()
}))

vi.mock('../../src/stores/pyodide-store', () => ({
  usePyodideStore: () => ({
    isReady: true,
    runPython: vi.fn().mockResolvedValue(undefined),
    runPythonWithResult: mocks.runPythonWithResult,
    setGlobal: vi.fn(),
    deleteGlobal: vi.fn()
  })
}))

import { useFlowStore } from '../../src/stores/flow-store'

describe('manual_input execution', () => {
  beforeEach(() => {
    setActivePinia(createPinia())
    sessionStorage.clear()
    vi.clearAllMocks()
    mocks.runPythonWithResult.mockResolvedValue({
      success: true,
      schema: [{ name: 'product_line', data_type: 'String' }],
      has_data: true
    })
  })

  it('executes off raw_data_format when there is no CSV text (core-imported flow)', async () => {
    const store = useFlowStore()
    const id = store.addNode('manual_input', 0, 0)
    store.updateNodeSettings(id, {
      ...(store.getNode(id)!.settings as any),
      raw_data_format: {
        columns: [
          { name: 'product_line', data_type: 'String' },
          { name: 'annual_target', data_type: 'Int64' }
        ],
        // columnar (data[col][row]), no accompanying text content
        data: [
          ['Home and lifestyle', 'Homeware'],
          [55000, 50000]
        ]
      }
    })

    const result = await store.executeNode(id)

    expect(result.success).toBe(true)
    const calls = mocks.runPythonWithResult.mock.calls.map(c => c[0] as string)
    expect(calls.some(c => c.includes('execute_manual_input'))).toBe(true)
  })

  it('still fails with "No data entered" when there is neither text nor raw_data_format', async () => {
    const store = useFlowStore()
    const id = store.addNode('manual_input', 0, 0) // default empty raw_data_format
    const result = await store.executeNode(id)
    expect(result.success).toBe(false)
    expect(result.error).toMatch(/no data entered/i)
    const calls = mocks.runPythonWithResult.mock.calls.map(c => c[0] as string)
    expect(calls.some(c => c.includes('execute_manual_input'))).toBe(false)
  })
})
