/**
 * validateFlowfileData embeds the flow JSON into a Python json.loads(...) call.
 * The payload must survive Python string-literal parsing: values containing
 * newlines/tabs/quotes/backslashes (e.g. multi-line template descriptions) must
 * not smuggle a raw control character into json.loads. We capture the generated
 * Python and prove the embedded literal round-trips back to the original data.
 *
 * JSON.parse of a JSON string literal decodes the exact same escape set Python
 * decodes for a string literal, so parsing the captured literal with JSON.parse
 * is a faithful proxy for what Pyodide does before json.loads runs.
 */

import { describe, it, expect, beforeEach, vi } from 'vitest'
import { createPinia, setActivePinia } from 'pinia'

const mocks = vi.hoisted(() => ({
  runPythonWithResult: vi.fn().mockResolvedValue({ success: true })
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
import type { FlowfileData } from '../../src/types'

// Pull the argument out of `data = json.loads(<literal>)` and decode it the way
// Pyodide would: outer string literal -> inner JSON text -> the object.
function decodeEmbeddedData(code: string): unknown {
  const m = code.match(/json\.loads\((".*")\)/s)
  if (!m) throw new Error('no json.loads(...) literal found in generated code')
  const innerJsonText = JSON.parse(m[1]) as string // Python string-literal decode
  return JSON.parse(innerJsonText) // json.loads
}

describe('validateFlowfileData embedding', () => {
  beforeEach(() => {
    setActivePinia(createPinia())
    vi.clearAllMocks()
    mocks.runPythonWithResult.mockResolvedValue({ success: true })
  })

  it('safely embeds control characters and escapes from a flow with a multi-line description', async () => {
    const store = useFlowStore()
    const data = {
      flowfile_version: '0.12.8',
      nodes: [
        {
          id: 1,
          type: 'manual_input',
          description: 'Clean and enrich raw sales,\nthen branch into three views:\ta\\b "quoted"',
          setting_input: {
            function: { function: 'format_date([date], "%Y-%m")' }
          }
        }
      ]
    } as unknown as FlowfileData

    const result = await store.validateFlowfileData(data)

    // The fix means validation actually ran (no JSONDecodeError thrown/caught).
    expect(result.success).toBe(true)

    const code = mocks.runPythonWithResult.mock.calls
      .map(c => c[0] as string)
      .find(c => c.includes('validate_flowfile_data'))!
    expect(code).toBeTruthy()
    // No raw newline may appear inside the embedded json.loads string literal.
    const literal = code.match(/json\.loads\((".*")\)/s)![1]
    expect(literal.includes('\n')).toBe(false)
    // And it must decode back to exactly the original data.
    expect(decodeEmbeddedData(code)).toEqual(data)
  })
})
