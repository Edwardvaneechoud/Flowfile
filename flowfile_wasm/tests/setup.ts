/**
 * Vitest Test Setup
 * Configures the test environment with necessary mocks and polyfills
 */

import { vi } from 'vitest'
import 'fake-indexeddb/auto'

// Mock sessionStorage
const sessionStorageMock = (() => {
  let store: Record<string, string> = {}
  return {
    getItem: (key: string) => store[key] || null,
    setItem: (key: string, value: string) => { store[key] = value.toString() },
    removeItem: (key: string) => { delete store[key] },
    clear: () => { store = {} },
    get length() { return Object.keys(store).length },
    key: (index: number) => Object.keys(store)[index] || null
  }
})()

Object.defineProperty(window, 'sessionStorage', {
  value: sessionStorageMock
})

// Mock Blob for file size calculations
if (typeof Blob === 'undefined') {
  global.Blob = class Blob {
    private content: BlobPart[]
    size: number
    type: string

    constructor(content: BlobPart[] = [], options: BlobPropertyBag = {}) {
      this.content = content
      this.type = options.type || ''
      this.size = content.reduce((acc, part) => {
        if (typeof part === 'string') return acc + part.length
        if (part instanceof ArrayBuffer) return acc + part.byteLength
        return acc
      }, 0)
    }
  } as any
}

// Reset mocks between tests
beforeEach(() => {
  sessionStorageMock.clear()
  vi.clearAllMocks()
})
