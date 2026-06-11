import { describe, it, expect } from 'vitest'
import { isReactive, reactive } from 'vue'
import {
  asFileContent,
  binaryContent,
  contentByteSize,
  detectFormat,
  isBinary,
  mimeFor,
  textContent
} from '../../src/types/file-content'

describe('FileContent helpers', () => {
  it('normalizes strings to text content', () => {
    expect(asFileContent('a,b\n1,2')).toEqual({ kind: 'text', data: 'a,b\n1,2' })
  })

  it('null-guards legacy persisted entries', () => {
    expect(asFileContent(null)).toEqual({ kind: 'text', data: '' })
    expect(asFileContent(undefined)).toEqual({ kind: 'text', data: '' })
  })

  it('passes existing content through unchanged', () => {
    const text = textContent('x')
    expect(asFileContent(text)).toBe(text)
    const bin = binaryContent(new Uint8Array([1, 2]), 'parquet')
    expect(asFileContent(bin)).toBe(bin)
  })

  it('isBinary narrows the union', () => {
    expect(isBinary(textContent('x'))).toBe(false)
    expect(isBinary(binaryContent(new Uint8Array(0), 'excel'))).toBe(true)
    expect(isBinary(undefined)).toBe(false)
  })

  it('measures byte size for both kinds', () => {
    expect(contentByteSize(textContent('abcd'))).toBe(4)
    expect(contentByteSize(binaryContent(new Uint8Array(10), 'parquet'))).toBe(10)
  })

  it('binary wrappers resist Vue reactivity (markRaw)', () => {
    const bin = binaryContent(new Uint8Array([1]), 'excel')
    const wrapped = reactive({ bin })
    expect(isReactive(wrapped.bin)).toBe(false)
  })

  it('detects formats from file names', () => {
    expect(detectFormat('sales.csv')).toBe('csv')
    expect(detectFormat('notes.txt')).toBe('csv')
    expect(detectFormat('book.XLSX')).toBe('excel')
    expect(detectFormat('book.xlsm')).toBe('excel')
    expect(detectFormat('data.parquet')).toBe('parquet')
    expect(detectFormat('data.pq')).toBe('parquet')
    expect(detectFormat('frame.arrow')).toBe('arrow-ipc')
    expect(detectFormat('no-extension')).toBe('csv')
  })

  it('maps formats to mime types', () => {
    expect(mimeFor('csv')).toBe('text/csv')
    expect(mimeFor('excel')).toContain('spreadsheetml')
    expect(mimeFor('parquet')).toBe('application/vnd.apache.parquet')
    expect(mimeFor('arrow-ipc')).toBe('application/vnd.apache.arrow.stream')
  })
})
