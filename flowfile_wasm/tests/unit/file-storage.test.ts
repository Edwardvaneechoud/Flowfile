/**
 * File Storage Unit Tests
 * Tests for IndexedDB-based file storage system
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { fileStorage, SIZE_THRESHOLD } from '../../src/stores/file-storage'

describe('FileStorageManager', () => {
  // Clean up IndexedDB after each test
  afterEach(async () => {
    await fileStorage.clearAll()
    await fileStorage.clearAllDownloads()
  })

  describe('shouldUseIndexedDB', () => {
    it('should return false for small content', () => {
      const smallContent = 'a'.repeat(1000) // 1KB
      expect(fileStorage.shouldUseIndexedDB(smallContent)).toBe(false)
    })

    it('should return true for content at threshold', () => {
      const largeContent = 'a'.repeat(SIZE_THRESHOLD)
      expect(fileStorage.shouldUseIndexedDB(largeContent)).toBe(true)
    })

    it('should return true for content above threshold', () => {
      const largeContent = 'a'.repeat(SIZE_THRESHOLD + 1000)
      expect(fileStorage.shouldUseIndexedDB(largeContent)).toBe(true)
    })

    it('should return false for empty string', () => {
      expect(fileStorage.shouldUseIndexedDB('')).toBe(false)
    })
  })

  describe('File Content Storage', () => {
    it('should store and retrieve large file content', async () => {
      const nodeId = 1
      const content = 'x'.repeat(SIZE_THRESHOLD + 1000) // Above threshold

      await fileStorage.setFileContent(nodeId, content)
      const retrieved = await fileStorage.getFileContent(nodeId)

      expect(retrieved).toBe(content)
    })

    it('should not store small files in IndexedDB', async () => {
      const nodeId = 2
      const content = 'small content'

      await fileStorage.setFileContent(nodeId, content)
      const retrieved = await fileStorage.getFileContent(nodeId)

      // Small files are not stored in IndexedDB, so should return null
      expect(retrieved).toBeNull()
    })

    it('should return null for non-existent file', async () => {
      const retrieved = await fileStorage.getFileContent(999)
      expect(retrieved).toBeNull()
    })

    it('should overwrite existing file content', async () => {
      const nodeId = 3
      const content1 = 'y'.repeat(SIZE_THRESHOLD + 100)
      const content2 = 'z'.repeat(SIZE_THRESHOLD + 200)

      await fileStorage.setFileContent(nodeId, content1)
      await fileStorage.setFileContent(nodeId, content2)

      const retrieved = await fileStorage.getFileContent(nodeId)
      expect(retrieved).toBe(content2)
    })

    it('should delete file content', async () => {
      const nodeId = 4
      const content = 'w'.repeat(SIZE_THRESHOLD + 500)

      await fileStorage.setFileContent(nodeId, content)
      await fileStorage.deleteFileContent(nodeId)

      const retrieved = await fileStorage.getFileContent(nodeId)
      expect(retrieved).toBeNull()
    })

    it('should handle deleting non-existent file gracefully', async () => {
      // Should not throw
      await expect(fileStorage.deleteFileContent(999)).resolves.toBeUndefined()
    })
  })

  describe('getAllNodeIds', () => {
    it('should return empty array when no files stored', async () => {
      const nodeIds = await fileStorage.getAllNodeIds()
      expect(nodeIds).toEqual([])
    })

    it('should return all stored node IDs', async () => {
      const content = 'a'.repeat(SIZE_THRESHOLD + 100)

      await fileStorage.setFileContent(10, content)
      await fileStorage.setFileContent(20, content)
      await fileStorage.setFileContent(30, content)

      const nodeIds = await fileStorage.getAllNodeIds()

      expect(nodeIds).toHaveLength(3)
      expect(nodeIds).toContain(10)
      expect(nodeIds).toContain(20)
      expect(nodeIds).toContain(30)
    })
  })

  describe('getStorageStats', () => {
    it('should return zero stats when empty', async () => {
      const stats = await fileStorage.getStorageStats()

      expect(stats.totalFiles).toBe(0)
      expect(stats.totalSize).toBe(0)
    })

    it('should return correct stats for stored files', async () => {
      const content1 = 'a'.repeat(SIZE_THRESHOLD + 1000)
      const content2 = 'b'.repeat(SIZE_THRESHOLD + 2000)

      await fileStorage.setFileContent(1, content1)
      await fileStorage.setFileContent(2, content2)

      const stats = await fileStorage.getStorageStats()

      expect(stats.totalFiles).toBe(2)
      expect(stats.totalSize).toBeGreaterThan(SIZE_THRESHOLD * 2)
    })
  })

  describe('clearAll', () => {
    it('should clear all stored files', async () => {
      const content = 'c'.repeat(SIZE_THRESHOLD + 100)

      await fileStorage.setFileContent(1, content)
      await fileStorage.setFileContent(2, content)
      await fileStorage.setFileContent(3, content)

      await fileStorage.clearAll()

      const nodeIds = await fileStorage.getAllNodeIds()
      expect(nodeIds).toEqual([])

      const stats = await fileStorage.getStorageStats()
      expect(stats.totalFiles).toBe(0)
    })
  })

  describe('Download Content Storage', () => {
    it('should store and retrieve download content', async () => {
      const nodeId = 100
      const content = 'csv,data,here'
      const fileName = 'output.csv'
      const fileType = 'csv'
      const mimeType = 'text/csv'
      const rowCount = 3

      await fileStorage.setDownloadContent(nodeId, content, fileName, fileType, mimeType, rowCount)
      const retrieved = await fileStorage.getDownloadContent(nodeId)

      expect(retrieved).not.toBeNull()
      expect(retrieved?.content).toBe(content)
      expect(retrieved?.fileName).toBe(fileName)
      expect(retrieved?.fileType).toBe(fileType)
      expect(retrieved?.mimeType).toBe(mimeType)
      expect(retrieved?.rowCount).toBe(rowCount)
      expect(retrieved?.timestamp).toBeDefined()
    })

    it('should return null/undefined for non-existent download', async () => {
      const retrieved = await fileStorage.getDownloadContent(999)
      expect(retrieved).toBeFalsy()
    })

    it('should overwrite existing download content', async () => {
      const nodeId = 101

      await fileStorage.setDownloadContent(nodeId, 'old', 'old.csv', 'csv', 'text/csv', 1)
      await fileStorage.setDownloadContent(nodeId, 'new', 'new.csv', 'csv', 'text/csv', 2)

      const retrieved = await fileStorage.getDownloadContent(nodeId)

      expect(retrieved?.content).toBe('new')
      expect(retrieved?.fileName).toBe('new.csv')
      expect(retrieved?.rowCount).toBe(2)
    })

    it('should store parquet download content', async () => {
      const nodeId = 102
      const content = 'base64encodedparquetdata'

      await fileStorage.setDownloadContent(
        nodeId,
        content,
        'output.parquet',
        'parquet',
        'application/octet-stream',
        1000
      )

      const retrieved = await fileStorage.getDownloadContent(nodeId)

      expect(retrieved?.fileType).toBe('parquet')
      expect(retrieved?.mimeType).toBe('application/octet-stream')
    })

    it('should delete download content', async () => {
      const nodeId = 103

      await fileStorage.setDownloadContent(nodeId, 'data', 'file.csv', 'csv', 'text/csv', 1)
      await fileStorage.deleteDownloadContent(nodeId)

      const retrieved = await fileStorage.getDownloadContent(nodeId)
      expect(retrieved).toBeFalsy()
    })

    it('should handle deleting non-existent download gracefully', async () => {
      await expect(fileStorage.deleteDownloadContent(999)).resolves.toBeUndefined()
    })

    it('should clear all download contents', async () => {
      await fileStorage.setDownloadContent(1, 'a', 'a.csv', 'csv', 'text/csv', 1)
      await fileStorage.setDownloadContent(2, 'b', 'b.csv', 'csv', 'text/csv', 1)
      await fileStorage.setDownloadContent(3, 'c', 'c.csv', 'csv', 'text/csv', 1)

      await fileStorage.clearAllDownloads()

      expect(await fileStorage.getDownloadContent(1)).toBeFalsy()
      expect(await fileStorage.getDownloadContent(2)).toBeFalsy()
      expect(await fileStorage.getDownloadContent(3)).toBeFalsy()
    })
  })

  describe('Concurrent Operations', () => {
    it('should handle concurrent file writes', async () => {
      const content = 'd'.repeat(SIZE_THRESHOLD + 100)

      const promises = [
        fileStorage.setFileContent(1, content + '1'),
        fileStorage.setFileContent(2, content + '2'),
        fileStorage.setFileContent(3, content + '3')
      ]

      await Promise.all(promises)

      const nodeIds = await fileStorage.getAllNodeIds()
      expect(nodeIds).toHaveLength(3)
    })

    it('should handle concurrent reads', async () => {
      const content = 'e'.repeat(SIZE_THRESHOLD + 100)

      await fileStorage.setFileContent(1, content + '1')
      await fileStorage.setFileContent(2, content + '2')
      await fileStorage.setFileContent(3, content + '3')

      const [result1, result2, result3] = await Promise.all([
        fileStorage.getFileContent(1),
        fileStorage.getFileContent(2),
        fileStorage.getFileContent(3)
      ])

      expect(result1).toBe(content + '1')
      expect(result2).toBe(content + '2')
      expect(result3).toBe(content + '3')
    })
  })

  describe('SIZE_THRESHOLD constant', () => {
    it('should be 5MB', () => {
      expect(SIZE_THRESHOLD).toBe(5 * 1024 * 1024)
    })
  })
})
