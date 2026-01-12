/**
 * File Storage Utility
 *
 * Hybrid storage system for file contents:
 * - Small files (< 5MB): sessionStorage (fast, synchronous)
 * - Large files (>= 5MB): IndexedDB (no size limit, async)
 *
 * This design optimizes for performance while avoiding sessionStorage limits.
 */

const DB_NAME = 'flowfile_wasm_files';
const DB_VERSION = 1;
const STORE_NAME = 'fileContents';
const SIZE_THRESHOLD = 5 * 1024 * 1024; // 5MB in bytes

interface FileEntry {
  nodeId: number;
  content: string;
  size: number;
  timestamp: number;
}

class FileStorageManager {
  private db: IDBDatabase | null = null;
  private initPromise: Promise<void> | null = null;

  /**
   * Initialize IndexedDB connection
   */
  private async init(): Promise<void> {
    if (this.db) return;

    if (this.initPromise) {
      return this.initPromise;
    }

    this.initPromise = new Promise<void>((resolve, reject) => {
      const request = indexedDB.open(DB_NAME, DB_VERSION);

      request.onerror = () => {
        console.error('Failed to open IndexedDB:', request.error);
        reject(request.error);
      };

      request.onsuccess = () => {
        this.db = request.result;
        resolve();
      };

      request.onupgradeneeded = (event) => {
        const db = (event.target as IDBOpenDBRequest).result;

        // Create object store if it doesn't exist
        if (!db.objectStoreNames.contains(STORE_NAME)) {
          const objectStore = db.createObjectStore(STORE_NAME, { keyPath: 'nodeId' });
          objectStore.createIndex('timestamp', 'timestamp', { unique: false });
          objectStore.createIndex('size', 'size', { unique: false });
        }
      };
    });

    return this.initPromise;
  }

  /**
   * Store file content with automatic storage selection based on size
   */
  async setFileContent(nodeId: number, content: string): Promise<void> {
    const size = new Blob([content]).size;

    if (size < SIZE_THRESHOLD) {
      // Small file: use sessionStorage (synchronous, fast)
      return;
    }

    // Large file: use IndexedDB
    await this.init();

    return new Promise<void>((resolve, reject) => {
      if (!this.db) {
        reject(new Error('IndexedDB not initialized'));
        return;
      }

      const transaction = this.db.transaction([STORE_NAME], 'readwrite');
      const store = transaction.objectStore(STORE_NAME);

      const entry: FileEntry = {
        nodeId,
        content,
        size,
        timestamp: Date.now()
      };

      const request = store.put(entry);

      request.onsuccess = () => resolve();
      request.onerror = () => {
        console.error('Failed to store file in IndexedDB:', request.error);
        reject(request.error);
      };
    });
  }

  /**
   * Retrieve file content from IndexedDB
   */
  async getFileContent(nodeId: number): Promise<string | null> {
    await this.init();

    return new Promise<string | null>((resolve, reject) => {
      if (!this.db) {
        reject(new Error('IndexedDB not initialized'));
        return;
      }

      const transaction = this.db.transaction([STORE_NAME], 'readonly');
      const store = transaction.objectStore(STORE_NAME);
      const request = store.get(nodeId);

      request.onsuccess = () => {
        const entry = request.result as FileEntry | undefined;
        resolve(entry?.content || null);
      };

      request.onerror = () => {
        console.error('Failed to retrieve file from IndexedDB:', request.error);
        reject(request.error);
      };
    });
  }

  /**
   * Delete file content from IndexedDB
   */
  async deleteFileContent(nodeId: number): Promise<void> {
    await this.init();

    return new Promise<void>((resolve, reject) => {
      if (!this.db) {
        reject(new Error('IndexedDB not initialized'));
        return;
      }

      const transaction = this.db.transaction([STORE_NAME], 'readwrite');
      const store = transaction.objectStore(STORE_NAME);
      const request = store.delete(nodeId);

      request.onsuccess = () => resolve();
      request.onerror = () => {
        console.error('Failed to delete file from IndexedDB:', request.error);
        reject(request.error);
      };
    });
  }

  /**
   * Get all node IDs that have files stored in IndexedDB
   */
  async getAllNodeIds(): Promise<number[]> {
    await this.init();

    return new Promise<number[]>((resolve, reject) => {
      if (!this.db) {
        reject(new Error('IndexedDB not initialized'));
        return;
      }

      const transaction = this.db.transaction([STORE_NAME], 'readonly');
      const store = transaction.objectStore(STORE_NAME);
      const request = store.getAllKeys();

      request.onsuccess = () => {
        resolve(request.result as number[]);
      };

      request.onerror = () => {
        console.error('Failed to get all keys from IndexedDB:', request.error);
        reject(request.error);
      };
    });
  }

  /**
   * Get metadata about stored files (for diagnostics)
   */
  async getStorageStats(): Promise<{ totalFiles: number; totalSize: number }> {
    await this.init();

    return new Promise<{ totalFiles: number; totalSize: number }>((resolve, reject) => {
      if (!this.db) {
        reject(new Error('IndexedDB not initialized'));
        return;
      }

      const transaction = this.db.transaction([STORE_NAME], 'readonly');
      const store = transaction.objectStore(STORE_NAME);
      const request = store.getAll();

      request.onsuccess = () => {
        const entries = request.result as FileEntry[];
        const totalSize = entries.reduce((sum, entry) => sum + entry.size, 0);
        resolve({
          totalFiles: entries.length,
          totalSize
        });
      };

      request.onerror = () => {
        console.error('Failed to get storage stats:', request.error);
        reject(request.error);
      };
    });
  }

  /**
   * Clear all files from IndexedDB
   */
  async clearAll(): Promise<void> {
    await this.init();

    return new Promise<void>((resolve, reject) => {
      if (!this.db) {
        reject(new Error('IndexedDB not initialized'));
        return;
      }

      const transaction = this.db.transaction([STORE_NAME], 'readwrite');
      const store = transaction.objectStore(STORE_NAME);
      const request = store.clear();

      request.onsuccess = () => resolve();
      request.onerror = () => {
        console.error('Failed to clear IndexedDB:', request.error);
        reject(request.error);
      };
    });
  }

  /**
   * Check if a file should be stored in IndexedDB based on size
   */
  shouldUseIndexedDB(content: string): boolean {
    const size = new Blob([content]).size;
    return size >= SIZE_THRESHOLD;
  }
}

// Export singleton instance
export const fileStorage = new FileStorageManager();
export { SIZE_THRESHOLD };
