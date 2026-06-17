/**
 * File Storage Utility
 *
 * Hybrid storage system for file contents:
 * - Small files (< 5MB): sessionStorage (fast, synchronous)
 * - Large files (>= 5MB): IndexedDB (no size limit, async)
 *
 * Also handles download content storage for output nodes.
 *
 * This design optimizes for performance while avoiding sessionStorage limits.
 */

import { asFileContent, binaryContent, contentByteSize, textContent, type BinaryFormat, type FileContent } from '../types/file-content';

const DB_NAME = 'flowfile_wasm_files';
const DB_VERSION = 5;
const STORE_NAME = 'fileContents';
const DOWNLOAD_STORE_NAME = 'downloadContents';
const RECENT_FLOWS_STORE = 'recentFlows';
const RUN_HISTORY_STORE = 'runHistory';
const CATALOG_DATASETS_STORE = 'catalogDatasets';
const SAVED_FLOWS_STORE = 'savedFlows';
const SAVED_FLOWS_MIGRATED_KEY = 'flowfile_wasm_savedflows_migrated_v5';
const SIZE_THRESHOLD = 5 * 1024 * 1024; // 5MB in bytes

// Every object store the app expects. init() self-heals if any is missing.
const REQUIRED_STORES = [
  STORE_NAME,
  DOWNLOAD_STORE_NAME,
  RECENT_FLOWS_STORE,
  RUN_HISTORY_STORE,
  CATALOG_DATASETS_STORE,
  SAVED_FLOWS_STORE,
];

interface FileEntry {
  nodeId: number;
  /** Uint8Array survives IndexedDB structured clone natively — no base64. */
  content: string | Uint8Array;
  kind?: 'text' | 'binary';
  format?: BinaryFormat;
  size: number;
  timestamp: number;
}

interface DownloadEntry {
  nodeId: number;
  content: string | Uint8Array;
  contentKind?: 'text' | 'binary';
  fileName: string;
  fileType: string;
  mimeType: string;
  rowCount: number;
  timestamp: number;
}

/** Legacy v3 store, superseded by savedFlows; retained only as a migration source. */
interface RecentFlowEntry {
  id: string;
  name: string;
  savedAt: number;
  nodeCount: number;
  snapshot: unknown; // FlowfileData JSON
  fileContents?: Record<number, string>; // small input CSVs so reopen restores data
}

/** A per-run summary surfaced in the Catalog Run History tab + overview stats. */
interface RunHistoryEntry {
  id: string;
  flowId?: string; // stable library id, for joining a run back to a saved flow
  flowName: string;
  startedAt: number;
  durationMs: number;
  nodesTotal: number;
  nodesCompleted: number;
  success: boolean;
  error?: string | null;
}

/**
 * A flow saved to the persistent in-browser library (the WASM analogue of the
 * full app's catalog flow_registrations). Keyed by a stable uuid `id` so rename
 * is non-lossy and re-saving updates the same entry.
 */
interface SavedFlowEntry {
  id: string;
  name: string;
  description: string;
  createdAt: number;
  updatedAt: number;
  nodeCount: number;
  snapshot: unknown; // FlowfileData JSON
  fileContents?: Record<number, string>; // small input CSVs so reopen restores data
}

/** A CSV table uploaded directly in the Catalog (read by the Read-from-Catalog node). */
interface CatalogDatasetEntry {
  name: string;
  content: string;
}

class FileStorageManager {
  private db: IDBDatabase | null = null;
  private initPromise: Promise<void> | null = null;
  /** In-flight clearAll(); writes await it so a fire-and-forget clear can never delete a just-written file. */
  private pendingClear: Promise<void> | null = null;

  /** Create any object store that doesn't exist yet (additive, idempotent). */
  private createStores(db: IDBDatabase): void {
    if (!db.objectStoreNames.contains(STORE_NAME)) {
      const objectStore = db.createObjectStore(STORE_NAME, { keyPath: 'nodeId' });
      objectStore.createIndex('timestamp', 'timestamp', { unique: false });
      objectStore.createIndex('size', 'size', { unique: false });
    }
    if (!db.objectStoreNames.contains(DOWNLOAD_STORE_NAME)) {
      const downloadStore = db.createObjectStore(DOWNLOAD_STORE_NAME, { keyPath: 'nodeId' });
      downloadStore.createIndex('timestamp', 'timestamp', { unique: false });
    }
    // v3: recent flows (legacy, kept as a migration source) + run history.
    if (!db.objectStoreNames.contains(RECENT_FLOWS_STORE)) {
      const recentStore = db.createObjectStore(RECENT_FLOWS_STORE, { keyPath: 'id' });
      recentStore.createIndex('savedAt', 'savedAt', { unique: false });
    }
    if (!db.objectStoreNames.contains(RUN_HISTORY_STORE)) {
      const runStore = db.createObjectStore(RUN_HISTORY_STORE, { keyPath: 'id' });
      runStore.createIndex('startedAt', 'startedAt', { unique: false });
    }
    // v4: user-uploaded catalog datasets.
    if (!db.objectStoreNames.contains(CATALOG_DATASETS_STORE)) {
      db.createObjectStore(CATALOG_DATASETS_STORE, { keyPath: 'name' });
    }
    // v5: the persistent flow library (stable uuid identity).
    if (!db.objectStoreNames.contains(SAVED_FLOWS_STORE)) {
      const savedStore = db.createObjectStore(SAVED_FLOWS_STORE, { keyPath: 'id' });
      savedStore.createIndex('updatedAt', 'updatedAt', { unique: false });
    }
  }

  /**
   * Open the IndexedDB connection, self-healing any missing object store. A
   * store added after a DB_VERSION bump can leave a DB stuck at that version
   * WITHOUT the store (onupgradeneeded won't re-run) — so after opening we
   * verify every required store exists and, if one is missing, reopen at the
   * next version to trigger an upgrade that creates it. If the on-disk version
   * is already ahead of DB_VERSION, we reopen without a version to match it.
   */
  private async init(): Promise<void> {
    if (this.db) return;
    if (this.initPromise) return this.initPromise;

    const p = new Promise<void>((resolve, reject) => {
      const fail = (err: unknown) => {
        console.error('Failed to open IndexedDB:', err);
        reject(err instanceof Error ? err : new Error(String(err)));
      };

      // `healed` marks an open that follows a version bump + createStores(): if a
      // required store is STILL missing afterwards, we reject instead of looping.
      const openAt = (version?: number, healed = false) => {
        let request: IDBOpenDBRequest;
        try {
          request = version === undefined ? indexedDB.open(DB_NAME) : indexedDB.open(DB_NAME, version);
        } catch (err) {
          fail(err);
          return;
        }

        request.onupgradeneeded = () => {
          try {
            this.createStores(request.result);
          } catch (err) {
            // Aborts the versionchange transaction; request.onerror fires next.
            console.error('Failed to create IndexedDB stores:', err);
          }
        };

        // A version-bumped open is blocked when another tab/connection holds the
        // DB open at the old version. Don't hang forever — surface a recoverable
        // error (initPromise resets below, so a retry works once the tab closes).
        request.onblocked = () =>
          fail(new Error('Flowfile storage is open in another tab — close other Flowfile tabs and reload.'));

        request.onerror = () => {
          // VersionError: the DB drifted ahead of DB_VERSION (a prior self-heal).
          // Retry without a version to open at whatever version exists.
          if (version !== undefined) {
            openAt(undefined, healed);
            return;
          }
          fail(request.error);
        };

        request.onsuccess = () => {
          const db = request.result;
          if (REQUIRED_STORES.some((s) => !db.objectStoreNames.contains(s))) {
            if (healed) {
              // Already bumped + ran createStores yet a store is missing — give up
              // loudly rather than spin through endless version bumps.
              db.close();
              fail(new Error('IndexedDB is missing required object stores after an upgrade.'));
              return;
            }
            const next = db.version + 1;
            db.close();
            openAt(next, true);
            return;
          }
          this.db = db;
          resolve();
          // One-time backfill of the new library store from legacy recent flows.
          void this.migrateRecentFlowsToSavedFlows();
        };
      };

      openAt(DB_VERSION);
    });

    // Reset on failure so a later call can retry (e.g. after a blocking tab closes
    // or a transient open error clears) instead of replaying a cached rejection.
    this.initPromise = p;
    p.catch(() => {
      if (this.initPromise === p) this.initPromise = null;
    });

    return p;
  }

  /**
   * Store file content with automatic storage selection based on size.
   * Binary content always goes to IndexedDB regardless of size — it can't
   * ride the sessionStorage JSON path.
   */
  async setFileContent(nodeId: number, content: string | FileContent): Promise<void> {
    const fc = asFileContent(content);
    const size = contentByteSize(fc);

    if (fc.kind === 'text' && size < SIZE_THRESHOLD) {
      // Small text file: lives in sessionStorage (synchronous, fast)
      return;
    }

    await this.init();
    if (this.pendingClear) {
      await this.pendingClear.catch(() => {});
    }

    return new Promise<void>((resolve, reject) => {
      if (!this.db) {
        reject(new Error('IndexedDB not initialized'));
        return;
      }

      const transaction = this.db.transaction([STORE_NAME], 'readwrite');
      const store = transaction.objectStore(STORE_NAME);

      const entry: FileEntry = {
        nodeId,
        content: fc.data,
        kind: fc.kind,
        format: fc.kind === 'binary' ? fc.format : undefined,
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
   * Retrieve file content from IndexedDB. Legacy entries (plain string, no
   * kind) come back as text.
   */
  async getFileContent(nodeId: number): Promise<FileContent | null> {
    await this.init();

    return new Promise<FileContent | null>((resolve, reject) => {
      if (!this.db) {
        reject(new Error('IndexedDB not initialized'));
        return;
      }

      const transaction = this.db.transaction([STORE_NAME], 'readonly');
      const store = transaction.objectStore(STORE_NAME);
      const request = store.get(nodeId);

      request.onsuccess = () => {
        const entry = request.result as FileEntry | undefined;
        if (!entry || entry.content == null) {
          resolve(null);
        } else if (entry.kind === 'binary' && entry.content instanceof Uint8Array) {
          resolve(binaryContent(entry.content, entry.format ?? 'parquet'));
        } else {
          resolve(textContent(entry.content as string));
        }
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
    const clearPromise = (async () => {
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
    })();

    this.pendingClear = clearPromise;
    try {
      await clearPromise;
    } finally {
      if (this.pendingClear === clearPromise) {
        this.pendingClear = null;
      }
    }
  }

  /**
   * Check if a file should be stored in IndexedDB. Binary always does;
   * text only above the size threshold.
   */
  shouldUseIndexedDB(content: string | FileContent): boolean {
    const fc = asFileContent(content);
    return fc.kind === 'binary' || contentByteSize(fc) >= SIZE_THRESHOLD;
  }

  // Download Content Storage (for output nodes)

  /**
   * Store download content for an output node
   */
  async setDownloadContent(
    nodeId: number,
    content: string | Uint8Array,
    fileName: string,
    fileType: string,
    mimeType: string,
    rowCount: number
  ): Promise<void> {
    await this.init();

    return new Promise<void>((resolve, reject) => {
      if (!this.db) {
        reject(new Error('IndexedDB not initialized'));
        return;
      }

      const transaction = this.db.transaction([DOWNLOAD_STORE_NAME], 'readwrite');
      const store = transaction.objectStore(DOWNLOAD_STORE_NAME);

      const entry: DownloadEntry = {
        nodeId,
        content,
        contentKind: typeof content === 'string' ? 'text' : 'binary',
        fileName,
        fileType,
        mimeType,
        rowCount,
        timestamp: Date.now()
      };

      const request = store.put(entry);

      request.onsuccess = () => resolve();
      request.onerror = () => {
        console.error('Failed to store download content in IndexedDB:', request.error);
        reject(request.error);
      };
    });
  }

  /**
   * Retrieve download content for an output node
   */
  async getDownloadContent(nodeId: number): Promise<DownloadEntry | null> {
    await this.init();

    return new Promise<DownloadEntry | null>((resolve, reject) => {
      if (!this.db) {
        reject(new Error('IndexedDB not initialized'));
        return;
      }

      const transaction = this.db.transaction([DOWNLOAD_STORE_NAME], 'readonly');
      const store = transaction.objectStore(DOWNLOAD_STORE_NAME);
      const request = store.get(nodeId);

      request.onsuccess = () => {
        resolve(request.result as DownloadEntry | null);
      };

      request.onerror = () => {
        console.error('Failed to retrieve download content from IndexedDB:', request.error);
        reject(request.error);
      };
    });
  }

  /**
   * Delete download content for an output node
   */
  async deleteDownloadContent(nodeId: number): Promise<void> {
    await this.init();

    return new Promise<void>((resolve, reject) => {
      if (!this.db) {
        reject(new Error('IndexedDB not initialized'));
        return;
      }

      const transaction = this.db.transaction([DOWNLOAD_STORE_NAME], 'readwrite');
      const store = transaction.objectStore(DOWNLOAD_STORE_NAME);
      const request = store.delete(nodeId);

      request.onsuccess = () => resolve();
      request.onerror = () => {
        console.error('Failed to delete download content from IndexedDB:', request.error);
        reject(request.error);
      };
    });
  }

  /**
   * Clear all download contents
   */
  async clearAllDownloads(): Promise<void> {
    await this.init();

    return new Promise<void>((resolve, reject) => {
      if (!this.db) {
        reject(new Error('IndexedDB not initialized'));
        return;
      }

      const transaction = this.db.transaction([DOWNLOAD_STORE_NAME], 'readwrite');
      const store = transaction.objectStore(DOWNLOAD_STORE_NAME);
      const request = store.clear();

      request.onsuccess = () => resolve();
      request.onerror = () => {
        console.error('Failed to clear download contents from IndexedDB:', request.error);
        reject(request.error);
      };
    });
  }

  // ── Recent flows (Home page) ──────────────────────────────────────────────

  /** Generic helper: read all rows from a store sorted desc by a numeric field. */
  private getAllFromStore<T>(storeName: string): Promise<T[]> {
    return this.init().then(
      () =>
        new Promise<T[]>((resolve, reject) => {
          if (!this.db) {
            reject(new Error('IndexedDB not initialized'));
            return;
          }
          let tx: IDBTransaction;
          try {
            // transaction() throws NotFoundError synchronously if the store is
            // absent on this handle — reject cleanly instead of throwing through.
            tx = this.db.transaction([storeName], 'readonly');
          } catch (err) {
            reject(err instanceof Error ? err : new Error(String(err)));
            return;
          }
          const req = tx.objectStore(storeName).getAll();
          req.onsuccess = () => resolve(req.result as T[]);
          req.onerror = () => reject(req.error);
        }),
    );
  }

  async putRecentFlow(entry: RecentFlowEntry): Promise<void> {
    await this.init();
    return new Promise<void>((resolve, reject) => {
      if (!this.db) {
        reject(new Error('IndexedDB not initialized'));
        return;
      }
      const tx = this.db.transaction([RECENT_FLOWS_STORE], 'readwrite');
      const req = tx.objectStore(RECENT_FLOWS_STORE).put(entry);
      req.onsuccess = () => resolve();
      req.onerror = () => reject(req.error);
    });
  }

  async getAllRecentFlows(): Promise<RecentFlowEntry[]> {
    const all = await this.getAllFromStore<RecentFlowEntry>(RECENT_FLOWS_STORE);
    return all.sort((a, b) => b.savedAt - a.savedAt);
  }

  async getRecentFlow(id: string): Promise<RecentFlowEntry | null> {
    await this.init();
    return new Promise<RecentFlowEntry | null>((resolve, reject) => {
      if (!this.db) {
        reject(new Error('IndexedDB not initialized'));
        return;
      }
      const tx = this.db.transaction([RECENT_FLOWS_STORE], 'readonly');
      const req = tx.objectStore(RECENT_FLOWS_STORE).get(id);
      req.onsuccess = () => resolve((req.result as RecentFlowEntry) ?? null);
      req.onerror = () => reject(req.error);
    });
  }

  async deleteRecentFlow(id: string): Promise<void> {
    await this.init();
    return new Promise<void>((resolve, reject) => {
      if (!this.db) {
        reject(new Error('IndexedDB not initialized'));
        return;
      }
      const tx = this.db.transaction([RECENT_FLOWS_STORE], 'readwrite');
      const req = tx.objectStore(RECENT_FLOWS_STORE).delete(id);
      req.onsuccess = () => resolve();
      req.onerror = () => reject(req.error);
    });
  }

  /** Keep only the newest `max` recent flows. */
  async pruneRecentFlows(max = 8): Promise<void> {
    const all = await this.getAllRecentFlows();
    const toDelete = all.slice(max);
    await Promise.all(toDelete.map((e) => this.deleteRecentFlow(e.id)));
  }

  // ── Run history (Catalog) ─────────────────────────────────────────────────

  async putRun(entry: RunHistoryEntry): Promise<void> {
    await this.init();
    return new Promise<void>((resolve, reject) => {
      if (!this.db) {
        reject(new Error('IndexedDB not initialized'));
        return;
      }
      const tx = this.db.transaction([RUN_HISTORY_STORE], 'readwrite');
      const req = tx.objectStore(RUN_HISTORY_STORE).put(entry);
      req.onsuccess = () => resolve();
      req.onerror = () => reject(req.error);
    });
  }

  async getAllRuns(): Promise<RunHistoryEntry[]> {
    const all = await this.getAllFromStore<RunHistoryEntry>(RUN_HISTORY_STORE);
    return all.sort((a, b) => b.startedAt - a.startedAt);
  }

  /** Keep only the newest `max` runs. */
  async pruneRuns(max = 50): Promise<void> {
    const all = await this.getAllRuns();
    const toDelete = all.slice(max);
    if (!toDelete.length) return;
    await this.init();
    await new Promise<void>((resolve, reject) => {
      if (!this.db) {
        reject(new Error('IndexedDB not initialized'));
        return;
      }
      const tx = this.db.transaction([RUN_HISTORY_STORE], 'readwrite');
      const store = tx.objectStore(RUN_HISTORY_STORE);
      toDelete.forEach((e) => store.delete(e.id));
      tx.oncomplete = () => resolve();
      tx.onerror = () => reject(tx.error);
    });
  }

  async clearRuns(): Promise<void> {
    await this.init();
    return new Promise<void>((resolve, reject) => {
      if (!this.db) {
        reject(new Error('IndexedDB not initialized'));
        return;
      }
      const tx = this.db.transaction([RUN_HISTORY_STORE], 'readwrite');
      const req = tx.objectStore(RUN_HISTORY_STORE).clear();
      req.onsuccess = () => resolve();
      req.onerror = () => reject(req.error);
    });
  }

  // ── Catalog datasets (user uploads) ───────────────────────────────────────

  async putCatalogDataset(entry: CatalogDatasetEntry): Promise<void> {
    await this.init();
    return new Promise<void>((resolve, reject) => {
      if (!this.db) {
        reject(new Error('IndexedDB not initialized'));
        return;
      }
      let tx: IDBTransaction;
      try {
        // transaction() throws NotFoundError synchronously if the store is absent
        // on this handle — reject cleanly so the caller can surface the failure.
        tx = this.db.transaction([CATALOG_DATASETS_STORE], 'readwrite');
      } catch (err) {
        reject(err instanceof Error ? err : new Error(String(err)));
        return;
      }
      const req = tx.objectStore(CATALOG_DATASETS_STORE).put(entry);
      req.onsuccess = () => resolve();
      req.onerror = () => reject(req.error);
    });
  }

  async getAllCatalogDatasets(): Promise<CatalogDatasetEntry[]> {
    return this.getAllFromStore<CatalogDatasetEntry>(CATALOG_DATASETS_STORE);
  }

  async deleteCatalogDataset(name: string): Promise<void> {
    await this.init();
    return new Promise<void>((resolve, reject) => {
      if (!this.db) {
        reject(new Error('IndexedDB not initialized'));
        return;
      }
      let tx: IDBTransaction;
      try {
        tx = this.db.transaction([CATALOG_DATASETS_STORE], 'readwrite');
      } catch (err) {
        reject(err instanceof Error ? err : new Error(String(err)));
        return;
      }
      const req = tx.objectStore(CATALOG_DATASETS_STORE).delete(name);
      req.onsuccess = () => resolve();
      req.onerror = () => reject(req.error);
    });
  }

  // ── Saved flows (the persistent flow library) ─────────────────────────────

  async putSavedFlow(entry: SavedFlowEntry): Promise<void> {
    await this.init();
    return new Promise<void>((resolve, reject) => {
      if (!this.db) {
        reject(new Error('IndexedDB not initialized'));
        return;
      }
      const tx = this.db.transaction([SAVED_FLOWS_STORE], 'readwrite');
      const req = tx.objectStore(SAVED_FLOWS_STORE).put(entry);
      req.onsuccess = () => resolve();
      req.onerror = () => reject(req.error);
    });
  }

  async getSavedFlow(id: string): Promise<SavedFlowEntry | null> {
    await this.init();
    return new Promise<SavedFlowEntry | null>((resolve, reject) => {
      if (!this.db) {
        reject(new Error('IndexedDB not initialized'));
        return;
      }
      const tx = this.db.transaction([SAVED_FLOWS_STORE], 'readonly');
      const req = tx.objectStore(SAVED_FLOWS_STORE).get(id);
      req.onsuccess = () => resolve((req.result as SavedFlowEntry) ?? null);
      req.onerror = () => reject(req.error);
    });
  }

  /** All saved flows, newest-modified first. The library is never pruned. */
  async getAllSavedFlows(): Promise<SavedFlowEntry[]> {
    const all = await this.getAllFromStore<SavedFlowEntry>(SAVED_FLOWS_STORE);
    return all.sort((a, b) => b.updatedAt - a.updatedAt);
  }

  async deleteSavedFlow(id: string): Promise<void> {
    await this.init();
    return new Promise<void>((resolve, reject) => {
      if (!this.db) {
        reject(new Error('IndexedDB not initialized'));
        return;
      }
      const tx = this.db.transaction([SAVED_FLOWS_STORE], 'readwrite');
      const req = tx.objectStore(SAVED_FLOWS_STORE).delete(id);
      req.onsuccess = () => resolve();
      req.onerror = () => reject(req.error);
    });
  }

  /** Clone a saved flow under a new id + name (Save-As semantics). */
  async duplicateSavedFlow(id: string, newId: string, newName: string): Promise<SavedFlowEntry | null> {
    const source = await this.getSavedFlow(id);
    if (!source) return null;
    const now = Date.now();
    const clone: SavedFlowEntry = { ...source, id: newId, name: newName, createdAt: now, updatedAt: now };
    await this.putSavedFlow(clone);
    return clone;
  }

  /**
   * One-time backfill: copy legacy `recentFlows` rows into `savedFlows`,
   * synthesizing a stable uuid + timestamps. Guarded by a localStorage flag so
   * it runs once, and skipped if the library already has entries. Best-effort.
   */
  private async migrateRecentFlowsToSavedFlows(): Promise<void> {
    try {
      if (localStorage.getItem(SAVED_FLOWS_MIGRATED_KEY)) return;
      if (!this.db?.objectStoreNames.contains(RECENT_FLOWS_STORE)) {
        localStorage.setItem(SAVED_FLOWS_MIGRATED_KEY, '1');
        return;
      }
      const existing = await this.getAllFromStore<SavedFlowEntry>(SAVED_FLOWS_STORE);
      if (existing.length === 0) {
        const legacy = await this.getAllFromStore<RecentFlowEntry>(RECENT_FLOWS_STORE);
        for (const r of legacy) {
          const id = globalThis.crypto?.randomUUID?.() ?? `mig-${r.id}-${r.savedAt}`;
          await this.putSavedFlow({
            id,
            name: r.name,
            description: '',
            createdAt: r.savedAt,
            updatedAt: r.savedAt,
            nodeCount: r.nodeCount,
            snapshot: r.snapshot,
            fileContents: r.fileContents,
          });
        }
      }
      localStorage.setItem(SAVED_FLOWS_MIGRATED_KEY, '1');
    } catch (e) {
      console.warn('[file-storage] savedFlows migration failed:', e);
    }
  }
}

export const fileStorage = new FileStorageManager();
export { SIZE_THRESHOLD };
export type { DownloadEntry, RecentFlowEntry, RunHistoryEntry, CatalogDatasetEntry, SavedFlowEntry };
