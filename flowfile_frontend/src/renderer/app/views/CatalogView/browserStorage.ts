// Storage seam for the catalog's persisted view state: a test passes its own store, the app
// resolves the browser's, and a runtime without one (or one that refuses access) yields null.

export interface StorageLike {
  getItem(key: string): string | null;
  setItem(key: string, value: string): void;
  removeItem(key: string): void;
}

export type BrowserStorageKind = "localStorage" | "sessionStorage";

export const resolveStorage = (
  storage: StorageLike | null | undefined,
  kind: BrowserStorageKind,
): StorageLike | null => {
  if (storage) return storage;
  try {
    return (globalThis as Partial<Record<BrowserStorageKind, StorageLike>>)[kind] ?? null;
  } catch {
    return null;
  }
};
