// Generic debounced localStorage draft with a versioned JSON envelope (node-designer pattern).

export const LOCAL_DRAFT_DEBOUNCE_MS = 500;

interface StorageLike {
  getItem(key: string): string | null;
  setItem(key: string, value: string): void;
  removeItem(key: string): void;
}

const resolveStorage = (): StorageLike | null => {
  try {
    return (globalThis as { localStorage?: StorageLike }).localStorage ?? null;
  } catch {
    return null;
  }
};

export interface LocalDraftOptions<T> {
  key: string;
  version: number;
  serialize: () => T | null;
}

export interface LocalDraft<T> {
  scheduleWrite(): void;
  writeNow(): void;
  peek(): { savedAt: number; data: T } | null;
  discard(): void;
  dispose(): void;
}

export function createLocalDraft<T>({
  key,
  version,
  serialize,
}: LocalDraftOptions<T>): LocalDraft<T> {
  let timer: ReturnType<typeof setTimeout> | null = null;
  let disposed = false;

  const cancelTimer = () => {
    if (timer !== null) {
      clearTimeout(timer);
      timer = null;
    }
  };

  const removeQuietly = (storage: StorageLike) => {
    try {
      storage.removeItem(key);
    } catch {
      // storage unavailable
    }
  };

  const writeNow = () => {
    cancelTimer();
    if (disposed) return;
    const storage = resolveStorage();
    if (!storage) return;
    try {
      const data = serialize();
      if (data === null) {
        storage.removeItem(key);
      } else {
        storage.setItem(key, JSON.stringify({ version, savedAt: Date.now(), data }));
      }
    } catch {
      // quota exceeded or storage disabled
    }
  };

  const scheduleWrite = () => {
    if (disposed) return;
    cancelTimer();
    timer = setTimeout(() => {
      timer = null;
      writeNow();
    }, LOCAL_DRAFT_DEBOUNCE_MS);
  };

  const peek = (): { savedAt: number; data: T } | null => {
    const storage = resolveStorage();
    if (!storage) return null;
    try {
      const raw = storage.getItem(key);
      if (raw === null) return null;
      const parsed: unknown = JSON.parse(raw);
      const envelope =
        typeof parsed === "object" && parsed !== null
          ? (parsed as { version?: unknown; savedAt?: unknown; data?: unknown })
          : null;
      if (
        envelope === null ||
        envelope.version !== version ||
        typeof envelope.savedAt !== "number"
      ) {
        removeQuietly(storage);
        return null;
      }
      return { savedAt: envelope.savedAt, data: envelope.data as T };
    } catch {
      removeQuietly(storage);
      return null;
    }
  };

  const discard = () => {
    cancelTimer();
    const storage = resolveStorage();
    if (storage) removeQuietly(storage);
  };

  const dispose = () => {
    disposed = true;
    cancelTimer();
  };

  return { scheduleWrite, writeNow, peek, discard, dispose };
}
