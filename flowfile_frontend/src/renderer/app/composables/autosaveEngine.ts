// Pure autosave state machine; DOM/Vue-free (the Vue seam lives in useAutosave.ts).

export const AUTOSAVE_DEBOUNCE_MS = 5000;
export const AUTOSAVE_MAX_WAIT_MS = 15000;
export const AUTOSAVE_RETRY_BASE_MS = 3000;
export const AUTOSAVE_RETRY_MAX_MS = 30000;
export const AUTOSAVE_REQUEST_TIMEOUT_MS = 15000;
// How long changes may sit unsaved before the status dot turns warning-orange.
export const AUTOSAVE_STALE_UNSAVED_MS = 30000;

export type AutosaveStatus = "idle" | "dirty" | "saving" | "retrying" | "conflict" | "blocked";
export type BlockKind = "forbidden" | "gone" | "auth";
export type SaveReason = "debounce" | "maxwait" | "retry" | "flush";

export interface AutosaveState {
  status: AutosaveStatus;
  lastSavedAt: number | null;
  lastError: string | null;
  blockKind: BlockKind | null;
  retryCount: number;
  /** When the oldest still-unsaved change was made; null when everything is saved. */
  dirtySince: number | null;
}

export interface AutosaveSnapshot<P> {
  payload: P;
  serialized: string;
}

export type AutosaveErrorKind =
  | "conflict"
  | "blocked-forbidden"
  | "blocked-gone"
  | "blocked-auth"
  | "retryable"
  | "rejected";

export function classifyAutosaveError(err: unknown): AutosaveErrorKind {
  const response =
    typeof err === "object" && err !== null
      ? (err as { response?: { status?: unknown; data?: unknown } }).response
      : undefined;
  const status = typeof response?.status === "number" ? response.status : null;
  if (status === null) return "retryable";
  if (status === 409) {
    const detail =
      typeof response?.data === "object" && response.data !== null
        ? (response.data as { detail?: unknown }).detail
        : undefined;
    const code =
      typeof detail === "object" && detail !== null
        ? (detail as { error?: unknown }).error
        : undefined;
    return code === "stale_write" ? "conflict" : "rejected";
  }
  if (status === 403) return "blocked-forbidden";
  if (status === 404 || status === 410) return "blocked-gone";
  if (status === 401) return "blocked-auth";
  if (status === 408 || status === 429 || status >= 500) return "retryable";
  if (status >= 400 && status < 500) return "rejected";
  return "retryable";
}

export interface AutosaveEngineOptions<P> {
  getSnapshot: () => AutosaveSnapshot<P> | null | Promise<AutosaveSnapshot<P> | null>;
  save: (payload: P, ctx: { reason: SaveReason }) => Promise<void>;
  onChange?: (state: AutosaveState) => void;
  debounceMs?: number;
  maxWaitMs?: number;
}

export interface AutosaveEngine {
  notifyChange(): void;
  flush(): Promise<boolean>;
  markSaved(serialized: string): void;
  reset(baselineSerialized: string): void;
  resolveConflictReloaded(baselineSerialized: string): void;
  /** Out-of-band writes (revert, overwrite) hit a stale-write 409 themselves. */
  enterConflict(message?: string): void;
  suspend(): void;
  resume(): void;
  isDirty(): boolean;
  state(): AutosaveState;
  dispose(): void;
}

export function createAutosaveEngine<P>(opts: AutosaveEngineOptions<P>): AutosaveEngine {
  const debounceMs = opts.debounceMs ?? AUTOSAVE_DEBOUNCE_MS;
  const maxWaitMs = opts.maxWaitMs ?? AUTOSAVE_MAX_WAIT_MS;

  let status: AutosaveStatus = "idle";
  let lastSavedAt: number | null = null;
  let lastError: string | null = null;
  let blockKind: BlockKind | null = null;
  let retryCount = 0;
  let dirtySince: number | null = null;

  let baseline: string | null = null;
  let pendingChanges = false;
  let redirty = false;
  let suspended = false;
  let disposed = false;
  // Bumped by markSaved/reset/resolveConflictReloaded/dispose to disown in-flight attempts.
  let epoch = 0;
  let inFlight: Promise<void> | null = null;

  let debounceTimer: ReturnType<typeof setTimeout> | null = null;
  let maxWaitTimer: ReturnType<typeof setTimeout> | null = null;
  let retryTimer: ReturnType<typeof setTimeout> | null = null;

  const snapshotState = (): AutosaveState => ({
    status,
    lastSavedAt,
    lastError,
    blockKind,
    retryCount,
    dirtySince,
  });

  const emit = () => opts.onChange?.(snapshotState());

  const clearDebounce = () => {
    if (debounceTimer !== null) {
      clearTimeout(debounceTimer);
      debounceTimer = null;
    }
  };
  const clearMaxWait = () => {
    if (maxWaitTimer !== null) {
      clearTimeout(maxWaitTimer);
      maxWaitTimer = null;
    }
  };
  const clearRetry = () => {
    if (retryTimer !== null) {
      clearTimeout(retryTimer);
      retryTimer = null;
    }
  };
  const clearAllTimers = () => {
    clearDebounce();
    clearMaxWait();
    clearRetry();
  };

  const onTimerFire = (reason: SaveReason) => {
    if (disposed || suspended || inFlight !== null) return;
    void runAttempt(reason);
  };

  const armDebounce = () => {
    clearDebounce();
    debounceTimer = setTimeout(() => {
      debounceTimer = null;
      onTimerFire("debounce");
    }, debounceMs);
  };
  const armMaxWaitIfNeeded = () => {
    if (maxWaitTimer !== null) return;
    maxWaitTimer = setTimeout(() => {
      maxWaitTimer = null;
      onTimerFire("maxwait");
    }, maxWaitMs);
  };
  const armRetry = (delayMs: number) => {
    clearRetry();
    retryTimer = setTimeout(() => {
      retryTimer = null;
      onTimerFire("retry");
    }, delayMs);
  };

  const enterDirty = () => {
    status = "dirty";
    if (!suspended) {
      armDebounce();
      armMaxWaitIfNeeded();
    }
  };

  const errorMessage = (err: unknown): string => {
    if (typeof err === "object" && err !== null && "message" in err) {
      const message = (err as { message?: unknown }).message;
      if (typeof message === "string" && message.length > 0) return message;
    }
    return String(err);
  };

  const finishAttempt = () => {
    if (pendingChanges) {
      enterDirty();
    } else {
      status = "idle";
    }
    emit();
  };

  const handleFailure = (err: unknown) => {
    pendingChanges = true;
    if (dirtySince === null) dirtySince = Date.now();
    lastError = errorMessage(err);
    const kind = classifyAutosaveError(err);
    if (kind === "conflict") {
      status = "conflict";
      clearAllTimers();
      emit();
      return;
    }
    if (kind === "blocked-forbidden" || kind === "blocked-gone" || kind === "blocked-auth") {
      status = "blocked";
      blockKind =
        kind === "blocked-forbidden" ? "forbidden" : kind === "blocked-gone" ? "gone" : "auth";
      clearAllTimers();
      emit();
      return;
    }
    if (kind === "retryable") {
      status = "retrying";
      const delayMs = Math.min(AUTOSAVE_RETRY_BASE_MS * 2 ** retryCount, AUTOSAVE_RETRY_MAX_MS);
      retryCount += 1;
      if (!suspended) armRetry(delayMs);
      emit();
      return;
    }
    status = "dirty";
    emit();
  };

  const runAttempt = (reason: SaveReason): Promise<void> => {
    const attemptEpoch = epoch;
    clearAllTimers();
    redirty = false;
    status = "saving";
    emit();
    const attempt = (async () => {
      try {
        const snapshot = await Promise.resolve(opts.getSnapshot());
        if (disposed || epoch !== attemptEpoch) return;
        if (snapshot === null) {
          status = "dirty";
          emit();
          return;
        }
        if (snapshot.serialized === baseline) {
          pendingChanges = redirty;
          dirtySince = redirty ? Date.now() : null;
          finishAttempt();
          return;
        }
        await opts.save(snapshot.payload, { reason });
        if (disposed || epoch !== attemptEpoch) return;
        baseline = snapshot.serialized;
        lastSavedAt = Date.now();
        retryCount = 0;
        lastError = null;
        blockKind = null;
        pendingChanges = redirty;
        dirtySince = redirty ? Date.now() : null;
        finishAttempt();
      } catch (err) {
        if (disposed || epoch !== attemptEpoch) return;
        handleFailure(err);
      }
    })();
    inFlight = attempt;
    void attempt.finally(() => {
      if (inFlight === attempt) inFlight = null;
    });
    return attempt;
  };

  const notifyChange = () => {
    if (disposed) return;
    if (!pendingChanges) dirtySince = Date.now();
    pendingChanges = true;
    if (status === "conflict" || status === "blocked") return;
    if (inFlight !== null || status === "saving") {
      redirty = true;
      return;
    }
    if (status === "retrying") {
      clearRetry();
      enterDirty();
      emit();
      return;
    }
    if (suspended) {
      if (status !== "dirty") {
        status = "dirty";
        emit();
      }
      return;
    }
    const wasDirty = status === "dirty";
    enterDirty();
    if (!wasDirty) emit();
  };

  const flush = async (): Promise<boolean> => {
    if (!disposed) {
      clearAllTimers();
      while (inFlight !== null) {
        await inFlight;
      }
      if (pendingChanges && !suspended && status !== "conflict" && status !== "blocked") {
        await runAttempt("flush");
      }
    }
    return status === "idle";
  };

  const adoptBaseline = (serialized: string, touchLastSavedAt: boolean) => {
    if (disposed) return;
    epoch += 1;
    inFlight = null;
    clearAllTimers();
    baseline = serialized;
    pendingChanges = false;
    redirty = false;
    status = "idle";
    lastError = null;
    blockKind = null;
    retryCount = 0;
    dirtySince = null;
    if (touchLastSavedAt) lastSavedAt = Date.now();
    emit();
  };

  return {
    notifyChange,
    flush,
    markSaved: (serialized: string) => adoptBaseline(serialized, true),
    reset: (baselineSerialized: string) => adoptBaseline(baselineSerialized, false),
    resolveConflictReloaded: (baselineSerialized: string) =>
      adoptBaseline(baselineSerialized, false),
    enterConflict: (message?: string) => {
      if (disposed) return;
      pendingChanges = true;
      if (dirtySince === null) dirtySince = Date.now();
      if (message) lastError = message;
      status = "conflict";
      clearAllTimers();
      emit();
    },
    suspend: () => {
      if (!disposed) suspended = true;
    },
    resume: () => {
      if (disposed || !suspended) return;
      suspended = false;
      if (inFlight !== null) return;
      if (pendingChanges && status !== "conflict" && status !== "blocked") {
        clearRetry();
        const wasDirty = status === "dirty";
        enterDirty();
        if (!wasDirty) emit();
      }
    },
    isDirty: () => pendingChanges,
    state: snapshotState,
    dispose: () => {
      disposed = true;
      epoch += 1;
      inFlight = null;
      clearAllTimers();
    },
  };
}
