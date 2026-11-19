/** The intervals an author can save and a viewer can pick, in seconds; null is off. */
export const AUTO_REFRESH_CHOICES: readonly (number | null)[] = [null, 60, 300, 900, 1800, 3600];

/** Core refuses a saved interval under this. */
export const AUTO_REFRESH_MIN_SECONDS = 60;

/** "Off", "1 min", "15 min", "1 hour". */
export function autoRefreshLabel(seconds: number | null | undefined): string {
  if (seconds == null) return "Off";
  if (seconds % 3600 === 0) return seconds === 3600 ? "1 hour" : `${seconds / 3600} hours`;
  if (seconds % 60 === 0) return `${seconds / 60} min`;
  return `${seconds} s`;
}

/** A saved interval as the viewer uses it: anything under the minimum or not a number is off. */
export function savedAutoRefresh(seconds: unknown): number | null {
  return typeof seconds === "number" &&
    Number.isFinite(seconds) &&
    seconds >= AUTO_REFRESH_MIN_SECONDS
    ? seconds
    : null;
}

export interface AutoRefreshTimers {
  setInterval: (fn: () => void, ms: number) => unknown;
  clearInterval: (handle: unknown) => void;
}

export interface AutoRefreshOptions {
  /** The refresh the Refresh button runs; a rejection is the caller's to report. */
  refresh: () => Promise<unknown>;
  /** True while a refresh started elsewhere (the button) is still running. */
  isBusy?: () => boolean;
  /** True while the page is hidden (`document.hidden`). */
  isHidden: () => boolean;
  timers?: AutoRefreshTimers;
}

export interface AutoRefresher {
  /** Starts, changes or (null) stops the interval; the first tick is one interval away. */
  setInterval(seconds: number | null): void;
  /** Call on `visibilitychange`: back on screen after a missed tick refreshes once. */
  visibilityChanged(): void;
  /** Ends the timer for good. */
  stop(): void;
  readonly seconds: number | null;
}

const defaultTimers: AutoRefreshTimers = {
  setInterval: (fn, ms) => globalThis.setInterval(fn, ms),
  clearInterval: (handle) => globalThis.clearInterval(handle as ReturnType<typeof setInterval>),
};

/**
 * Runs `refresh` every interval. A tick is skipped while a refresh is still running, so requests
 * never stack; a tick while the page is hidden is only noted, and the page refreshes once when it
 * comes back, the interval then starting over from that refresh.
 */
export function createAutoRefresher(options: AutoRefreshOptions): AutoRefresher {
  const timers = options.timers ?? defaultTimers;
  let seconds: number | null = null;
  let handle: unknown = null;
  let running = false;
  let missed = false;
  let stopped = false;

  const clear = () => {
    if (handle != null) timers.clearInterval(handle);
    handle = null;
  };

  const busy = () => running || (options.isBusy?.() ?? false);

  const run = () => {
    running = true;
    let pending: Promise<unknown>;
    try {
      pending = options.refresh();
    } catch (err) {
      pending = Promise.reject(err);
    }
    pending
      .catch(() => undefined)
      .finally(() => {
        running = false;
      });
  };

  const tick = () => {
    if (stopped) return;
    if (options.isHidden()) {
      missed = true;
      return;
    }
    if (busy()) return;
    run();
  };

  const start = () => {
    clear();
    if (stopped || seconds == null) return;
    handle = timers.setInterval(tick, seconds * 1000);
  };

  return {
    setInterval(next) {
      if (stopped) return;
      seconds = next;
      missed = false;
      start();
    },
    visibilityChanged() {
      if (stopped || seconds == null || options.isHidden() || !missed) return;
      missed = false;
      if (!busy()) run();
      start();
    },
    stop() {
      stopped = true;
      clear();
    },
    get seconds() {
      return seconds;
    },
  };
}
