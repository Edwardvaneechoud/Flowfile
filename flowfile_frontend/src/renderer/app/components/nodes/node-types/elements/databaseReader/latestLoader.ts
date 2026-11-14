// A list that only the latest load may fill: framework-free, so the drawers pass a reactive state
// and the tests a plain object.

export interface ListState<T> {
  options: T[];
  loading: boolean;
}

export interface LatestLoader<T> {
  /**
   * Start a load: the options clear at once and only this call, if no newer one started, may
   * write the result, report its error and clear the loading flag.
   */
  load(fetcher: () => Promise<T[]>, onError?: (error: unknown) => void): Promise<void>;
  /** Drop the options and supersede any load in flight. */
  invalidate(): void;
}

export function createLatestLoader<T>(state: ListState<T>): LatestLoader<T> {
  let seq = 0;
  return {
    async load(fetcher, onError) {
      const token = ++seq;
      state.options = [];
      state.loading = true;
      try {
        const result = await fetcher();
        if (token === seq) state.options = result;
      } catch (error) {
        if (token === seq) onError?.(error);
      } finally {
        if (token === seq) state.loading = false;
      }
    },
    invalidate() {
      seq += 1;
      state.options = [];
      state.loading = false;
    },
  };
}
