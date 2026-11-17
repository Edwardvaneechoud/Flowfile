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
  /** True while the options are the answer of the latest load and nothing invalidated them since. */
  hasAnswer(): boolean;
}

/**
 * A list that only the latest load may fill. Framework-free, so the drawers pass a reactive state
 * and the tests a plain object.
 */
export function createLatestLoader<T>(state: ListState<T>): LatestLoader<T> {
  let seq = 0;
  let answered = false;
  return {
    async load(fetcher, onError) {
      const token = ++seq;
      answered = false;
      state.options = [];
      state.loading = true;
      try {
        const result = await fetcher();
        if (token === seq) {
          state.options = result;
          answered = true;
        }
      } catch (error) {
        if (token === seq) onError?.(error);
      } finally {
        if (token === seq) state.loading = false;
      }
    },
    invalidate() {
      seq += 1;
      answered = false;
      state.options = [];
      state.loading = false;
    },
    hasAnswer() {
      return answered;
    },
  };
}
