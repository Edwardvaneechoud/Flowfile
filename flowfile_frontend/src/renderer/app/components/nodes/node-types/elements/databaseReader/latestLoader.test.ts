import { describe, expect, it, vi } from "vitest";

import { shouldLoadTables } from "./databaseChoice";
import { createLatestLoader, type ListState } from "./latestLoader";

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

const emptyState = (): ListState<string> => ({ options: [], loading: false });

describe("createLatestLoader", () => {
  it("keeps the newer answer when an older one resolves after it", async () => {
    const state = emptyState();
    const loader = createLatestLoader(state);
    const older = deferred<string[]>();
    const newer = deferred<string[]>();

    const first = loader.load(() => older.promise);
    const second = loader.load(() => newer.promise);
    newer.resolve(["ORDERS"]);
    await second;
    older.resolve(["PUBLIC.ORDERS", "PUBLIC.CUSTOMERS"]);
    await first;

    expect(state.options).toEqual(["ORDERS"]);
    expect(state.loading).toBe(false);
  });

  it("stays loading until the latest load settles", async () => {
    const state = emptyState();
    const loader = createLatestLoader(state);
    const older = deferred<string[]>();
    const newer = deferred<string[]>();

    const first = loader.load(() => older.promise);
    const second = loader.load(() => newer.promise);
    older.resolve(["STALE"]);
    await first;

    expect(state.loading).toBe(true);
    expect(state.options).toEqual([]);

    newer.resolve(["FRESH"]);
    await second;
    expect(state.loading).toBe(false);
    expect(state.options).toEqual(["FRESH"]);
  });

  it("clears the options as soon as a load starts", () => {
    const state: ListState<string> = { options: ["OLD"], loading: false };
    const loader = createLatestLoader(state);
    loader.load(() => deferred<string[]>().promise);
    expect(state.options).toEqual([]);
    expect(state.loading).toBe(true);
  });

  it("says nothing about an error from a superseded load", async () => {
    const state = emptyState();
    const loader = createLatestLoader(state);
    const onError = vi.fn();
    const older = deferred<string[]>();
    const newer = deferred<string[]>();

    const first = loader.load(() => older.promise, onError);
    const second = loader.load(() => newer.promise, onError);
    newer.resolve(["ORDERS"]);
    await second;
    older.reject(new Error("timed out"));
    await first;

    expect(onError).not.toHaveBeenCalled();
    expect(state.options).toEqual(["ORDERS"]);
  });

  it("reports an error from the latest load", async () => {
    const state = emptyState();
    const loader = createLatestLoader(state);
    const onError = vi.fn();
    const failure = new Error("no access");

    await loader.load(() => Promise.reject(failure), onError);

    expect(onError).toHaveBeenCalledWith(failure);
    expect(state.loading).toBe(false);
  });

  it("drops a load in flight on invalidate", async () => {
    const state = emptyState();
    const loader = createLatestLoader(state);
    const pending = deferred<string[]>();

    const load = loader.load(() => pending.promise);
    loader.invalidate();
    expect(state.loading).toBe(false);
    pending.resolve(["STALE"]);
    await load;

    expect(state.options).toEqual([]);
    expect(state.loading).toBe(false);
  });
});

describe("table list for a dialect that browses databases", () => {
  // The decision useDatabaseChoice.loadTables makes before it calls core.
  const loadTables = (browses: boolean, schema: string | undefined, state: ListState<string>) => {
    const loader = createLatestLoader(state);
    const fetchTables = vi.fn(() => Promise.resolve(["PUBLIC.ORDERS"]));
    if (shouldLoadTables(browses, schema)) loader.load(fetchTables);
    else loader.invalidate();
    return fetchTables;
  };

  it("is not requested until a schema is chosen", () => {
    const state: ListState<string> = { options: ["STALE"], loading: true };
    const fetchTables = loadTables(true, undefined, state);
    expect(fetchTables).not.toHaveBeenCalled();
    expect(state).toEqual({ options: [], loading: false });
  });

  it("is requested once a schema is chosen, and always for other dialects", () => {
    expect(loadTables(true, "PUBLIC", emptyState())).toHaveBeenCalledTimes(1);
    expect(loadTables(false, undefined, emptyState())).toHaveBeenCalledTimes(1);
  });
});
