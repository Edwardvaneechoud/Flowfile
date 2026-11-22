import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { effectScope, nextTick, ref } from "vue";
import type { SearchResponse } from "../../types/search.types";

const search = vi.fn();
vi.mock("../../api/search.api", () => ({
  SearchApi: { search: (...args: unknown[]) => search(...args) },
}));

const { errorMessage, useSearchResults } = await import("./useSearchResults");

const answer = (query: string): SearchResponse => ({
  query,
  tokens: [query],
  kinds: ["table"],
  hits: [],
  counts: { table: 0 },
  truncated: false,
  truncated_kinds: [],
});

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((r) => (resolve = r));
  return { promise, resolve };
}

describe("useSearchResults", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    search.mockReset();
  });
  afterEach(() => vi.useRealTimers());

  it("debounces, keeps only the newest answer and stays loading until it lands", async () => {
    const first = deferred<SearchResponse>();
    const second = deferred<SearchResponse>();
    search.mockReturnValueOnce(first.promise).mockReturnValueOnce(second.promise);
    const query = ref("ord");
    const scope = effectScope();
    const state = scope.run(() => useSearchResults(query, { debounceMs: 100 }))!;

    expect(state.loading.value).toBe(true);
    await vi.advanceTimersByTimeAsync(100);
    expect(search).toHaveBeenCalledTimes(1);
    const firstSignal = search.mock.calls[0][1].signal as AbortSignal;

    query.value = "orders";
    await nextTick();
    expect(firstSignal.aborted).toBe(true);
    first.resolve(answer("ord"));
    await Promise.resolve();
    expect(state.loading.value).toBe(true);
    expect(state.response.value).toBeNull();

    await vi.advanceTimersByTimeAsync(100);
    second.resolve(answer("orders"));
    await vi.runAllTimersAsync();
    expect(state.response.value?.query).toBe("orders");
    expect(state.loading.value).toBe(false);
    scope.stop();
  });

  it("clears without a request for a blank query", async () => {
    const query = ref("   ");
    const scope = effectScope();
    const state = scope.run(() => useSearchResults(query))!;
    await vi.runAllTimersAsync();
    expect(search).not.toHaveBeenCalled();
    expect(state.loading.value).toBe(false);
    expect(state.response.value).toBeNull();
    scope.stop();
  });

  it("reports a failed request and recovers on refresh", async () => {
    search.mockRejectedValueOnce({ response: { data: { detail: "boom" } } });
    search.mockResolvedValueOnce(answer("x"));
    const scope = effectScope();
    const state = scope.run(() => useSearchResults(ref("x"), { debounceMs: 10 }))!;
    await vi.runAllTimersAsync();
    expect(state.error.value).toBe("boom");
    state.refresh();
    await vi.runAllTimersAsync();
    expect(state.error.value).toBeNull();
    expect(state.response.value?.query).toBe("x");
    scope.stop();
  });
});

describe("errorMessage", () => {
  it("says what core refused, and stays generic when core gave nothing to show", () => {
    const refused = (status: number, detail: unknown) => ({
      response: { status, data: { detail } },
    });
    expect(errorMessage(refused(400, "No tokens"))).toBe("No tokens");
    expect(
      errorMessage(
        refused(422, [{ loc: ["query", "q"], msg: "String should have at most 200 characters" }]),
      ),
    ).toBe("String should have at most 200 characters");
    expect(errorMessage(refused(422, [{}]))).toBe("That search can't be run; try shorter words.");
    expect(errorMessage(new Error("Network Error"))).toBe("Search is unavailable right now.");
  });
});
