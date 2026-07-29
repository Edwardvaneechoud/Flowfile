// Unit tests for the generic debounced localStorage draft. Node env: a mock
// Storage is installed on globalThis (node-designer-store test pattern).

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { LOCAL_DRAFT_DEBOUNCE_MS, createLocalDraft } from "./localDraft";

class MemoryStorage {
  data = new Map<string, string>();
  getItem(key: string): string | null {
    return this.data.has(key) ? (this.data.get(key) as string) : null;
  }
  setItem(key: string, value: string): void {
    this.data.set(key, value);
  }
  removeItem(key: string): void {
    this.data.delete(key);
  }
}

const KEY = "test.autosave.draft";
const g = globalThis as { localStorage?: unknown };

let localStorageMock: MemoryStorage;

beforeEach(() => {
  vi.useFakeTimers();
  localStorageMock = new MemoryStorage();
  g.localStorage = localStorageMock;
});

afterEach(() => {
  vi.useRealTimers();
  delete g.localStorage;
});

const makeDraft = (serialize: () => { n: number } | null, version = 1) =>
  createLocalDraft<{ n: number }>({ key: KEY, version, serialize });

describe("scheduleWrite", () => {
  it("writes the versioned envelope at exactly the debounce delay", () => {
    const draft = makeDraft(() => ({ n: 7 }));
    draft.scheduleWrite();
    vi.advanceTimersByTime(LOCAL_DRAFT_DEBOUNCE_MS - 1);
    expect(localStorageMock.getItem(KEY)).toBeNull();
    vi.advanceTimersByTime(1);
    const raw = localStorageMock.getItem(KEY);
    expect(raw).not.toBeNull();
    const parsed = JSON.parse(raw as string);
    expect(parsed.version).toBe(1);
    expect(typeof parsed.savedAt).toBe("number");
    expect(parsed.data).toEqual({ n: 7 });
  });

  it("coalesces rapid schedules into a single trailing write", () => {
    const spy = vi.spyOn(localStorageMock, "setItem");
    const draft = makeDraft(() => ({ n: 1 }));
    draft.scheduleWrite();
    vi.advanceTimersByTime(300);
    draft.scheduleWrite();
    vi.advanceTimersByTime(LOCAL_DRAFT_DEBOUNCE_MS - 1);
    expect(spy).not.toHaveBeenCalled();
    vi.advanceTimersByTime(1);
    expect(spy).toHaveBeenCalledTimes(1);
  });

  it("removes the key when serialize returns null (clean)", () => {
    localStorageMock.setItem(KEY, "stale");
    const draft = makeDraft(() => null);
    draft.scheduleWrite();
    vi.advanceTimersByTime(LOCAL_DRAFT_DEBOUNCE_MS);
    expect(localStorageMock.getItem(KEY)).toBeNull();
  });

  it("swallows a quota throw from setItem", () => {
    vi.spyOn(localStorageMock, "setItem").mockImplementation(() => {
      throw new Error("QuotaExceededError");
    });
    const draft = makeDraft(() => ({ n: 1 }));
    draft.scheduleWrite();
    expect(() => vi.advanceTimersByTime(LOCAL_DRAFT_DEBOUNCE_MS)).not.toThrow();
    expect(() => draft.writeNow()).not.toThrow();
  });
});

describe("writeNow", () => {
  it("bypasses the debounce and cancels the pending timer", () => {
    const spy = vi.spyOn(localStorageMock, "setItem");
    const draft = makeDraft(() => ({ n: 2 }));
    draft.scheduleWrite();
    draft.writeNow();
    expect(spy).toHaveBeenCalledTimes(1);
    const parsed = JSON.parse(localStorageMock.getItem(KEY) as string);
    expect(parsed.data).toEqual({ n: 2 });
    vi.advanceTimersByTime(LOCAL_DRAFT_DEBOUNCE_MS);
    expect(spy).toHaveBeenCalledTimes(1);
  });
});

describe("peek", () => {
  it("round-trips a written draft", () => {
    const draft = makeDraft(() => ({ n: 3 }));
    draft.writeNow();
    const peeked = draft.peek();
    expect(peeked).not.toBeNull();
    expect(peeked?.data).toEqual({ n: 3 });
    expect(typeof peeked?.savedAt).toBe("number");
  });

  it("drops corrupt JSON and removes the key", () => {
    localStorageMock.setItem(KEY, "{not json");
    const draft = makeDraft(() => ({ n: 1 }));
    expect(draft.peek()).toBeNull();
    expect(localStorageMock.getItem(KEY)).toBeNull();
  });

  it("drops a version-mismatched envelope and removes the key", () => {
    localStorageMock.setItem(KEY, JSON.stringify({ version: 2, savedAt: 123, data: { n: 1 } }));
    const draft = makeDraft(() => ({ n: 1 }), 1);
    expect(draft.peek()).toBeNull();
    expect(localStorageMock.getItem(KEY)).toBeNull();
  });

  it("drops a non-object envelope and returns null when the key is absent", () => {
    localStorageMock.setItem(KEY, JSON.stringify("just a string"));
    const draft = makeDraft(() => ({ n: 1 }));
    expect(draft.peek()).toBeNull();
    expect(localStorageMock.getItem(KEY)).toBeNull();
    expect(draft.peek()).toBeNull();
  });
});

describe("discard", () => {
  it("removes the key and cancels a pending write", () => {
    const spy = vi.spyOn(localStorageMock, "setItem");
    const draft = makeDraft(() => ({ n: 4 }));
    draft.writeNow();
    spy.mockClear();
    draft.scheduleWrite();
    draft.discard();
    expect(localStorageMock.getItem(KEY)).toBeNull();
    vi.advanceTimersByTime(LOCAL_DRAFT_DEBOUNCE_MS);
    expect(spy).not.toHaveBeenCalled();
  });
});

describe("dispose", () => {
  it("cancels the pending timer without writing", () => {
    const draft = makeDraft(() => ({ n: 5 }));
    draft.scheduleWrite();
    draft.dispose();
    vi.advanceTimersByTime(LOCAL_DRAFT_DEBOUNCE_MS);
    expect(localStorageMock.getItem(KEY)).toBeNull();
  });
});

describe("without storage available", () => {
  it("all operations are safe no-ops", () => {
    delete g.localStorage;
    const draft = makeDraft(() => ({ n: 6 }));
    expect(() => {
      draft.scheduleWrite();
      vi.advanceTimersByTime(LOCAL_DRAFT_DEBOUNCE_MS);
      draft.writeNow();
      draft.discard();
      draft.dispose();
    }).not.toThrow();
    expect(draft.peek()).toBeNull();
  });
});
