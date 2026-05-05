// Unit tests for the agent-store persistence helpers (Bug-2 fix).
//
// Pure helpers, no Vue/Pinia deps — same harness as W27's
// `ai-store-persistence.test.ts`. Hand-rolls a `MockStorage` so we don't
// need jsdom.

import { describe, expect, it } from "vitest";
import {
  AGENT_PERSISTENCE_KEY,
  MAX_PERSISTED_AGENT_EVENTS,
  clearPersistedAgentState,
  loadPersistedAgentState,
  persistAgentState,
  type PersistedAgentState,
} from "./ai-agent-store-persistence";

class MockStorage {
  private map = new Map<string, string>();
  private quotaExceeded = false;
  setQuotaExceeded(v: boolean) {
    this.quotaExceeded = v;
  }
  getItem(key: string): string | null {
    return this.map.has(key) ? this.map.get(key)! : null;
  }
  setItem(key: string, value: string): void {
    if (this.quotaExceeded) {
      const err = new Error("QuotaExceededError");
      err.name = "QuotaExceededError";
      throw err;
    }
    this.map.set(key, value);
  }
  removeItem(key: string): void {
    this.map.delete(key);
  }
}

const minimalState = (overrides: Partial<PersistedAgentState> = {}): PersistedAgentState => ({
  events: [],
  currentSessionId: null,
  status: "idle",
  driftDetail: null,
  lastResult: null,
  error: null,
  ...overrides,
});

describe("ai-agent-store-persistence", () => {
  it("returns empty state when storage has no entry", () => {
    const storage = new MockStorage();
    const state = loadPersistedAgentState(storage);
    expect(state.events).toEqual([]);
    expect(state.currentSessionId).toBeNull();
    expect(state.status).toBe("idle");
  });

  it("round-trips a populated state cleanly", () => {
    const storage = new MockStorage();
    const original = minimalState({
      events: [{ kind: "thinking", payload: { text: "hi" }, at: 1234 }],
      currentSessionId: "sess-1",
      status: "paused_drift",
      driftDetail: {
        missingNodeIds: [3],
        externalAddedNodeIds: [],
        nodeTypes: { 3: "filter" },
      },
    });
    persistAgentState(original, storage);
    const loaded = loadPersistedAgentState(storage);
    expect(loaded.events).toEqual(original.events);
    expect(loaded.currentSessionId).toBe("sess-1");
    expect(loaded.status).toBe("paused_drift");
    expect(loaded.driftDetail).toEqual(original.driftDetail);
  });

  it("normalises status='running' to 'idle' on hydrate (SSE stream is dead)", () => {
    const storage = new MockStorage();
    persistAgentState(
      minimalState({ status: "running", currentSessionId: "sess-running" }),
      storage,
    );
    const loaded = loadPersistedAgentState(storage);
    // currentSessionId is preserved (the user might still want to query the
    // server about it), but the local status flips to idle so the UI doesn't
    // show a forever-spinning state.
    expect(loaded.status).toBe("idle");
    expect(loaded.currentSessionId).toBe("sess-running");
  });

  it("preserves status='paused_drift' so the resume buttons can fire", () => {
    const storage = new MockStorage();
    persistAgentState(
      minimalState({
        status: "paused_drift",
        currentSessionId: "sess-paused",
        driftDetail: { missingNodeIds: [1], externalAddedNodeIds: [], nodeTypes: {} },
      }),
      storage,
    );
    const loaded = loadPersistedAgentState(storage);
    expect(loaded.status).toBe("paused_drift");
    expect(loaded.currentSessionId).toBe("sess-paused");
  });

  it("drops malformed events without crashing", () => {
    const storage = new MockStorage();
    storage.setItem(
      AGENT_PERSISTENCE_KEY,
      JSON.stringify({
        events: [
          { kind: "thinking", payload: { text: "ok" }, at: 1 },
          { kind: "garbage_kind", payload: {}, at: 2 },
          { kind: "info", payload: null, at: 3 },
          { kind: "info", payload: { x: 1 }, at: "not-a-number" },
          "string-not-object",
        ],
      }),
    );
    const loaded = loadPersistedAgentState(storage);
    expect(loaded.events).toEqual([{ kind: "thinking", payload: { text: "ok" }, at: 1 }]);
  });

  it("recovers from corrupt JSON by removing the bad entry", () => {
    const storage = new MockStorage();
    storage.setItem(AGENT_PERSISTENCE_KEY, "{not json");
    const loaded = loadPersistedAgentState(storage);
    expect(loaded.events).toEqual([]);
    expect(storage.getItem(AGENT_PERSISTENCE_KEY)).toBeNull();
  });

  it("caps events at MAX_PERSISTED_AGENT_EVENTS (tail-preserving)", () => {
    const storage = new MockStorage();
    const events = Array.from({ length: MAX_PERSISTED_AGENT_EVENTS + 50 }, (_, i) => ({
      kind: "thinking" as const,
      payload: { idx: i },
      at: i,
    }));
    persistAgentState(minimalState({ events }), storage);
    const loaded = loadPersistedAgentState(storage);
    expect(loaded.events.length).toBe(MAX_PERSISTED_AGENT_EVENTS);
    expect(loaded.events[0].payload.idx).toBe(50);
    expect(loaded.events[loaded.events.length - 1].payload.idx).toBe(
      MAX_PERSISTED_AGENT_EVENTS + 50 - 1,
    );
  });

  it("swallows quota-exceeded errors", () => {
    const storage = new MockStorage();
    storage.setQuotaExceeded(true);
    expect(() => persistAgentState(minimalState({ currentSessionId: "x" }), storage)).not.toThrow();
  });

  it("sanitises driftDetail nodeTypes (string keys → numeric)", () => {
    const storage = new MockStorage();
    storage.setItem(
      AGENT_PERSISTENCE_KEY,
      JSON.stringify({
        driftDetail: {
          missingNodeIds: [3],
          externalAddedNodeIds: [42],
          nodeTypes: { "3": "filter", "42": "manual_input", garbage: "x" },
        },
      }),
    );
    const loaded = loadPersistedAgentState(storage);
    expect(loaded.driftDetail).not.toBeNull();
    expect(loaded.driftDetail!.nodeTypes[3]).toBe("filter");
    expect(loaded.driftDetail!.nodeTypes[42]).toBe("manual_input");
    expect(Object.keys(loaded.driftDetail!.nodeTypes)).toHaveLength(2);
  });

  it("clearPersistedAgentState wipes the entry", () => {
    const storage = new MockStorage();
    persistAgentState(minimalState({ currentSessionId: "x" }), storage);
    expect(storage.getItem(AGENT_PERSISTENCE_KEY)).not.toBeNull();
    clearPersistedAgentState(storage);
    expect(storage.getItem(AGENT_PERSISTENCE_KEY)).toBeNull();
  });
});
