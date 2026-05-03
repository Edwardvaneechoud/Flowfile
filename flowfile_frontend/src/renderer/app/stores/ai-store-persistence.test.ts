// W27 — unit tests for the chat-history persistence helpers.
//
// The helpers are intentionally pure (no Vue, no Pinia, no real DOM) so a
// hand-rolled `Storage` mock is enough — no jsdom / happy-dom needed.

import { describe, expect, it } from "vitest";

import {
  MAX_PERSISTED_MESSAGES,
  PERSISTENCE_KEY,
  clearPersistedAiState,
  highestPersistedMessageId,
  loadPersistedAiState,
  persistAiState,
} from "./ai-store-persistence";
import type { ChatMessage } from "./ai-store";

interface MockStorage {
  getItem(key: string): string | null;
  setItem(key: string, value: string): void;
  removeItem(key: string): void;
  _data: Map<string, string>;
}

const makeStorage = (): MockStorage => {
  const data = new Map<string, string>();
  return {
    _data: data,
    getItem: (key) => (data.has(key) ? data.get(key)! : null),
    setItem: (key, value) => {
      data.set(key, value);
    },
    removeItem: (key) => {
      data.delete(key);
    },
  };
};

const sampleMessages = (): ChatMessage[] => [
  { id: 1, role: "user", content: "Hello" },
  { id: 2, role: "assistant", content: "Hi there!", pending: false },
];

describe("loadPersistedAiState", () => {
  it("returns empty state when storage is empty", () => {
    const storage = makeStorage();
    const state = loadPersistedAiState(storage);
    expect(state.messages).toEqual([]);
    expect(state.selectedProvider).toBeNull();
    expect(state.selectedModel).toBeNull();
  });

  it("round-trips persisted messages, provider, model", () => {
    const storage = makeStorage();
    persistAiState(
      {
        messages: sampleMessages(),
        selectedProvider: "anthropic",
        selectedModel: "claude-opus-4-7",
      },
      storage,
    );

    const state = loadPersistedAiState(storage);
    expect(state.messages).toHaveLength(2);
    expect(state.messages[0]).toMatchObject({ id: 1, role: "user", content: "Hello" });
    expect(state.messages[1]).toMatchObject({ id: 2, role: "assistant", content: "Hi there!" });
    expect(state.selectedProvider).toBe("anthropic");
    expect(state.selectedModel).toBe("claude-opus-4-7");
  });

  it("treats corrupt JSON as empty (no crash) and clears the bad entry", () => {
    const storage = makeStorage();
    storage.setItem(PERSISTENCE_KEY, "{ this is not valid json");

    const state = loadPersistedAiState(storage);
    expect(state.messages).toEqual([]);
    expect(state.selectedProvider).toBeNull();
    expect(state.selectedModel).toBeNull();
    // Bad entry was removed so subsequent reads aren't repeatedly corrupt.
    expect(storage.getItem(PERSISTENCE_KEY)).toBeNull();
  });

  it("treats non-object payload as empty", () => {
    const storage = makeStorage();
    storage.setItem(PERSISTENCE_KEY, JSON.stringify("a string, not the expected shape"));

    const state = loadPersistedAiState(storage);
    expect(state.messages).toEqual([]);
    expect(state.selectedProvider).toBeNull();
    expect(state.selectedModel).toBeNull();
  });

  it("filters out malformed messages (missing id / role / content)", () => {
    const storage = makeStorage();
    storage.setItem(
      PERSISTENCE_KEY,
      JSON.stringify({
        messages: [
          { id: 1, role: "user", content: "ok" },
          { id: "not-a-number", role: "user", content: "bad id" },
          { id: 3, role: "system", content: "bad role" },
          { id: 4, role: "assistant", content: 42 },
          { id: 5, role: "assistant", content: "fine" },
        ],
        selectedProvider: "openai",
        selectedModel: null,
      }),
    );

    const state = loadPersistedAiState(storage);
    expect(state.messages.map((m) => m.id)).toEqual([1, 5]);
    expect(state.selectedProvider).toBe("openai");
    expect(state.selectedModel).toBeNull();
  });

  it("strips `pending: true` so a hydrated placeholder doesn't render as a stuck spinner", () => {
    const storage = makeStorage();
    persistAiState(
      {
        messages: [
          { id: 1, role: "user", content: "Hi" },
          { id: 2, role: "assistant", content: "partial...", pending: true },
        ],
        selectedProvider: null,
        selectedModel: null,
      },
      storage,
    );

    const state = loadPersistedAiState(storage);
    expect(state.messages[1].pending).toBe(false);
    // partial content is preserved — useful UX even if the stream was killed
    expect(state.messages[1].content).toBe("partial...");
  });

  it("returns empty when storage is null (e.g., SSR / no window)", () => {
    const state = loadPersistedAiState(null);
    expect(state.messages).toEqual([]);
    expect(state.selectedProvider).toBeNull();
    expect(state.selectedModel).toBeNull();
  });
});

describe("persistAiState", () => {
  it("writes JSON under the versioned key", () => {
    const storage = makeStorage();
    persistAiState(
      { messages: sampleMessages(), selectedProvider: "groq", selectedModel: null },
      storage,
    );

    expect(storage.getItem(PERSISTENCE_KEY)).not.toBeNull();
    const raw = storage.getItem(PERSISTENCE_KEY)!;
    const parsed = JSON.parse(raw);
    expect(parsed.messages).toHaveLength(2);
    expect(parsed.selectedProvider).toBe("groq");
  });

  it("caps at MAX_PERSISTED_MESSAGES (keeps the most recent)", () => {
    const storage = makeStorage();
    const many: ChatMessage[] = [];
    for (let i = 1; i <= MAX_PERSISTED_MESSAGES + 50; i += 1) {
      many.push({ id: i, role: i % 2 === 0 ? "assistant" : "user", content: `m${i}` });
    }
    persistAiState({ messages: many, selectedProvider: null, selectedModel: null }, storage);

    const state = loadPersistedAiState(storage);
    expect(state.messages).toHaveLength(MAX_PERSISTED_MESSAGES);
    // Tail-preserving slice: the newest message survived.
    expect(state.messages[state.messages.length - 1].id).toBe(MAX_PERSISTED_MESSAGES + 50);
    // The 51st-from-newest didn't.
    expect(state.messages[0].id).toBe(51);
  });

  it("swallows quota errors so a full sessionStorage doesn't crash the app", () => {
    const storage = makeStorage();
    // Make setItem always throw (mimicking QuotaExceededError).
    storage.setItem = () => {
      throw new Error("QuotaExceededError");
    };

    expect(() =>
      persistAiState(
        { messages: sampleMessages(), selectedProvider: null, selectedModel: null },
        storage,
      ),
    ).not.toThrow();
  });

  it("is a no-op when storage is null", () => {
    expect(() =>
      persistAiState(
        { messages: sampleMessages(), selectedProvider: null, selectedModel: null },
        null,
      ),
    ).not.toThrow();
  });
});

describe("clearPersistedAiState", () => {
  it("removes the persisted entry", () => {
    const storage = makeStorage();
    persistAiState(
      { messages: sampleMessages(), selectedProvider: "anthropic", selectedModel: null },
      storage,
    );
    expect(storage.getItem(PERSISTENCE_KEY)).not.toBeNull();

    clearPersistedAiState(storage);
    expect(storage.getItem(PERSISTENCE_KEY)).toBeNull();
  });

  it("is a no-op when storage is null", () => {
    expect(() => clearPersistedAiState(null)).not.toThrow();
  });
});

describe("highestPersistedMessageId", () => {
  it("returns 0 for an empty array", () => {
    expect(highestPersistedMessageId([])).toBe(0);
  });

  it("returns the maximum id", () => {
    expect(
      highestPersistedMessageId([
        { id: 3, role: "user", content: "" },
        { id: 7, role: "assistant", content: "" },
        { id: 5, role: "user", content: "" },
      ]),
    ).toBe(7);
  });
});

describe("acceptance criteria — full round-trip mirroring W27 spec", () => {
  // Spec-criterion 5 verbatim: "mock sessionStorage, push messages,
  // re-instantiate the store, assert messages restored. Negative case:
  // corrupt JSON in sessionStorage → store hydrates as empty (no crash)."
  it("push messages then re-load → messages restored", () => {
    const storage = makeStorage();

    // Tab 1: fresh start.
    let snapshot = loadPersistedAiState(storage);
    expect(snapshot.messages).toEqual([]);

    // Tab 1: push messages and persist.
    const pushed: ChatMessage[] = [
      { id: 1, role: "user", content: "What's up?" },
      { id: 2, role: "assistant", content: "Not much." },
    ];
    persistAiState(
      { messages: pushed, selectedProvider: "openai", selectedModel: "gpt-4o" },
      storage,
    );

    // Tab 1, after refresh: re-load.
    snapshot = loadPersistedAiState(storage);
    expect(snapshot.messages.map((m) => m.content)).toEqual(["What's up?", "Not much."]);
    expect(snapshot.selectedProvider).toBe("openai");
    expect(snapshot.selectedModel).toBe("gpt-4o");
  });

  it("corrupt JSON → empty hydrate, no crash", () => {
    const storage = makeStorage();
    storage.setItem(PERSISTENCE_KEY, "<<<not json>>>");
    const snapshot = loadPersistedAiState(storage);
    expect(snapshot.messages).toEqual([]);
    expect(snapshot.selectedProvider).toBeNull();
    expect(snapshot.selectedModel).toBeNull();
  });
});
