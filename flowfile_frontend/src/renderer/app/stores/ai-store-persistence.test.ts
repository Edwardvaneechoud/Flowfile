// Unit tests for the chat-history + AI-settings persistence helpers.
//
// The helpers are intentionally pure (no Vue, no Pinia, no real DOM)
// so a hand-rolled `Storage` mock is enough — no jsdom / happy-dom
// needed.

import { describe, expect, it } from "vitest";

import {
  MAX_PERSISTED_MESSAGES,
  PERSISTENCE_KEY,
  SETTINGS_PERSISTENCE_KEY,
  chatPersistenceKey,
  clearPersistedAiState,
  highestPersistedMessageId,
  loadPersistedAiSettings,
  loadPersistedAiState,
  persistAiSettings,
  persistAiState,
} from "./ai-store-persistence";
import type { PersistedAiSettings } from "./ai-store-persistence";
import type { ChatMessage } from "./ai-store";

// No-flow callers (the helper's default) write through the
// `unscoped` bucket. Legacy tests in this file all want the no-flow
// path, so reuse the resolved key rather than spelling the suffix in
// every assertion.
const UNSCOPED_KEY = chatPersistenceKey(null);

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
  { id: 1, createdAt: 1_700_000_000_000, role: "user", content: "Hello" },
  {
    id: 2,
    createdAt: 1_700_000_000_001,
    role: "assistant",
    content: "Hi there!",
    pending: false,
  },
];

const sampleSettings = (): PersistedAiSettings => ({
  selectedProvider: "anthropic",
  selectedModel: "claude-opus-4-7",
  splitModels: true,
  simpleProvider: "local",
  simpleModel: "qwen2.5-coder-3b",
  selectedAgentSurface: "agent_staged",
  verifyPlanCompletion: true,
});

describe("loadPersistedAiState", () => {
  it("returns empty state when storage is empty", () => {
    const storage = makeStorage();
    const state = loadPersistedAiState(storage);
    expect(state.messages).toEqual([]);
    expect(state.agentModeAccepted).toBeNull();
  });

  it("round-trips persisted messages and the conversation-scoped accept flag", () => {
    const storage = makeStorage();
    persistAiState({ messages: sampleMessages(), agentModeAccepted: true }, storage);

    const state = loadPersistedAiState(storage);
    expect(state.messages).toHaveLength(2);
    expect(state.messages[0]).toMatchObject({ id: 1, role: "user", content: "Hello" });
    expect(state.messages[1]).toMatchObject({ id: 2, role: "assistant", content: "Hi there!" });
    expect(state.agentModeAccepted).toBe(true);
  });

  it("still reads the legacy settings fields older builds wrote into the flow blob", () => {
    const storage = makeStorage();
    storage.setItem(
      UNSCOPED_KEY,
      JSON.stringify({
        messages: sampleMessages(),
        selectedProvider: "anthropic",
        selectedModel: "x",
        splitModels: true,
        simpleProvider: "local",
        simpleModel: "qwen",
        selectedAgentSurface: "agent_complex",
        verifyPlanCompletion: true,
      }),
    );
    const state = loadPersistedAiState(storage);
    expect(state.selectedProvider).toBe("anthropic");
    expect(state.selectedModel).toBe("x");
    expect(state.splitModels).toBe(true);
    expect(state.simpleProvider).toBe("local");
    expect(state.simpleModel).toBe("qwen");
    expect(state.selectedAgentSurface).toBe("agent_complex");
    expect(state.verifyPlanCompletion).toBe(true);
  });

  it("defaults the legacy settings fields to null when absent", () => {
    const storage = makeStorage();
    storage.setItem(UNSCOPED_KEY, JSON.stringify({ messages: sampleMessages() }));
    const state = loadPersistedAiState(storage);
    expect(state.selectedProvider).toBeNull();
    expect(state.selectedModel).toBeNull();
    expect(state.splitModels).toBeNull();
    expect(state.simpleProvider).toBeNull();
    expect(state.simpleModel).toBeNull();
    expect(state.selectedAgentSurface).toBeNull();
    expect(state.verifyPlanCompletion).toBeNull();
  });

  it("treats corrupt JSON as empty (no crash) and clears the bad entry", () => {
    const storage = makeStorage();
    storage.setItem(UNSCOPED_KEY, "{ this is not valid json");

    const state = loadPersistedAiState(storage);
    expect(state.messages).toEqual([]);
    // Bad entry was removed so subsequent reads aren't repeatedly corrupt.
    expect(storage.getItem(UNSCOPED_KEY)).toBeNull();
  });

  it("treats non-object payload as empty", () => {
    const storage = makeStorage();
    storage.setItem(UNSCOPED_KEY, JSON.stringify("a string, not the expected shape"));

    const state = loadPersistedAiState(storage);
    expect(state.messages).toEqual([]);
  });

  it("filters out malformed messages (missing id / role / content)", () => {
    const storage = makeStorage();
    storage.setItem(
      UNSCOPED_KEY,
      JSON.stringify({
        messages: [
          { id: 1, role: "user", content: "ok" },
          { id: "not-a-number", role: "user", content: "bad id" },
          { id: 3, role: "system", content: "bad role" },
          { id: 4, role: "assistant", content: 42 },
          { id: 5, role: "assistant", content: "fine" },
        ],
      }),
    );

    const state = loadPersistedAiState(storage);
    expect(state.messages.map((m) => m.id)).toEqual([1, 5]);
  });

  it("strips `pending: true` so a hydrated placeholder doesn't render as a stuck spinner", () => {
    const storage = makeStorage();
    persistAiState(
      {
        messages: [
          { id: 1, createdAt: 1_700_000_000_000, role: "user", content: "Hi" },
          {
            id: 2,
            createdAt: 1_700_000_000_001,
            role: "assistant",
            content: "partial...",
            pending: true,
          },
        ],
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
  });
});

describe("persistAiState", () => {
  it("writes JSON under the versioned key (unscoped path)", () => {
    const storage = makeStorage();
    persistAiState({ messages: sampleMessages(), agentModeAccepted: false }, storage);

    expect(storage.getItem(UNSCOPED_KEY)).not.toBeNull();
    const raw = storage.getItem(UNSCOPED_KEY)!;
    const parsed = JSON.parse(raw);
    expect(parsed.messages).toHaveLength(2);
    expect(parsed.agentModeAccepted).toBe(false);
  });

  it("never writes the settings fields into the flow blob (they live in the settings bucket)", () => {
    const storage = makeStorage();
    // A legacy entry still carrying the old fields on disk …
    storage.setItem(
      UNSCOPED_KEY,
      JSON.stringify({
        messages: [],
        selectedProvider: "groq",
        selectedAgentSurface: "agent_live",
      }),
    );
    // … drops them on the next save.
    persistAiState({ messages: sampleMessages() }, storage);
    const parsed = JSON.parse(storage.getItem(UNSCOPED_KEY)!);
    expect(parsed).not.toHaveProperty("selectedProvider");
    expect(parsed).not.toHaveProperty("selectedAgentSurface");
    expect(Object.keys(parsed).sort()).toEqual(["agentModeAccepted", "messages"]);
  });

  it("caps at MAX_PERSISTED_MESSAGES (keeps the most recent)", () => {
    const storage = makeStorage();
    const many: ChatMessage[] = [];
    for (let i = 1; i <= MAX_PERSISTED_MESSAGES + 50; i += 1) {
      many.push({
        id: i,
        createdAt: 1_700_000_000_000 + i,
        role: i % 2 === 0 ? "assistant" : "user",
        content: `m${i}`,
      });
    }
    persistAiState({ messages: many }, storage);

    const state = loadPersistedAiState(storage);
    expect(state.messages).toHaveLength(MAX_PERSISTED_MESSAGES);
    // Tail-preserving slice: the newest message survived.
    expect(state.messages[state.messages.length - 1].id).toBe(MAX_PERSISTED_MESSAGES + 50);
    // The 51st-from-newest didn't.
    expect(state.messages[0].id).toBe(51);
  });

  it("swallows quota errors so a full localStorage doesn't crash the app", () => {
    const storage = makeStorage();
    // Make setItem always throw (mimicking QuotaExceededError).
    storage.setItem = () => {
      throw new Error("QuotaExceededError");
    };

    expect(() => persistAiState({ messages: sampleMessages() }, storage)).not.toThrow();
  });

  it("is a no-op when storage is null", () => {
    expect(() => persistAiState({ messages: sampleMessages() }, null)).not.toThrow();
  });
});

describe("clearPersistedAiState", () => {
  it("removes the persisted entry", () => {
    const storage = makeStorage();
    persistAiState({ messages: sampleMessages() }, storage);
    expect(storage.getItem(UNSCOPED_KEY)).not.toBeNull();

    clearPersistedAiState(storage);
    expect(storage.getItem(UNSCOPED_KEY)).toBeNull();
  });

  it("is a no-op when storage is null", () => {
    expect(() => clearPersistedAiState(null)).not.toThrow();
  });

  it("clears only the targeted flow's key", () => {
    const storage = makeStorage();
    persistAiState({ messages: sampleMessages() }, storage, 7);
    persistAiState({ messages: sampleMessages() }, storage, 9);

    clearPersistedAiState(storage, 7);
    expect(storage.getItem(chatPersistenceKey(7))).toBeNull();
    expect(storage.getItem(chatPersistenceKey(9))).not.toBeNull();
  });
});

describe("highestPersistedMessageId", () => {
  it("returns 0 for an empty array", () => {
    expect(highestPersistedMessageId([])).toBe(0);
  });

  it("returns the maximum id", () => {
    expect(
      highestPersistedMessageId([
        { id: 3, createdAt: 1_700_000_000_003, role: "user", content: "" },
        { id: 7, createdAt: 1_700_000_000_007, role: "assistant", content: "" },
        { id: 5, createdAt: 1_700_000_000_005, role: "user", content: "" },
      ]),
    ).toBe(7);
  });
});

describe("acceptance criteria — full round-trip", () => {
  // Push messages, re-load, assert messages restored. Negative case:
  // corrupt JSON in storage → hydrates as empty (no crash).
  it("push messages then re-load → messages restored", () => {
    const storage = makeStorage();

    // Tab 1: fresh start.
    let snapshot = loadPersistedAiState(storage);
    expect(snapshot.messages).toEqual([]);

    // Tab 1: push messages and persist.
    const pushed: ChatMessage[] = [
      { id: 1, createdAt: 1_700_000_000_001, role: "user", content: "What's up?" },
      { id: 2, createdAt: 1_700_000_000_002, role: "assistant", content: "Not much." },
    ];
    persistAiState({ messages: pushed }, storage);

    // Tab 1, after refresh: re-load.
    snapshot = loadPersistedAiState(storage);
    expect(snapshot.messages.map((m) => m.content)).toEqual(["What's up?", "Not much."]);
  });

  it("corrupt JSON → empty hydrate, no crash", () => {
    const storage = makeStorage();
    storage.setItem(UNSCOPED_KEY, "<<<not json>>>");
    const snapshot = loadPersistedAiState(storage);
    expect(snapshot.messages).toEqual([]);
  });
});

// Per-flow scoping. The chat trail is keyed by `flow_id` so switching
// flows shows the right conversation; localStorage is the default
// surface so the trail survives Electron app restart.

describe("chatPersistenceKey", () => {
  it("formats a real flow id as the v1-suffixed key", () => {
    expect(chatPersistenceKey(7)).toBe(`${PERSISTENCE_KEY}.7`);
  });

  it("falls back to the `unscoped` bucket when no flow is open", () => {
    expect(chatPersistenceKey(null)).toBe(`${PERSISTENCE_KEY}.unscoped`);
  });

  it("keeps each flow id in its own bucket (no collisions)", () => {
    expect(chatPersistenceKey(7)).not.toBe(chatPersistenceKey(9));
    expect(chatPersistenceKey(7)).not.toBe(chatPersistenceKey(null));
  });

  it("never collides with the device-wide settings bucket", () => {
    expect(chatPersistenceKey(null)).not.toBe(SETTINGS_PERSISTENCE_KEY);
    expect(SETTINGS_PERSISTENCE_KEY.startsWith(PERSISTENCE_KEY)).toBe(false);
  });
});

describe("per-flow round-trip", () => {
  it("persists each flow's messages under its own key", () => {
    const storage = makeStorage();
    const flow7Messages: ChatMessage[] = [
      { id: 1, createdAt: 1, role: "user", content: "from flow 7" },
    ];
    const flow9Messages: ChatMessage[] = [
      { id: 2, createdAt: 2, role: "user", content: "from flow 9" },
      { id: 3, createdAt: 3, role: "assistant", content: "reply on flow 9" },
    ];

    persistAiState({ messages: flow7Messages }, storage, 7);
    persistAiState({ messages: flow9Messages }, storage, 9);

    expect(storage.getItem(chatPersistenceKey(7))).not.toBeNull();
    expect(storage.getItem(chatPersistenceKey(9))).not.toBeNull();

    const loaded7 = loadPersistedAiState(storage, 7);
    const loaded9 = loadPersistedAiState(storage, 9);
    expect(loaded7.messages.map((m) => m.content)).toEqual(["from flow 7"]);
    expect(loaded9.messages.map((m) => m.content)).toEqual(["from flow 9", "reply on flow 9"]);
  });

  it("simulates a flow switch A → B → A and restores A's history on return", () => {
    const storage = makeStorage();

    // Land on flow 7 first, push a message, persist.
    persistAiState(
      { messages: [{ id: 1, createdAt: 10, role: "user", content: "hello on 7" }] },
      storage,
      7,
    );

    // Switch to flow 9. Flow 7's bucket is untouched; flow 9 starts empty
    // because nothing's been persisted under its key yet.
    const onArrivalAt9 = loadPersistedAiState(storage, 9);
    expect(onArrivalAt9.messages).toEqual([]);

    // User chats on flow 9, persist there.
    persistAiState(
      { messages: [{ id: 1, createdAt: 11, role: "user", content: "hello on 9" }] },
      storage,
      9,
    );

    // Switch back to flow 7. Original messages still there.
    const onReturnAt7 = loadPersistedAiState(storage, 7);
    expect(onReturnAt7.messages.map((m) => m.content)).toEqual(["hello on 7"]);

    // And flow 9's bucket is unaffected.
    const recheck9 = loadPersistedAiState(storage, 9);
    expect(recheck9.messages.map((m) => m.content)).toEqual(["hello on 9"]);
  });

  it("starts fresh when switching to a flow with no prior persisted state", () => {
    const storage = makeStorage();
    // Pre-populate a different flow so the storage isn't entirely empty —
    // we want to prove the *target* flow is the empty one, not just that
    // the whole store is.
    persistAiState(
      { messages: [{ id: 1, createdAt: 10, role: "user", content: "noise" }] },
      storage,
      7,
    );

    const fresh = loadPersistedAiState(storage, 9001);
    expect(fresh.messages).toEqual([]);
    expect(fresh.agentModeAccepted).toBeNull();
  });

  it("keeps the unscoped bucket isolated from real-flow buckets", () => {
    const storage = makeStorage();
    persistAiState(
      { messages: [{ id: 1, createdAt: 1, role: "user", content: "no-flow chat" }] },
      storage,
      null,
    );
    persistAiState(
      { messages: [{ id: 2, createdAt: 2, role: "user", content: "flow chat" }] },
      storage,
      42,
    );

    expect(loadPersistedAiState(storage, null).messages.map((m) => m.content)).toEqual([
      "no-flow chat",
    ]);
    expect(loadPersistedAiState(storage, 42).messages.map((m) => m.content)).toEqual(["flow chat"]);
  });
});

describe("quota + corruption", () => {
  it("swallows quota errors when persisting under a per-flow key", () => {
    const storage = makeStorage();
    storage.setItem = () => {
      throw new Error("QuotaExceededError");
    };

    expect(() => persistAiState({ messages: sampleMessages() }, storage, 7)).not.toThrow();
  });

  it("clears only the corrupt flow's key on bad JSON, leaving siblings alone", () => {
    const storage = makeStorage();
    // Healthy flow 9 entry.
    persistAiState(
      { messages: [{ id: 1, createdAt: 1, role: "user", content: "intact" }] },
      storage,
      9,
    );
    // Corrupt flow 7 entry.
    storage.setItem(chatPersistenceKey(7), "{broken");

    const loaded7 = loadPersistedAiState(storage, 7);
    expect(loaded7.messages).toEqual([]);
    // Flow 7's corrupt entry was scrubbed.
    expect(storage.getItem(chatPersistenceKey(7))).toBeNull();
    // Flow 9's entry is untouched.
    expect(storage.getItem(chatPersistenceKey(9))).not.toBeNull();
    const loaded9 = loadPersistedAiState(storage, 9);
    expect(loaded9.messages.map((m) => m.content)).toEqual(["intact"]);
  });
});

// Device-wide settings bucket: provider/model, the simple tier and the agent
// toggles. One key regardless of which flow is open — this is what
// Settings → AI edits.

describe("AI settings bucket", () => {
  it("returns null when nothing has been written (fresh install)", () => {
    expect(loadPersistedAiSettings(makeStorage())).toBeNull();
  });

  it("returns null when storage is unavailable", () => {
    expect(loadPersistedAiSettings(null)).toBeNull();
  });

  it("round-trips every field under its own key", () => {
    const storage = makeStorage();
    persistAiSettings(sampleSettings(), storage);

    expect(storage.getItem(SETTINGS_PERSISTENCE_KEY)).not.toBeNull();
    expect(loadPersistedAiSettings(storage)).toEqual(sampleSettings());
  });

  it("round-trips nulls so an unset field stays unset", () => {
    const storage = makeStorage();
    persistAiSettings(
      {
        selectedProvider: "openai",
        selectedModel: null,
        splitModels: null,
        simpleProvider: null,
        simpleModel: null,
        selectedAgentSurface: null,
        verifyPlanCompletion: null,
      },
      storage,
    );
    const loaded = loadPersistedAiSettings(storage);
    expect(loaded?.selectedProvider).toBe("openai");
    expect(loaded?.selectedModel).toBeNull();
    expect(loaded?.selectedAgentSurface).toBeNull();
  });

  it.each(["agent_complex", "agent_staged", "agent_live"] as const)(
    "round-trips agent surface %s",
    (surface) => {
      const storage = makeStorage();
      persistAiSettings({ ...sampleSettings(), selectedAgentSurface: surface }, storage);
      expect(loadPersistedAiSettings(storage)?.selectedAgentSurface).toBe(surface);
    },
  );

  it("rejects an unknown surface value as null (defensive — bad data on disk)", () => {
    const storage = makeStorage();
    storage.setItem(
      SETTINGS_PERSISTENCE_KEY,
      JSON.stringify({ selectedProvider: "x", selectedAgentSurface: "agent_unknown_variant" }),
    );
    const loaded = loadPersistedAiSettings(storage);
    expect(loaded?.selectedProvider).toBe("x");
    expect(loaded?.selectedAgentSurface).toBeNull();
  });

  it("treats corrupt JSON as never-written and scrubs the entry", () => {
    const storage = makeStorage();
    storage.setItem(SETTINGS_PERSISTENCE_KEY, "{nope");
    expect(loadPersistedAiSettings(storage)).toBeNull();
    expect(storage.getItem(SETTINGS_PERSISTENCE_KEY)).toBeNull();
  });

  it("does not touch the per-flow chat buckets", () => {
    const storage = makeStorage();
    persistAiState({ messages: sampleMessages() }, storage, 7);
    persistAiSettings(sampleSettings(), storage);
    expect(loadPersistedAiState(storage, 7).messages).toHaveLength(2);
    expect(JSON.parse(storage.getItem(chatPersistenceKey(7))!)).not.toHaveProperty(
      "selectedProvider",
    );
  });

  it("swallows quota errors", () => {
    const storage = makeStorage();
    storage.setItem = () => {
      throw new Error("QuotaExceededError");
    };
    expect(() => persistAiSettings(sampleSettings(), storage)).not.toThrow();
  });
});
