// @vitest-environment happy-dom
// The palette opens what the input asks for, never a hit of an older or failed search, and is modal for the keys.
import { createPinia, setActivePinia } from "pinia";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { nextTick } from "vue";
import type { SearchHit, SearchResponse } from "../../types/search.types";
import { mountIn, settle } from "../../__fixtures__/mount";

const search = vi.fn();
const run = vi.fn();
vi.mock("../../api/search.api", () => ({
  SearchApi: { search: (...args: unknown[]) => search(...args) },
}));
vi.mock("vue-router", () => ({
  useRouter: () => ({ push: vi.fn(), currentRoute: { value: { fullPath: "/" } } }),
}));
vi.mock("./useOpenSearchHit", () => ({ useOpenSearchHit: () => ({ run }) }));

const { default: GlobalSearchPalette } = await import("./GlobalSearchPalette.vue");
const { useGlobalSearchStore } = await import("../../stores/global-search-store");
const { SEARCH_KINDS } = await import("./searchKinds");

const table = (id: number, name: string): SearchHit => ({
  kind: "table",
  id,
  name,
  qualified_name: name,
  namespace_id: null,
  path: [],
  description: null,
  subtitle: null,
  parent: null,
  matches: [],
  score: 90,
  uri: `flowfile://catalog/tables/${id}`,
  updated_at: null,
  details: {},
});

const answer = (query: string, hits: SearchHit[]): SearchResponse => ({
  query,
  tokens: [query],
  kinds: SEARCH_KINDS.map((meta) => meta.kind),
  hits,
  counts: { table: hits.length },
  truncated: false,
  truncated_kinds: [],
});

const input = () => document.querySelector<HTMLInputElement>("[data-testid=global-search-input]")!;
const panel = () => document.querySelector<HTMLElement>("[role=dialog]")!;
const press = (key: string, init: KeyboardEventInit = {}) =>
  input().dispatchEvent(
    new KeyboardEvent("keydown", { key, bubbles: true, cancelable: true, ...init }),
  );

async function openWith(query: string) {
  const store = useGlobalSearchStore();
  store.query = query;
  store.open();
  await settle();
  return store;
}

async function typeQuery(store: ReturnType<typeof useGlobalSearchStore>, query: string) {
  store.query = query;
  await nextTick();
}

async function elapse(ms = 200) {
  vi.advanceTimersByTime(ms);
  await settle();
}

describe("GlobalSearchPalette", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    setActivePinia(createPinia());
    search.mockReset();
    run.mockReset();
  });
  afterEach(() => vi.useRealTimers());

  it("waits for the answer to what was typed before Enter opens anything", async () => {
    search.mockImplementation(async (q: string) =>
      q === "ord" ? answer("ord", [table(1, "ordinals")]) : answer("orders", [table(2, "orders")]),
    );
    await mountIn(GlobalSearchPalette, { pinia: createPinia() });
    const store = await openWith("ord");
    await elapse();
    expect(document.querySelector("[data-testid=search-hit-table-1]")).not.toBeNull();

    await typeQuery(store, "orders");
    press("Enter");
    await settle();
    expect(run).not.toHaveBeenCalled();

    await elapse();
    expect(run).toHaveBeenCalledTimes(1);
    expect(run.mock.calls[0][0]).toMatchObject({ to: { query: { tableId: "2" } } });
  });

  it("drops a waiting Enter when the query changes again", async () => {
    search.mockImplementation(async (q: string) => answer(q, [table(q.length, q)]));
    await mountIn(GlobalSearchPalette, { pinia: createPinia() });
    const store = await openWith("ord");
    await elapse();
    await typeQuery(store, "orders");
    press("Enter");
    await typeQuery(store, "orders eu");
    await elapse();
    expect(run).not.toHaveBeenCalled();
  });

  it("hides the previous hits once a search fails and Enter opens none of them", async () => {
    search
      .mockResolvedValueOnce(answer("ord", [table(1, "ordinals")]))
      .mockRejectedValueOnce(new Error("Network Error"));
    await mountIn(GlobalSearchPalette, { pinia: createPinia() });
    const store = await openWith("ord");
    await elapse();
    await typeQuery(store, "orders");
    await elapse();
    expect(document.querySelector("[role=alert]")?.textContent).toContain("unavailable");
    expect(input().getAttribute("aria-activedescendant")).toBeNull();
    expect(input().getAttribute("aria-expanded")).toBe("false");
    press("Enter");
    await settle();
    expect(run).not.toHaveBeenCalled();
  });

  it("keeps the app's shortcuts from acting behind it, but not its own toggle", async () => {
    search.mockResolvedValue(answer("ord", []));
    await mountIn(GlobalSearchPalette, { pinia: createPinia() });
    await openWith("ord");
    const seen: string[] = [];
    const listener = (event: KeyboardEvent) => seen.push(event.key);
    window.addEventListener("keydown", listener);
    try {
      press("s", { metaKey: true });
      press("Backspace");
      press("F", {
        metaKey: true,
        ctrlKey: false,
        shiftKey: true,
        code: "KeyF",
      } as KeyboardEventInit);
      press("F", { ctrlKey: true, shiftKey: true, code: "KeyF" } as KeyboardEventInit);
    } finally {
      window.removeEventListener("keydown", listener);
    }
    expect(seen.length).toBe(1);
  });

  it("makes way for another dialog that takes focus instead of pulling focus back", async () => {
    search.mockResolvedValue(answer("ord", []));
    await mountIn(GlobalSearchPalette, { pinia: createPinia() });
    const store = await openWith("ord");
    const overlay = document.createElement("div");
    overlay.className = "el-overlay";
    const button = document.createElement("button");
    overlay.appendChild(button);
    document.body.appendChild(overlay);
    try {
      panel().dispatchEvent(new FocusEvent("focusout", { bubbles: true, relatedTarget: button }));
      await settle();
      expect(store.isOpen).toBe(false);
    } finally {
      overlay.remove();
    }
  });
});
