import { defineStore } from "pinia";
import { ref } from "vue";

const RECENT_KEY = "flowfile.search.recent.v1";
const MAX_RECENT = 8;

function readRecent(): string[] {
  try {
    const raw = localStorage.getItem(RECENT_KEY);
    const parsed = raw ? JSON.parse(raw) : [];
    return Array.isArray(parsed) ? parsed.filter((q): q is string => typeof q === "string") : [];
  } catch {
    return [];
  }
}

function writeRecent(queries: string[]): void {
  try {
    localStorage.setItem(RECENT_KEY, JSON.stringify(queries));
  } catch {
    // Storage blocked: recents just don't survive a reload.
  }
}

/** The global search palette: whether it is open, what it holds, and this browser's recent searches. */
export const useGlobalSearchStore = defineStore("globalSearch", () => {
  const isOpen = ref(false);
  const query = ref("");
  const recent = ref<string[]>(readRecent());

  function open(initialQuery?: string): void {
    if (initialQuery !== undefined) query.value = initialQuery;
    isOpen.value = true;
  }

  function close(): void {
    isOpen.value = false;
  }

  function toggle(): void {
    if (isOpen.value) close();
    else open();
  }

  function remember(value: string): void {
    const trimmed = value.trim();
    if (!trimmed) return;
    recent.value = [trimmed, ...recent.value.filter((q) => q !== trimmed)].slice(0, MAX_RECENT);
    writeRecent(recent.value);
  }

  function clearRecent(): void {
    recent.value = [];
    writeRecent([]);
  }

  /** On sign-in and sign-out: another user must not see this one's query or recent searches. */
  function reset(): void {
    close();
    query.value = "";
    clearRecent();
  }

  return { isOpen, query, recent, open, close, toggle, remember, clearRecent, reset };
});
