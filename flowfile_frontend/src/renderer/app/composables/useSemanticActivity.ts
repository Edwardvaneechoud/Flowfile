import { computed, reactive, readonly } from "vue";
import { SemanticModelApi } from "../api/semanticModel.api";
import { useCatalogStore } from "../stores/catalog-store";
import { useSemanticModelStore } from "../stores/semantic-model-store";
import type { SemanticActivity } from "../types/semantic.types";

export const SUGGEST_BUSY_TEXT = "A suggestion is already running for this model.";

const POLL_MS = 3000;
const DISMISS_KEY = "flowfile.semantic.activityDismissed.v1";

function readDismissed(): Record<string, string> {
  try {
    const raw = JSON.parse(sessionStorage.getItem(DISMISS_KEY) ?? "{}");
    return raw && typeof raw === "object" ? raw : {};
  } catch {
    return {};
  }
}

// Module scope, not a store: one status per open model, shared by every surface that can start a suggestion.
const state = reactive({
  namespaceId: null as number | null,
  running: null as SemanticActivity | null,
  last: null as SemanticActivity | null,
  now: Date.now(),
  dismissed: readDismissed(),
});
let timer: ReturnType<typeof setTimeout> | null = null;
let seq = 0;
let attached = 0;
let inFlight = 0;

function schedule(): void {
  if (timer) clearTimeout(timer);
  timer = null;
  if (state.namespaceId === null || attached === 0) return;
  if (state.running !== null || inFlight > 0) timer = setTimeout(() => void refresh(), POLL_MS);
}

async function reloadModel(id: number): Promise<void> {
  const store = useSemanticModelStore();
  if (store.namespaceId !== id) return;
  await Promise.all([store.load(id), useCatalogStore().loadTree()]);
}

async function refresh(): Promise<void> {
  const id = state.namespaceId;
  if (id === null) return;
  const token = ++seq;
  try {
    const out = await SemanticModelApi.activity(id);
    if (token !== seq || state.namespaceId !== id) return;
    const ended = state.running !== null && out.running === null;
    state.running = out.running;
    state.last = out.last;
    state.now = Date.now();
    // A run this page started reloads through its own store action; one started elsewhere reloads here.
    if (ended && inFlight === 0) void reloadModel(id);
  } catch {
    // an older core has no activity route: no status line, the buttons keep their own state
  }
  schedule();
}

function ago(iso: string, now: number): string {
  const seconds = Math.max(0, Math.round((now - Date.parse(iso)) / 1000));
  if (seconds < 45) return "just now";
  const minutes = Math.round(seconds / 60);
  if (minutes < 60) return minutes === 1 ? "1 minute ago" : `${minutes} minutes ago`;
  const hours = Math.round(minutes / 60);
  return hours === 1 ? "1 hour ago" : `${hours} hours ago`;
}

function dismissLast(): void {
  const last = state.last;
  if (state.namespaceId === null || !last?.finished_at) return;
  state.dismissed = { ...state.dismissed, [String(state.namespaceId)]: last.finished_at };
  try {
    sessionStorage.setItem(DISMISS_KEY, JSON.stringify(state.dismissed));
  } catch {
    // a blocked sessionStorage only means the line can show again after a reload
  }
}

/** The open model's Suggest status (core's `…/semantic-model/activity`), polled while a run goes on. */
export function useSemanticActivity() {
  const busy = computed(() => state.running !== null);

  const statusLine = computed(() => {
    const r = state.running;
    if (!r) return "";
    return `${r.label} started ${ago(r.started_at, state.now)}${r.step ? ` · ${r.step}` : ""}`;
  });

  const result = computed(() => {
    const last = state.last;
    if (!last?.finished_at || state.running || state.namespaceId === null) return null;
    if (state.dismissed[String(state.namespaceId)] === last.finished_at) return null;
    const text = last.ok
      ? (last.summary ?? "The suggestion finished.")
      : `The suggestion failed${last.error ? `: ${last.error}` : "."}`;
    return { text, ok: !!last.ok, mine: last.mine };
  });

  /** Follow `namespaceId` while a component that shows the status is mounted; pair with `detach`. */
  function attach(namespaceId: number): void {
    attached++;
    open(namespaceId);
  }

  function detach(): void {
    attached = Math.max(0, attached - 1);
    if (attached === 0 && timer) {
      clearTimeout(timer);
      timer = null;
    }
  }

  function open(namespaceId: number): void {
    if (state.namespaceId !== namespaceId) {
      state.namespaceId = namespaceId;
      state.running = null;
      state.last = null;
    }
    void refresh();
  }

  /** Run a suggest request from this page, reading the status while it goes on. A surface that shows its own
   * outcome passes `ownOutcome`, so the shared result line does not repeat it. */
  async function track<T>(work: () => Promise<T>, opts: { ownOutcome?: boolean } = {}): Promise<T> {
    inFlight++;
    const first = setTimeout(() => void refresh(), 400);
    schedule();
    try {
      return await work();
    } finally {
      clearTimeout(first);
      await refresh();
      inFlight--;
      schedule();
      if (opts.ownOutcome) dismissLast();
    }
  }

  return {
    activity: readonly(state),
    busy,
    statusLine,
    result,
    attach,
    detach,
    open,
    refresh,
    track,
    dismiss: dismissLast,
  };
}
