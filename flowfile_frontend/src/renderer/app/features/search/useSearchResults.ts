import axios from "axios";
import { onScopeDispose, ref, toValue, watch, type MaybeRefOrGetter } from "vue";
import { SearchApi } from "../../api/search.api";
import type { SearchKind, SearchResponse } from "../../types/search.types";

/** The longest query core accepts (`SearchRequest.query`). */
export const MAX_QUERY_LENGTH = 200;

export interface SearchResultsOptions {
  kinds?: MaybeRefOrGetter<SearchKind[] | undefined>;
  limitPerKind?: MaybeRefOrGetter<number>;
  debounceMs?: number;
}

export function errorMessage(error: unknown): string {
  const response = (error as { response?: { status?: number; data?: { detail?: unknown } } })
    ?.response;
  const detail = response?.data?.detail;
  if (typeof detail === "string") return detail;
  if (response?.status === 422) {
    const first = Array.isArray(detail) ? detail[0] : null;
    const msg = first && typeof first === "object" ? (first as { msg?: unknown }).msg : null;
    return typeof msg === "string" && msg ? msg : "That search can't be run; try shorter words.";
  }
  return "Search is unavailable right now.";
}

/**
 * `GET /search` for a query that changes as the user types: debounced, the request before the newest aborted, and
 * only the newest answer kept. The previous answer stays shown while the next one loads.
 */
export function useSearchResults(
  query: MaybeRefOrGetter<string>,
  options: SearchResultsOptions = {},
) {
  const response = ref<SearchResponse | null>(null);
  const loading = ref(false);
  const error = ref<string | null>(null);
  const debounceMs = options.debounceMs ?? 150;

  let timer: ReturnType<typeof setTimeout> | null = null;
  let controller: AbortController | null = null;
  let seq = 0;

  function cancelPending(): void {
    if (timer !== null) clearTimeout(timer);
    timer = null;
    controller?.abort();
    controller = null;
  }

  async function run(ticket: number): Promise<void> {
    timer = null;
    const q = toValue(query).trim();
    if (ticket !== seq || !q) return;
    const mine = new AbortController();
    controller = mine;
    try {
      const result = await SearchApi.search(q, {
        kinds: toValue(options.kinds),
        limitPerKind: toValue(options.limitPerKind),
        signal: mine.signal,
      });
      if (ticket !== seq) return;
      response.value = result;
      error.value = null;
    } catch (e) {
      if (ticket !== seq || axios.isCancel(e)) return;
      error.value = errorMessage(e);
    } finally {
      if (ticket === seq) loading.value = false;
    }
  }

  function schedule(immediate = false): void {
    cancelPending();
    const ticket = ++seq;
    if (!toValue(query).trim()) {
      response.value = null;
      error.value = null;
      loading.value = false;
      return;
    }
    loading.value = true;
    if (immediate) void run(ticket);
    else timer = setTimeout(() => void run(ticket), debounceMs);
  }

  watch(
    () => [toValue(query), toValue(options.kinds)?.join(","), toValue(options.limitPerKind)],
    () => schedule(),
    { immediate: true },
  );

  onScopeDispose(cancelPending);

  return { response, loading, error, refresh: () => schedule(true) };
}
