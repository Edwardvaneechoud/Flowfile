// Pinia store for the settings-autocomplete surface.
//
// Distinct from the chat store because cancellation semantics differ
// — chat cancels per-message (long-running stream); autocomplete
// cancels per keystroke (short fast JSON). Sharing one AbortController
// across surfaces would mean a chat send aborts an in-flight formula
// suggestion fetch.
//
// The store doesn't cache results — autocomplete is fire-and-forget with the
// CodeMirror dropdown owning the rendered list. We only track the single
// in-flight controller so a fresh keystroke can cancel its predecessor.

import { defineStore } from "pinia";
import { ref } from "vue";

import {
  fetchFormulaSuggestions,
  fetchJoinKeySuggestions,
  AiDisabledError,
  FormulaAutocompleteRequest,
  FormulaSuggestionsResponse,
  JoinKeyAutocompleteRequest,
  JoinKeySuggestionsResponse,
} from "../api/ai.api";
import { useAiStore } from "./ai-store";

const isAbortError = (err: unknown): boolean => {
  if (err instanceof DOMException && err.name === "AbortError") return true;
  if (err && typeof err === "object" && (err as { name?: string }).name === "CanceledError")
    return true;
  return false;
};

export const useAiAutocompleteStore = defineStore("aiAutocomplete", () => {
  // The most recent in-flight controller, of any surface (formula / join).
  // Replacing it cancels the prior request — autocomplete is only ever
  // showing ONE list at a time, so a single shared controller is the
  // right granularity.
  const inflight = ref<AbortController | null>(null);
  // Last error that wasn't a cancellation. Surfaced for debug; the
  // CodeMirror source treats these as "no AI suggestions this round" and
  // falls back silently.
  const lastError = ref<unknown>(null);
  // True iff the backend last returned 503 — surface so callers can render
  // a one-time "AI disabled" badge without re-fetching.
  const aiDisabled = ref(false);

  const cancel = (): void => {
    if (inflight.value !== null) {
      inflight.value.abort();
      inflight.value = null;
    }
  };

  const _replaceController = (): AbortController => {
    cancel();
    const c = new AbortController();
    inflight.value = c;
    return c;
  };

  // Inject the user's chat provider / model when the caller didn't
  // pin one. Without this, a body with no ``provider`` field falls
  // through to the route's default and 409s if that default isn't the
  // user's configured provider. The caller's explicit value
  // (``aiCompletions.ts`` passes one via ``opts.getProvider``) wins;
  // this only fills the gap when the body omits the field
  // (``Join.vue`` does).
  const _withDefaults = <T extends { provider?: string; model?: string | null }>(
    body: T,
  ): T => {
    const aiStore = useAiStore();
    return {
      ...body,
      provider: body.provider ?? aiStore.selectedProvider ?? undefined,
      model: body.model ?? aiStore.selectedModel ?? undefined,
    };
  };

  const getFormulaSuggestions = async (
    body: FormulaAutocompleteRequest,
  ): Promise<FormulaSuggestionsResponse | null> => {
    const controller = _replaceController();
    try {
      const result = await fetchFormulaSuggestions(_withDefaults(body), controller.signal);
      lastError.value = null;
      aiDisabled.value = false;
      return result;
    } catch (error) {
      if (isAbortError(error)) return null;
      if (error instanceof AiDisabledError) {
        aiDisabled.value = true;
        lastError.value = null;
        return null;
      }
      lastError.value = error;
      return null;
    } finally {
      // Only clear if THIS controller is still the active one; a faster
      // keystroke may have replaced it before our finally fires.
      if (inflight.value === controller) inflight.value = null;
    }
  };

  const getJoinKeySuggestions = async (
    body: JoinKeyAutocompleteRequest,
  ): Promise<JoinKeySuggestionsResponse | null> => {
    const controller = _replaceController();
    try {
      const result = await fetchJoinKeySuggestions(_withDefaults(body), controller.signal);
      lastError.value = null;
      aiDisabled.value = false;
      return result;
    } catch (error) {
      if (isAbortError(error)) return null;
      if (error instanceof AiDisabledError) {
        aiDisabled.value = true;
        lastError.value = null;
        return null;
      }
      lastError.value = error;
      return null;
    } finally {
      if (inflight.value === controller) inflight.value = null;
    }
  };

  return {
    inflight,
    lastError,
    aiDisabled,
    cancel,
    getFormulaSuggestions,
    getJoinKeySuggestions,
  };
});
