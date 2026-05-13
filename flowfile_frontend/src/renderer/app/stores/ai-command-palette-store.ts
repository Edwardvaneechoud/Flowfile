// Pinia store for the Cmd+K command palette.
//
// Owns the lifecycle of one palette session: open/close, the typed prompt,
// in-flight request, and post-success handoff to the diff store. The
// store does NOT own the staged GraphDiff itself; that belongs to
// `useAiDiffStore` so the existing accept/reject UI mounts transparently.
// After a successful submit we call `aiDiffStore.setCurrentDiff(diff)` and
// close the palette — the user sees the diff in `AiDiffPanel` inside the
// existing AI drawer.
//
// Cancellation is per-submit: each `submit()` call gets a fresh
// AbortController that supersedes any in-flight one. A user pressing Esc
// while loading aborts the request before the palette closes.
//
// Lifecycle invariants:
// - Only one inflight request at a time (`cancel()` runs on each new submit).
// - On submit-success the palette closes; the diff is in `useAiDiffStore`.
// - On submit-degraded the palette stays open so the user can retry; the
//   degraded reason is rendered inline.
// - `error` is set for transport-level failures (HTTP 4xx/5xx that aren't
//   the soft-failure path) and AiDisabledError. The palette stays open so
//   the user can adjust their prompt or provider.

import { defineStore } from "pinia";
import { ref } from "vue";

import {
  AiDisabledError,
  submitCommandPalette,
  type CommandPaletteDegradedReason,
  type CommandPaletteInsertionContext,
  type CommandPaletteRefusal,
  type CommandPaletteRequest,
  type CommandPaletteResponse,
} from "../api/ai.api";
import { useAiDiffStore } from "./ai-diff-store";
import { useEditorStore } from "./editor-store";

const isAbortError = (err: unknown): boolean =>
  err instanceof DOMException && err.name === "AbortError";

export interface SubmitOptions {
  prompt: string;
  flowId: number;
  provider: string;
  model?: string | null;
  selectedNodeIds?: number[];
  insertionContext?: CommandPaletteInsertionContext | null;
}

export const useAiCommandPaletteStore = defineStore("aiCommandPalette", () => {
  const isOpen = ref(false);
  const prompt = ref("");
  const loading = ref(false);
  const error = ref<string | null>(null);
  const aiDisabled = ref(false);
  const degradedReason = ref<CommandPaletteDegradedReason | null>(null);
  const rationale = ref<string | null>(null);
  const refused = ref<CommandPaletteRefusal[]>([]);
  let inflight: AbortController | null = null;

  const cancel = (): void => {
    if (inflight !== null) {
      inflight.abort();
      inflight = null;
    }
  };

  const open = (): void => {
    isOpen.value = true;
  };

  const close = (): void => {
    // Close preserves the prior response (`error` / `refused` /
    // `rationale` / `degradedReason`) so an accidental dismiss doesn't
    // erase what the user was reading. State is cleared at the start of
    // a new submission via `clearResult()`, or by an explicit caller.
    cancel();
    isOpen.value = false;
    loading.value = false;
  };

  const clearResult = (): void => {
    error.value = null;
    aiDisabled.value = false;
    degradedReason.value = null;
    rationale.value = null;
    refused.value = [];
  };

  const toggle = (): void => {
    if (isOpen.value) close();
    else open();
  };

  const setPrompt = (text: string): void => {
    prompt.value = text;
  };

  const submit = async (options: SubmitOptions): Promise<void> => {
    cancel();
    // Wipe any prior response before validating the new prompt so a
    // stale rationale from a previous run can't leak into the next
    // submit's failure UI. Validation errors set `error` after this
    // clear, so the user still sees the new error.
    clearResult();
    const trimmed = options.prompt.trim();
    if (!trimmed) {
      error.value = "Type a request first.";
      return;
    }
    if (!options.provider) {
      error.value = "Configure an AI provider first.";
      return;
    }

    const controller = new AbortController();
    inflight = controller;
    loading.value = true;

    const request: CommandPaletteRequest = {
      flowId: options.flowId,
      prompt: trimmed,
      provider: options.provider,
      model: options.model ?? null,
      selectedNodeIds: options.selectedNodeIds,
      insertionContext: options.insertionContext ?? null,
    };

    let response: CommandPaletteResponse | null = null;
    try {
      response = await submitCommandPalette(request, controller.signal);
    } catch (err) {
      if (isAbortError(err)) {
        // user-initiated cancel; leave state as-is for the next attempt.
        return;
      }
      if (err instanceof AiDisabledError) {
        aiDisabled.value = true;
        error.value = "AI features are disabled.";
        return;
      }
      error.value = err instanceof Error ? err.message : String(err);
      return;
    } finally {
      if (inflight === controller) inflight = null;
      loading.value = false;
    }

    if (!response) return;

    rationale.value = response.rationale;
    refused.value = response.refused;

    if (response.degraded || !response.diff) {
      degradedReason.value = response.reason;
      // Surface a human-readable explanation for the inline error slot.
      if (response.reason === "no_tool_calls") {
        error.value =
          response.rationale ?? "The model couldn't construct a valid action for that prompt.";
      } else if (response.reason === "all_refused") {
        error.value = "Every proposed action was refused — usually a missing column reference.";
      } else if (response.reason === "timeout") {
        error.value = "AI provider timed out — try again in a moment.";
      } else if (response.reason === "provider_error") {
        error.value = "AI provider returned an error.";
      } else if (response.reason === "empty_catalog") {
        error.value = "Cmd+K tool surface is empty (configuration error).";
      } else {
        error.value = "AI couldn't produce a usable result.";
      }
      return;
    }

    // Success path: hand the staged diff to the diff store and close
    // the palette.
    const diffStore = useAiDiffStore();
    diffStore.setCurrentDiff(response.diff);

    // Open the AI drawer so the user sees the diff panel.
    const editorStore = useEditorStore();
    editorStore.openAiDrawer?.();

    // Drop the rationale + refused list now that the diff has been
    // handed off; the diff panel renders both from the diff itself.
    clearResult();
    close();
  };

  return {
    // state
    isOpen,
    prompt,
    loading,
    error,
    aiDisabled,
    degradedReason,
    rationale,
    refused,
    // actions
    open,
    close,
    toggle,
    setPrompt,
    submit,
    cancel,
    clearResult,
  };
});
