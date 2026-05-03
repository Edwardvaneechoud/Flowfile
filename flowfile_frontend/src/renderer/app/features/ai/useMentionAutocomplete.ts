// W24 — `@`-mention autocomplete composable.
//
// Wraps a textarea + its bound text ref to provide:
//   - kind / ref candidates as the user types `@…`
//   - keyboard navigation (Up / Down / Enter / Tab / Escape)
//   - text-replacement on pick, with caret restoration
//
// Owns no I/O — the candidate node list is supplied via a callback so
// the caller controls whether to read from a Pinia store, a Vue Flow
// instance, or a fixture in tests.

import { computed, nextTick, ref, watch, type ComputedRef, type Ref } from "vue";
import {
  buildKindCandidates,
  buildRefCandidates,
  detectActiveTrigger,
  formatMentionInsert,
  type MentionCandidate,
} from "./mentionVocabulary";

export interface MentionAutocompleteApi {
  isOpen: ComputedRef<boolean>;
  candidates: ComputedRef<MentionCandidate[]>;
  activeIndex: Ref<number>;
  triggerSpan: ComputedRef<[number, number] | null>;
  caretPosition: ComputedRef<{ top: number; left: number } | null>;
  onInput: () => void;
  onKeyDown: (event: KeyboardEvent) => boolean;
  pick: (candidate: MentionCandidate) => void;
  setActiveIndex: (index: number) => void;
  close: () => void;
}

export type NodesProvider = () => ReadonlyArray<{
  id: number | string;
  name?: string | null;
}>;

export function useMentionAutocomplete(
  textareaRef: Ref<HTMLTextAreaElement | null>,
  textRef: Ref<string>,
  nodesProvider: NodesProvider,
): MentionAutocompleteApi {
  // Internal trigger state — tracked manually rather than recomputed
  // from `text + caret` on every change so we can `close()` deterministically
  // (e.g. on Escape) and not re-open until the user types again.
  const activeIndex = ref(0);
  const isManuallyClosed = ref(false);
  const triggerSpan = ref<[number, number] | null>(null);
  const triggerKind = ref<MentionCandidate["kind"] | undefined>(undefined);
  const triggerPrefix = ref("");
  const caretPositionState = ref<{ top: number; left: number } | null>(null);

  const candidates = computed<MentionCandidate[]>(() => {
    if (!triggerSpan.value) return [];
    if (triggerKind.value === undefined) {
      return buildKindCandidates(triggerPrefix.value);
    }
    if (triggerKind.value === "node" || triggerKind.value === "schema") {
      return buildRefCandidates(triggerKind.value, triggerPrefix.value, nodesProvider());
    }
    return [];
  });

  const isOpen = computed(() => candidates.value.length > 0 && triggerSpan.value !== null);

  const triggerSpanComputed = computed<[number, number] | null>(() => triggerSpan.value);
  const caretPositionComputed = computed(() => caretPositionState.value);

  // Reset activeIndex any time the candidate set changes so the
  // selection lands on the first row instead of an out-of-range index.
  watch(candidates, () => {
    activeIndex.value = 0;
  });

  const refresh = (): void => {
    const ta = textareaRef.value;
    if (!ta) {
      triggerSpan.value = null;
      caretPositionState.value = null;
      return;
    }
    const caret = ta.selectionStart ?? textRef.value.length;
    const trigger = detectActiveTrigger(textRef.value, caret);
    if (trigger === null || isManuallyClosed.value) {
      triggerSpan.value = null;
      caretPositionState.value = null;
      return;
    }
    triggerSpan.value = trigger.span;
    triggerKind.value = trigger.kind;
    triggerPrefix.value = trigger.refPrefix;
    caretPositionState.value = computeCaretPosition(ta, trigger.span[0]);
  };

  const onInput = (): void => {
    // Any new input invalidates a manual close (Escape state is per-trigger).
    isManuallyClosed.value = false;
    refresh();
  };

  const onKeyDown = (event: KeyboardEvent): boolean => {
    if (!isOpen.value) return false;
    switch (event.key) {
      case "ArrowDown": {
        event.preventDefault();
        const list = candidates.value;
        if (list.length > 0) activeIndex.value = (activeIndex.value + 1) % list.length;
        return true;
      }
      case "ArrowUp": {
        event.preventDefault();
        const list = candidates.value;
        if (list.length > 0)
          activeIndex.value = (activeIndex.value - 1 + list.length) % list.length;
        return true;
      }
      case "Enter":
      case "Tab": {
        const list = candidates.value;
        if (list.length === 0) return false;
        event.preventDefault();
        const target = list[Math.min(activeIndex.value, list.length - 1)];
        pick(target);
        return true;
      }
      case "Escape": {
        event.preventDefault();
        close();
        return true;
      }
      default:
        return false;
    }
  };

  const pick = (candidate: MentionCandidate): void => {
    const span = triggerSpan.value;
    if (!span) return;
    const insert = formatMentionInsert(candidate);
    const before = textRef.value.slice(0, span[0]);
    const after = textRef.value.slice(span[1]);
    textRef.value = before + insert + after;

    // A "kind-only" pick (e.g. picking the `@node:` row) commits the
    // kind + colon and leaves the dropdown open so the next keystroke
    // lands in ref-mode. A "complete" pick (bare kind or a chosen ref)
    // is a commit gesture: keep the dropdown closed until the user
    // starts a new trigger.
    const isKindOnly =
      (candidate.kind === "node" || candidate.kind === "schema") &&
      (candidate.ref === undefined || candidate.ref === "");

    const newCaret = span[0] + insert.length;
    void nextTick(() => {
      const ta = textareaRef.value;
      if (ta) {
        ta.focus();
        ta.setSelectionRange(newCaret, newCaret);
      }
      if (isKindOnly) {
        isManuallyClosed.value = false;
        refresh();
      } else {
        // Commit gesture — close the popup. It re-opens the next time
        // `onInput` fires and `detectActiveTrigger` finds a fresh `@…`.
        close();
      }
    });
  };

  const setActiveIndex = (index: number): void => {
    const list = candidates.value;
    if (list.length === 0) return;
    activeIndex.value = Math.max(0, Math.min(index, list.length - 1));
  };

  const close = (): void => {
    isManuallyClosed.value = true;
    triggerSpan.value = null;
    caretPositionState.value = null;
  };

  return {
    isOpen,
    candidates,
    activeIndex,
    triggerSpan: triggerSpanComputed,
    caretPosition: caretPositionComputed,
    onInput,
    onKeyDown,
    pick,
    setActiveIndex,
    close,
  };
}

/**
 * Approximate the caret pixel position for a textarea offset by mirroring
 * the textarea's styling onto a hidden div and reading the position of a
 * sentinel `<span>` placed at the same character index. The technique is
 * a textbook fix for "where would the caret be?" without introducing a
 * library; ±a few pixels of imprecision is fine for an autocomplete popup.
 */
function computeCaretPosition(
  textarea: HTMLTextAreaElement,
  index: number,
): { top: number; left: number } {
  const mirror = document.createElement("div");
  const computed = window.getComputedStyle(textarea);
  // Copy the relevant styles onto the mirror so wrap behaviour matches.
  const stylesToCopy = [
    "boxSizing",
    "width",
    "height",
    "overflowX",
    "overflowY",
    "borderTopWidth",
    "borderRightWidth",
    "borderBottomWidth",
    "borderLeftWidth",
    "paddingTop",
    "paddingRight",
    "paddingBottom",
    "paddingLeft",
    "fontStyle",
    "fontVariant",
    "fontWeight",
    "fontStretch",
    "fontSize",
    "fontSizeAdjust",
    "lineHeight",
    "fontFamily",
    "textAlign",
    "textTransform",
    "textIndent",
    "textDecoration",
    "letterSpacing",
    "wordSpacing",
    "tabSize",
    "whiteSpace",
    "wordWrap",
    "overflowWrap",
  ] as const;
  for (const prop of stylesToCopy) {
    (mirror.style as unknown as Record<string, string>)[prop] = computed[prop];
  }
  mirror.style.position = "absolute";
  mirror.style.visibility = "hidden";
  mirror.style.top = "0";
  mirror.style.left = "-9999px";

  const before = textarea.value.slice(0, index);
  const sentinel = document.createElement("span");
  sentinel.textContent = "​"; // zero-width space; non-empty so getBoundingClientRect works.

  const beforeText = document.createTextNode(before);
  mirror.appendChild(beforeText);
  mirror.appendChild(sentinel);
  document.body.appendChild(mirror);

  const sentinelRect = sentinel.getBoundingClientRect();
  const taRect = textarea.getBoundingClientRect();

  // Position relative to the textarea's top-left, accounting for scroll.
  const top = sentinelRect.top - taRect.top - textarea.scrollTop + sentinelRect.height;
  const left = sentinelRect.left - taRect.left - textarea.scrollLeft;

  document.body.removeChild(mirror);
  return { top, left };
}
