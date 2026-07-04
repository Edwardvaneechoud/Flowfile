// Popover-side wiring for the ghost-node surface.
//
// The suggestion is "what node should follow THIS node?" — triggered by a
// deliberate action on a node (right-click → "Suggest next node…"), which
// calls `store.beginIntent()`. This composable powers the Canvas-rendered
// popover that completes the flow:
//   1. (trigger, elsewhere) store.beginIntent(anchor, flowId) — opens the
//      inline intent input at the right-clicked node.
//   2. submitIntent(text) — sends the user's intent to the AI store; the
//      popover shows loading, then the proposed node(s) as a ghost to confirm.
//   3. acceptSuggestion(index) — materialises the chosen ghost as a real node
//      wired downstream of the source node.
//
// The composable is thin — flow id, cancellation, request state, and
// materialisation all live in the Pinia store so the trigger (a node
// component) and the popover (Canvas) share the same state.

import { computed, onUnmounted } from "vue";

import { NextNodeSuggestion } from "../../api/ai.api";
import { useAiGhostNodeStore } from "../../stores/ai-ghost-node-store";

// Rightward offset (flow coords) for the materialised node relative to its
// source. Flows read left→right (outputs on the right, inputs on the left), so
// the next node lands to the right of the one it follows rather than below it.
const NODE_DROP_OFFSET_X = 220;

export interface GhostNodeOptions {
  /** Provider override — defaults to the store's per-surface resolution. */
  provider?: string;
  /** Cap on how many candidates the AI returns. */
  maxSuggestions?: number;
}

export const useGhostNodeSuggestions = (options: GhostNodeOptions = {}) => {
  const store = useAiGhostNodeStore();

  /** Fire the request once the user has typed (or skipped) their intent. */
  const submitIntent = (intent: string): void => {
    const anchor = store.anchor;
    const flowId = store.flowId;
    if (anchor === null || flowId === null) return;
    void store.requestSuggestions(
      {
        flowId,
        upstreamNodeId: anchor.upstreamNodeId,
        provider: options.provider,
        intent: intent.trim() || null,
        maxSuggestions: options.maxSuggestions,
      },
      anchor,
    );
  };

  /** Caller wires this into the canvas pane click so an outside click dismisses
   * the popup (input or suggestions). Ignored while a request is in flight —
   * a stray canvas click shouldn't discard a suggestion mid-generation. */
  const onViewportClick = (): void => {
    if (store.isLoading) return;
    store.clear();
  };

  const acceptSuggestion = async (
    indexOrSuggestion: number | NextNodeSuggestion,
  ): Promise<number | null> => {
    const flowId = store.flowId;
    const currentAnchor = store.anchor;
    if (flowId === null || currentAnchor === null) return null;
    const list = store.suggestions;
    const suggestion =
      typeof indexOrSuggestion === "number" ? list[indexOrSuggestion] : indexOrSuggestion;
    if (!suggestion) return null;
    // The AI route accepts string OR number, but the materialise path needs an
    // integer node id for ``editor/add_node/``'s NodePromise shape.
    const upstreamId = Number(currentAnchor.upstreamNodeId);
    if (Number.isNaN(upstreamId)) return null;
    return store.materialize(
      suggestion,
      flowId,
      upstreamId,
      currentAnchor.nodeX + NODE_DROP_OFFSET_X,
      currentAnchor.nodeY,
    );
  };

  const isVisible = computed(
    () =>
      store.anchor !== null &&
      (store.awaitingIntent ||
        store.suggestions.length > 0 ||
        store.isLoading ||
        store.degradedReason !== null),
  );

  onUnmounted(() => {
    store.cancel();
  });

  return {
    isVisible,
    awaitingIntent: computed(() => store.awaitingIntent),
    suggestions: computed(() => store.suggestions),
    anchor: computed(() => store.anchor),
    isLoading: computed(() => store.isLoading),
    aiDisabled: computed(() => store.aiDisabled),
    degradedReason: computed(() => store.degradedReason),
    submitIntent,
    onViewportClick,
    acceptSuggestion,
    clear: () => store.clear(),
  };
};
