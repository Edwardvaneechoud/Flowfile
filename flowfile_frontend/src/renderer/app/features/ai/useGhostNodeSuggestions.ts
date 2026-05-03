// Edge-hover wiring for the W32 ghost-node surface.
//
// VueFlow exposes per-edge mouse events on `<VueFlow @edge-mouse-enter>` /
// `@edge-mouse-leave`. This composable wraps those into a debounced request
// to the AI store, anchored at the edge midpoint, and returns visibility
// state the popup component can render against.
//
// Debounce is 400 ms — short enough that a deliberate hover commits, long
// enough that a hover-flick across multiple edges doesn't fire N requests.
// W14 rate-limit accounting still applies if the timer leaks (it shouldn't),
// so this is best-effort UX rather than a hard contract.
//
// Lifecycle:
//   - on edge-mouse-enter: schedule a request after the debounce window
//   - on edge-mouse-leave: cancel any pending timer + cancel the inflight call
//   - on viewport click: clear the popup (handled by the consumer; the
//     composable just exposes `clear()`)
//
// The composable is read-only — `requestSuggestions` and `materialize` go
// through the Pinia store so cancellation semantics are shared with any
// other surface that wants to interrupt the hover request.

import type { GraphEdge } from "@vue-flow/core";
import { computed, onUnmounted, ref } from "vue";

import { NextNodeSuggestion } from "../../api/ai.api";
import { useAiGhostNodeStore } from "../../stores/ai-ghost-node-store";

const DEBOUNCE_MS = 400;

export interface GhostNodeOptions {
  /** Provider override — defaults to the store's default (anthropic). */
  provider?: string;
  /** When set, requests skip the debounce window (used in tests). */
  skipDebounce?: boolean;
}

/** Subset of VueFlow's ``GraphEdge`` we read.
 *
 * Pulled out so the composable doesn't pin a specific edge `Data` /
 * `CustomEvents` shape — VueFlow's full ``GraphEdge`` type takes generics
 * that bubble up nastily through the props chain.
 */
type GhostEdgePayload = Pick<
  GraphEdge,
  "id" | "source" | "target" | "sourceX" | "sourceY" | "targetX" | "targetY"
>;

export const useGhostNodeSuggestions = (options: GhostNodeOptions = {}) => {
  const store = useAiGhostNodeStore();
  let pendingTimer: ReturnType<typeof setTimeout> | null = null;
  const flowIdRef = ref<number | null>(null);
  const lastEdgeId = ref<string | null>(null);

  const _clearTimer = (): void => {
    if (pendingTimer !== null) {
      clearTimeout(pendingTimer);
      pendingTimer = null;
    }
  };

  /** Caller wires this into VueFlow's `<VueFlow @edge-mouse-enter>` event.
   *
   * `flowId` MUST be supplied — there's no canonical "current flow" coupling
   * at the composable layer because consumers vary (designer vs preview).
   * Calling without a `flowId` is a no-op.
   */
  const onEdgeMouseEnter = (
    payload: { edge: GhostEdgePayload; event?: unknown },
    flowId: number | null,
  ): void => {
    if (flowId === null) return;
    flowIdRef.value = flowId;
    const edge = payload.edge;
    if (!edge?.source) return;
    lastEdgeId.value = edge.id ?? `${edge.source}->${edge.target}`;

    const upstreamNodeId = edge.source;
    const sourceX = edge.sourceX ?? 0;
    const sourceY = edge.sourceY ?? 0;
    const targetX = edge.targetX ?? sourceX;
    const targetY = edge.targetY ?? sourceY;

    const anchor = {
      upstreamNodeId,
      edgeMidX: (sourceX + targetX) / 2,
      edgeMidY: (sourceY + targetY) / 2,
    };

    _clearTimer();

    const fire = () => {
      pendingTimer = null;
      void store.requestSuggestions(
        {
          flowId,
          upstreamNodeId,
          provider: options.provider,
        },
        anchor,
      );
    };

    if (options.skipDebounce) {
      fire();
    } else {
      pendingTimer = setTimeout(fire, DEBOUNCE_MS);
    }
  };

  const onEdgeMouseLeave = (): void => {
    _clearTimer();
    // Don't clear suggestions immediately — the user may be moving toward the
    // popup. Component-level handlers close the popup on outside click.
  };

  /** Caller wires this into the canvas's pane click handler so the popup
   * disappears when the user clicks anywhere off the suggestion list. */
  const onViewportClick = (): void => {
    _clearTimer();
    store.clear();
  };

  const acceptSuggestion = async (
    indexOrSuggestion: number | NextNodeSuggestion,
  ): Promise<number | null> => {
    if (flowIdRef.value === null) return null;
    const currentAnchor = store.anchor;
    if (currentAnchor === null) return null;
    const list = store.suggestions;
    const suggestion =
      typeof indexOrSuggestion === "number" ? list[indexOrSuggestion] : indexOrSuggestion;
    if (!suggestion) return null;
    // Round-trip the upstream id through Number() — the AI route accepts
    // string OR number; the materialise path needs an integer for
    // ``editor/add_node/``'s NodePromise shape.
    const upstreamId = Number(currentAnchor.upstreamNodeId);
    if (Number.isNaN(upstreamId)) return null;
    return store.materialize(
      suggestion,
      flowIdRef.value,
      upstreamId,
      currentAnchor.edgeMidX,
      currentAnchor.edgeMidY + 80,
    );
  };

  const isVisible = computed(
    () =>
      store.anchor !== null &&
      (store.suggestions.length > 0 || store.isLoading || store.degradedReason !== null),
  );

  onUnmounted(() => {
    _clearTimer();
    store.cancel();
  });

  return {
    isVisible,
    suggestions: computed(() => store.suggestions),
    anchor: computed(() => store.anchor),
    isLoading: computed(() => store.isLoading),
    aiDisabled: computed(() => store.aiDisabled),
    degradedReason: computed(() => store.degradedReason),
    lastEdgeId: computed(() => lastEdgeId.value),
    onEdgeMouseEnter,
    onEdgeMouseLeave,
    onViewportClick,
    acceptSuggestion,
    clear: () => {
      _clearTimer();
      store.clear();
    },
  };
};
