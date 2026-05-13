// Pinia store for the edge ghost-node surface.
//
// Distinct from the autocomplete store because the lifecycle is
// different: edge-hover ghost suggestions surface a small popover that
// the user clicks to materialise, with cancel-on-leave semantics. The
// autocomplete store cancels per-keystroke; this one cancels
// per-hover. Sharing one AbortController would mean a hover-flick
// cancels an active formula suggestion fetch.
//
// `materialize()` performs the three-step graph-mutation dance the
// AI's validated suggestion implies:
//   1. POST editor/add_node/?flow_id&node_id&node_type&pos_x&pos_y
//   2. POST update_settings?node_type with the validated settings dict
//   3. POST editor/connect_node/ with the upstream→ghost connection
//
// All three already exist as production endpoints — this store leans
// on them rather than introducing a new "atomic apply" path. The
// GraphDiff staging mechanism owns multi-suggestion atomic apply;
// ghost nodes ship single-suggestion materialisation with the existing
// per-step undo semantics.

import axios from "axios";
import { defineStore } from "pinia";
import { ref } from "vue";

import {
  AiDisabledError,
  fetchNextNodeSuggestions,
  NextNodeSuggestion,
  NextNodeSuggestionsResponse,
  SuggestNextNodeRequest,
} from "../api/ai.api";
import { FlowApi } from "../api/flow.api";
import type { NodeConnection } from "../types/canvas.types";

const isAbortError = (err: unknown): boolean => {
  if (err instanceof DOMException && err.name === "AbortError") return true;
  if (err && typeof err === "object" && (err as { name?: string }).name === "CanceledError")
    return true;
  return false;
};

export interface GhostNodeAnchor {
  upstreamNodeId: number | string;
  edgeMidX: number;
  edgeMidY: number;
}

export const useAiGhostNodeStore = defineStore("aiGhostNode", () => {
  const inflight = ref<AbortController | null>(null);
  const suggestions = ref<NextNodeSuggestion[]>([]);
  const anchor = ref<GhostNodeAnchor | null>(null);
  const isLoading = ref(false);
  const lastError = ref<unknown>(null);
  const aiDisabled = ref(false);
  const degradedReason = ref<string | null>(null);

  const cancel = (): void => {
    if (inflight.value !== null) {
      inflight.value.abort();
      inflight.value = null;
    }
    isLoading.value = false;
  };

  const clear = (): void => {
    cancel();
    suggestions.value = [];
    anchor.value = null;
    degradedReason.value = null;
  };

  const _replaceController = (): AbortController => {
    cancel();
    const c = new AbortController();
    inflight.value = c;
    return c;
  };

  const requestSuggestions = async (
    body: SuggestNextNodeRequest,
    anchorPos: GhostNodeAnchor,
  ): Promise<NextNodeSuggestionsResponse | null> => {
    const controller = _replaceController();
    isLoading.value = true;
    anchor.value = anchorPos;
    suggestions.value = [];
    degradedReason.value = null;

    try {
      const result = await fetchNextNodeSuggestions(body, controller.signal);
      lastError.value = null;
      aiDisabled.value = false;
      suggestions.value = result.suggestions;
      degradedReason.value = result.degraded ? (result.reason ?? "degraded") : null;
      return result;
    } catch (error) {
      if (isAbortError(error)) {
        return null;
      }
      if (error instanceof AiDisabledError) {
        aiDisabled.value = true;
        lastError.value = null;
        anchor.value = null;
        return null;
      }
      lastError.value = error;
      anchor.value = null;
      return null;
    } finally {
      if (inflight.value === controller) {
        inflight.value = null;
        isLoading.value = false;
      }
    }
  };

  const _allocateNodeId = (): number => {
    // Mirrors the convention used elsewhere in the renderer for client-allocated
    // node ids: a high-resolution timestamp suffix keeps collisions astronomically
    // unlikely and the backend treats node_id as opaque.
    return Date.now() + Math.floor(Math.random() * 1000);
  };

  /** Materialise a single ghost suggestion as a real node + edge.
   *
   * Returns the freshly-allocated node id on success, or `null` on failure
   * (the caller can leave the popover up and surface a toast / log entry).
   *
   * Three-call sequence — each existing in production today, no new endpoints:
   *
   *   1. ``editor/add_node/`` — empty node of the requested type at the
   *      requested position.
   *   2. ``update_settings`` — apply the validated settings the LLM produced.
   *   3. ``editor/connect_node/`` — wire upstream → ghost.
   *
   * Atomicity is per-step (existing UNDO covers each). The GraphDiff
   * surface owns multi-step atomic apply; this is intentionally the
   * simpler path.
   */
  const materialize = async (
    suggestion: NextNodeSuggestion,
    flowId: number,
    upstreamNodeId: number,
    posX: number,
    posY: number,
  ): Promise<number | null> => {
    const newNodeId = _allocateNodeId();
    try {
      await FlowApi.insertNode(flowId, newNodeId, suggestion.nodeType, posX, posY);
      // settings carry flow_id / node_id from the LLM's perspective — overwrite
      // with the real ones before sending. The backend update_settings handler
      // ignores stale flow_id but accurate values keep audit logs honest.
      const settingsBody = {
        ...suggestion.settings,
        flow_id: flowId,
        node_id: newNodeId,
      };
      await axios.post("update_settings", settingsBody, {
        params: { node_type: suggestion.nodeType },
        headers: { "Content-Type": "application/json", accept: "application/json" },
      });
      const connection: NodeConnection = {
        input_connection: {
          node_id: newNodeId,
          connection_class: "input-0",
        },
        output_connection: {
          node_id: upstreamNodeId,
          connection_class: "output-0",
        },
      };
      await FlowApi.connectNode(flowId, connection);
      return newNodeId;
    } catch (error) {
      lastError.value = error;
      return null;
    } finally {
      // Always close the popover after a materialise attempt — the user has
      // committed to one (or hit an error); either way the hover affordance
      // should yield to the canvas.
      clear();
    }
  };

  return {
    inflight,
    suggestions,
    anchor,
    isLoading,
    lastError,
    aiDisabled,
    degradedReason,
    cancel,
    clear,
    requestSuggestions,
    materialize,
  };
});
