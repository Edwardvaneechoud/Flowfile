/**
 * Keeps this window in step with core's change feed for the active flow (GET editor/events).
 *
 * A change another client made (a second tab, another user, a pop-out window) arrives as an
 * event and becomes the same signal a local change raises: `requestReload` for the graph, which
 * re-renders the canvas, the notebook and the dirty state through `loadFlow`'s history update;
 * `requestRunStateRefresh` for a run, which the header turns into the polling a flow load starts
 * or stops. Events this window caused itself (same `clientId`) only record the revision.
 */
import { defineStore } from "pinia";
import { ref, watch } from "vue";
import authService from "../services/auth.service";
import { clientId } from "../services/clientId";
import {
  FlowEventsHttpError,
  streamFlowEvents,
  type FlowEvent,
} from "../services/flowEventsClient";
import { useEditorStore } from "./editor-store";
import { useFlowStore } from "./flow-store";

/** Foreign graph events within this window are one reload. */
export const RELOAD_DEBOUNCE_MS = 150;
export const RETRY_BASE_MS = 1000;
export const RETRY_MAX_MS = 30_000;

const sleep = (ms: number, signal: AbortSignal): Promise<void> =>
  new Promise((resolve) => {
    if (signal.aborted) return resolve();
    const timer = setTimeout(done, ms);
    function done() {
      signal.removeEventListener("abort", done);
      clearTimeout(timer);
      resolve();
    }
    signal.addEventListener("abort", done);
  });

const isAbort = (error: unknown): boolean =>
  (error as { name?: string } | null)?.name === "AbortError";

export const useFlowSyncStore = defineStore("flow-sync", () => {
  const flowStore = useFlowStore();
  const connected = ref(false);
  /** Per flow, the last revision this window saw: its own responses and every event. */
  const lastRevision = ref<Record<number, number>>({});
  /** The flow whose stream last ended with `closed`, and a counter so the same id closing again is seen. */
  const closedFlowId = ref<number | null>(null);
  const closeCount = ref(0);
  /** The last `rekeyed` event: the flow moved from one id to another (a Save As). */
  const rekeyedTo = ref<{ from: number; to: number } | null>(null);

  let controller: AbortController | null = null;
  let activeFlowId: number | null = null;
  let reloadTimer: ReturnType<typeof setTimeout> | null = null;

  function noteRevision(flowId: number, revision: number | null | undefined) {
    if (typeof revision === "number") lastRevision.value[flowId] = revision;
  }

  function scheduleReload() {
    if (reloadTimer) clearTimeout(reloadTimer);
    reloadTimer = setTimeout(() => {
      reloadTimer = null;
      flowStore.requestReload();
    }, RELOAD_DEBOUNCE_MS);
  }

  function handleEvent(flowId: number, event: FlowEvent) {
    if (event.flow_id !== flowId) return;
    if (event.kind === "hello") {
      const known = lastRevision.value[flowId];
      const missed = known !== undefined && event.revision != null && event.revision !== known;
      noteRevision(flowId, event.revision);
      if (missed) scheduleReload();
      if (event.is_running !== undefined && event.is_running !== useEditorStore().isRunning) {
        flowStore.requestRunStateRefresh();
      }
      return;
    }
    noteRevision(flowId, event.revision);
    if (event.kind === "closed") {
      closedFlowId.value = flowId;
      closeCount.value += 1;
      endStream(flowId);
      return;
    }
    if (event.kind === "rekeyed") {
      if (typeof event.new_flow_id === "number") {
        rekeyedTo.value = { from: flowId, to: event.new_flow_id };
      }
      endStream(flowId);
      return;
    }
    if (event.origin === clientId) return;
    switch (event.kind) {
      case "graph":
        scheduleReload();
        break;
      case "run_started":
      case "run_ended":
        flowStore.requestRunStateRefresh();
        break;
      case "saved":
        useEditorStore().bumpGraphVersion();
        break;
    }
  }

  async function run(flowId: number, signal: AbortSignal) {
    let attempt = 0;
    while (!signal.aborted) {
      try {
        const token = await authService.getToken();
        if (!token) throw new Error("No token for the change feed");
        await streamFlowEvents({
          flowId,
          token,
          clientId,
          signal,
          onOpen: () => {
            connected.value = true;
            attempt = 0;
          },
          onEvent: (event) => handleEvent(flowId, event),
        });
      } catch (error) {
        if (signal.aborted || isAbort(error)) break;
        if (
          error instanceof FlowEventsHttpError &&
          (error.status === 403 || error.status === 404)
        ) {
          break;
        }
      } finally {
        connected.value = false;
      }
      if (signal.aborted) break;
      // Core ended the stream (a restart) or the connection dropped: back off, then reconnect.
      attempt += 1;
      await sleep(Math.min(RETRY_BASE_MS * 2 ** (attempt - 1), RETRY_MAX_MS), signal);
    }
    connected.value = false;
  }

  function start(flowId: number) {
    stop();
    controller = new AbortController();
    activeFlowId = flowId;
    void run(flowId, controller.signal);
  }

  function stop() {
    if (reloadTimer) {
      clearTimeout(reloadTimer);
      reloadTimer = null;
    }
    controller?.abort();
    controller = null;
    activeFlowId = null;
    connected.value = false;
  }

  /** A final event ends its own stream only: one that lands after a flow switch must not abort the next flow's. */
  function endStream(flowId: number) {
    if (activeFlowId === flowId) stop();
  }

  watch(
    () => flowStore.historyState,
    (state) => {
      if (state.flow_id != null) noteRevision(state.flow_id, state.revision);
    },
  );

  watch(
    () => flowStore.flowId,
    (flowId) => {
      if (flowId > 0) start(flowId);
      else stop();
    },
    { immediate: true },
  );

  return {
    connected,
    lastRevision,
    closedFlowId,
    closeCount,
    rekeyedTo,
    noteRevision,
    handleEvent,
    start,
    stop,
  };
});
