import {
  ref,
  shallowRef,
  shallowReactive,
  computed,
  onUnmounted,
  Ref,
  markRaw,
  watch,
  effectScope,
  reactive,
  type Component,
} from "vue";
import axios from "axios";
import { ElNotification } from "element-plus";
import { Promotion, DataLine } from "@element-plus/icons-vue";
import { useNodeStore } from "../stores/column-store";
import { useEditorStore } from "../stores/editor-store";
import { useResultsStore } from "../stores/results-store";
import { useFlowSyncStore } from "../stores/flow-sync-store";
import { useTutorialStore } from "../stores/tutorial-store";
import { VueFlowStore } from "@vue-flow/core";
import type { RunInformation, FlowSettings } from "../types";
import { FlowApi } from "../api";

interface PollingConfig {
  interval?: number;
  enabled?: boolean;
  maxAttempts?: number;
}

interface NotificationConfig {
  title: string;
  message: string;
  type: "success" | "error";
}

// Timer lifecycle is shared; entries still belong to either the persistent map or one caller.
class PollingEntry {
  interval: number | null;

  constructor(
    public check: () => Promise<void>,
    public delay: number,
    public dispose: (() => void) | undefined,
    public completionCallbacks: Array<() => void>,
    public target?: OperationTarget,
  ) {
    this.interval = setInterval(check, delay) as unknown as number;
  }

  pause() {
    if (this.interval !== null) clearInterval(this.interval);
    this.interval = null;
  }

  resume(delay = this.delay) {
    if (this.interval !== null) return;
    if (this.target) {
      this.target.failures = 0;
      this.target.paused = false;
    }
    this.interval = setInterval(this.check, delay) as unknown as number;
  }

  stop() {
    this.pause();
    this.dispose?.();
  }
}

// Singleton state that persists across component instances
class FlowExecutionState {
  private static instance: FlowExecutionState;
  private pollingIntervals = new Map<string, PollingEntry>();
  private activeExecutions = new Map<
    number,
    { starting?: boolean; cancelled?: boolean; terminal?: boolean; owner?: OperationTarget }
  >();

  static getInstance(): FlowExecutionState {
    if (!FlowExecutionState.instance) {
      FlowExecutionState.instance = new FlowExecutionState();
    }
    return FlowExecutionState.instance;
  }

  setPollingInterval(key: string, entry: PollingEntry) {
    this.pollingIntervals.set(key, entry);
  }

  hasStatusPolling(id: number): boolean {
    return [...this.pollingIntervals.values()].some((entry) => entry.target?.id === id);
  }

  checkStatusPolling(id: number): boolean {
    this.resumeStatusPolling(id);
    const owners = [...this.pollingIntervals.values()].filter((entry) => entry.target?.id === id);
    owners.forEach((entry) => void entry.check());
    return owners.length > 0;
  }

  getPollingEntry(key: string) {
    return this.pollingIntervals.get(key);
  }

  pausePollingInterval(key: string) {
    this.pollingIntervals.get(key)?.pause();
  }

  resumeStatusPolling(id: number) {
    this.pollingIntervals.forEach((entry) => {
      if (entry.target?.id === id) entry.resume();
    });
  }

  addCompletionCallbacks(key: string, completionCallbacks: Array<() => void>) {
    this.pollingIntervals.get(key)?.completionCallbacks.push(...completionCallbacks.splice(0));
  }

  getOperationOwner(id: number) {
    const owner =
      [...this.pollingIntervals.values()].find((entry) => entry.target?.id === id)?.target ??
      this.activeExecutions.get(id)?.owner;
    return owner?.stopped ? undefined : owner;
  }

  clearPollingInterval(key: string) {
    const interval = this.pollingIntervals.get(key);
    if (interval) {
      interval.stop();
      this.pollingIntervals.delete(key);
    }
  }

  setStarting(id: number, starting: boolean, owner?: OperationTarget) {
    this.activeExecutions.set(id, {
      ...this.activeExecutions.get(id),
      starting,
      ...(starting ? { terminal: false, owner } : {}),
    });
  }

  isStarting(id: number): boolean {
    return this.activeExecutions.get(id)?.starting || false;
  }

  setTerminal(id: number) {
    this.activeExecutions.set(id, {
      ...this.activeExecutions.get(id),
      starting: false,
      terminal: true,
    });
  }

  isTerminal(id: number): boolean {
    return this.activeExecutions.get(id)?.terminal || false;
  }

  rekeyExecution(from: number, to: number) {
    const execution = this.activeExecutions.get(from);
    if (execution) {
      this.activeExecutions.set(to, { ...this.activeExecutions.get(to), ...execution });
      this.activeExecutions.delete(from);
    }
  }

  setCancelled(id: number, cancelled: boolean) {
    this.activeExecutions.set(id, { ...this.activeExecutions.get(id), cancelled });
  }

  isCancelled(id: number): boolean {
    return this.activeExecutions.get(id)?.cancelled || false;
  }

  clearAll() {
    this.pollingIntervals.forEach((_, key) => this.clearPollingInterval(key));
    this.pollingIntervals.clear();
    this.activeExecutions.clear();
  }

  rekeyPollingInterval(from: string, to: string) {
    if (from === to) return;
    const interval = this.pollingIntervals.get(from);
    if (!interval) return;
    const destination = this.pollingIntervals.get(to);
    if (destination) {
      if (interval.target && destination.target) {
        interval.target.transferredTo = destination.target;
        destination.target.pendingCancel ??= interval.target.pendingCancel;
      }
      destination.completionCallbacks.push(...interval.completionCallbacks.splice(0));
      this.clearPollingInterval(from);
      return;
    }
    this.pollingIntervals.set(to, interval);
    this.pollingIntervals.delete(from);
  }
}

const isResponseSuccessful = (status: number): boolean => status >= 200 && status < 300;

// Starts in progress, marked synchronously: a second click during the pre-run save must not start a second run.
const startsInProgress = new Set<string>();

export const resetFlowExecutionState = () => {
  FlowExecutionState.getInstance().clearAll();
  startsInProgress.clear();
};

interface OperationTarget {
  id: number;
  stop: () => void;
  polling?: boolean;
  silent?: boolean;
  stopped?: boolean;
  paused?: boolean;
  failures?: number;
  generation?: number;
  confirmedRunning?: boolean;
  startPolling?: () => void;
  check?: () => Promise<void>;
  transferredTo?: OperationTarget;
  cancelOwner?: OperationTarget;
  pendingCancel?: OperationTarget;
}

const currentOwner = (target: OperationTarget): OperationTarget =>
  target.transferredTo ? currentOwner(target.transferredTo) : target;

const startOnce = async (key: string, start: () => Promise<void>): Promise<void> => {
  if (startsInProgress.has(key)) return;
  startsInProgress.add(key);
  try {
    await start();
  } finally {
    startsInProgress.delete(key);
  }
};

const getRunStatus = async (flowId: number) => {
  const response = await axios.get("/flow/run_status/", {
    params: { flow_id: flowId },
    headers: { accept: "application/json" },
  });
  return response;
};

export function useFlowExecution(
  // Accept a getter (`() => number`) or a Ref so the composable always reads
  // the *current* flow_id. Passing a raw number freezes the value at setup
  // time — fine only for components whose flow id is fixed for their entire
  // lifetime. After Save As re-keys to a new id, callers that captured the
  // old number keep polling stale endpoints.
  flowId: Ref<number> | number | (() => number),
  pollingConfig: PollingConfig = {
    interval: 2000,
    enabled: true,
    maxAttempts: 5,
  },
  options: {
    persistPolling?: boolean; // If true, polling continues even when component unmounts
    pollingKey?: string; // Custom key for tracking this specific polling instance
  } = {},
) {
  const nodeStore = useNodeStore();
  const editorStore = useEditorStore();
  const resultsStore = useResultsStore();
  const flowSync = useFlowSyncStore();
  const state = FlowExecutionState.getInstance();
  const localPollingEntry = shallowRef<PollingEntry>();
  const isExecuting = ref(false);
  const cancelTarget = ref<OperationTarget | null>(null);

  const getFlowId = () => {
    if (typeof flowId === "function") return flowId();
    return typeof flowId === "number" ? flowId : flowId.value;
  };
  const isSelectedFlow = (id: number) => nodeStore.flow_id === id;
  const isCancelling = computed(() => {
    const pending = cancelTarget.value;
    return pending?.id === getFlowId() && !currentOwner(pending.cancelOwner ?? pending).stopped;
  });

  const getPollingKey = (suffix = "", id = getFlowId()) => {
    const customKey = options.pollingKey || `flow_${id}`;
    return suffix ? `${customKey}_${suffix}` : customKey;
  };

  const captureTarget = (id: number, suffix = ""): OperationTarget => {
    const scope = effectScope(true);
    const target: OperationTarget = reactive({
      id,
      stop: () => {
        target.stopped = true;
        scope.stop();
      },
    });
    scope.run(() =>
      watch(
        () => flowSync.rekeyedTo,
        (move) => {
          if (move?.from !== target.id) return;
          const previousId = target.id;
          state.rekeyExecution(previousId, move.to);
          target.id = move.to;
          if (options.persistPolling)
            state.rekeyPollingInterval(
              getPollingKey(suffix, previousId),
              getPollingKey(suffix, target.id),
            );
        },
        { flush: "sync" },
      ),
    );
    return target;
  };

  const withTarget = async (action: (target: OperationTarget) => Promise<void>, suffix = "") => {
    const target = captureTarget(getFlowId(), suffix);
    try {
      await action(target);
    } finally {
      if (!target.polling) target.stop();
    }
  };

  // Flow control methods
  const freezeFlow = () => {
    const vueFlowElement: VueFlowStore = nodeStore.vueFlowInstance;
    if (vueFlowElement) {
      vueFlowElement.nodesDraggable.value = false;
      vueFlowElement.nodesConnectable.value = false;
      vueFlowElement.elementsSelectable.value = false;
    }
  };

  const unFreezeFlow = () => {
    const vueFlowElement: VueFlowStore = nodeStore.vueFlowInstance;
    if (vueFlowElement) {
      vueFlowElement.nodesDraggable.value = true;
      vueFlowElement.nodesConnectable.value = true;
      vueFlowElement.elementsSelectable.value = true;
    }
  };

  const showNotification = (
    title: string,
    message: string,
    type?: "success" | "error",
    dangerouslyUseHTMLString?: boolean,
    icon?: Component,
  ) => {
    ElNotification({
      title,
      message,
      type,
      position: "top-left",
      dangerouslyUseHTMLString,
      ...(icon ? { icon: markRaw(icon) } : {}),
    });
  };

  // Polling management with persistence option
  const startPolling = (
    checkFn: () => Promise<void>,
    pollingKeySuffix = "",
    id = getFlowId(),
    dispose?: () => void,
    completionCallbacks: Array<() => void> = [],
    target?: OperationTarget,
  ) => {
    const key = getPollingKey(pollingKeySuffix, id);

    const existing = options.persistPolling ? state.getPollingEntry(key) : isPollingActive();
    if (!existing && pollingConfig.enabled) {
      const entry = new PollingEntry(
        checkFn,
        pollingConfig.interval || 2000,
        dispose,
        completionCallbacks,
        target,
      );
      if (options.persistPolling) state.setPollingInterval(key, entry);
      else localPollingEntry.value = shallowReactive(entry);
      return true;
    }
    if (options.persistPolling) {
      state.resumeStatusPolling(id);
      state.addCompletionCallbacks(key, completionCallbacks);
    } else localPollingEntry.value?.resume(pollingConfig.interval || 2000);
    dispose?.();
    return false;
  };

  const startStatusPolling = (targetOrId: OperationTarget | number, focusResultPanels = true) => {
    const target = typeof targetOrId === "number" ? captureTarget(targetOrId) : targetOrId;
    if (target.stopped) return;
    if (typeof targetOrId === "number" && isSelectedFlow(target.id))
      state.setCancelled(target.id, editorStore.isCanceled);
    if (target.polling) {
      state.resumeStatusPolling(target.id);
      localPollingEntry.value?.resume(pollingConfig.interval || 2000);
      return;
    }
    if (typeof targetOrId === "number" && state.isStarting(target.id)) {
      const owner = state.getOperationOwner(target.id);
      if (owner) owner.confirmedRunning = true;
      target.stop();
      return;
    }
    if (typeof targetOrId === "number" && state.hasStatusPolling(target.id)) {
      state.resumeStatusPolling(target.id);
      target.stop();
      return;
    }
    if (typeof targetOrId === "number")
      state.setCancelled(target.id, isSelectedFlow(target.id) && editorStore.isCanceled);
    target.check = () => checkRunStatus(undefined, "", focusResultPanels, undefined, target);
    target.polling = startPolling(target.check, "", target.id, target.stop, [], target);
  };

  const stopPolling = (pollingKeySuffix = "", id = getFlowId()) => {
    if (options.persistPolling) {
      const key = getPollingKey(pollingKeySuffix, id);
      state.clearPollingInterval(key);
    } else {
      localPollingEntry.value?.stop();
      localPollingEntry.value = undefined;
    }
  };

  const isPollingActive = (pollingKeySuffix = ""): boolean => {
    if (options.persistPolling) {
      const key = getPollingKey(pollingKeySuffix);
      // A paused status timer still owns an operation with an unknown outcome.
      return !!state.getPollingEntry(key);
    }
    return (
      !!localPollingEntry.value &&
      (localPollingEntry.value.dispose !== undefined || localPollingEntry.value.interval !== null)
    );
  };

  const createNotificationConfig = (runInfo: RunInformation): NotificationConfig => ({
    title: runInfo.success ? "Success" : "Error",
    message: runInfo.success
      ? "The operation has completed successfully"
      : "There were issues with the operation, check the logging for more information",
    type: runInfo.success ? "success" : "error",
  });

  const checkRunStatus = async (
    customSuccessMessage?: string,
    pollingKeySuffix = "",
    focusResultPanels = true,
    onComplete?: () => void,
    targetId?: OperationTarget,
  ) => {
    const getTargetId = () => targetId?.id ?? getFlowId();
    const requestedId = getTargetId();
    const generation = targetId?.generation;
    let notified = false;
    const finish = () => {
      state.setTerminal(requestedId);
      stopPolling(pollingKeySuffix, requestedId);
      targetId?.stop();
      if (isSelectedFlow(requestedId)) {
        unFreezeFlow();
        editorStore.isRunning = false;
        editorStore.isCanceled = false;
        isExecuting.value = false;
      }
      if (!notified) {
        notified = true;
        onComplete?.();
      }
    };

    try {
      const response = await getRunStatus(requestedId);
      if (
        targetId?.stopped ||
        targetId?.paused ||
        generation !== targetId?.generation ||
        requestedId !== getTargetId()
      )
        return;
      if (isResponseSuccessful(response.status) && targetId) targetId.failures = 0;
      if (!isResponseSuccessful(response.status) && response.status !== 404)
        throw new Error(`Unexpected run status: ${response.status}`);
      if (isResponseSuccessful(response.status) && isSelectedFlow(requestedId))
        nodeStore.insertRunResult(response.data);

      if (response.status === 200) {
        finish();
        if (!isSelectedFlow(requestedId) || targetId?.silent) return;
        const cancelled = state.isCancelled(requestedId);
        // A pending request is intent, not proof: settle the owner without claiming
        // cancellation or failure before its ACK. Successful results remain Success.
        if (!cancelled && targetId?.pendingCancel && !response.data.success) return;
        if (pollingKeySuffix === "" && !cancelled) {
          useTutorialStore().notify({
            type: "flow-run-completed",
            success: !!response.data.success,
          });
        }

        // Surfacing Results/Logs trips the drawer's auto-focus watcher, so a run
        // started from inside a panel opts out and stays on that panel.
        if (focusResultPanels && !cancelled) {
          editorStore.setShowFlowResult(true);
          editorStore.updateLogViewerVisibility(true);
        }

        if (cancelled) {
          showNotification("Cancelled", "The operation was cancelled");
          return;
        }
        const notificationConfig = createNotificationConfig(response.data);
        if (customSuccessMessage && response.data.success) {
          notificationConfig.message = customSuccessMessage;
        }

        showNotification(
          notificationConfig.title,
          notificationConfig.message,
          notificationConfig.type,
        );
      } else if (response.status === 404) {
        finish();
        if (isSelectedFlow(requestedId)) resultsStore.resetRunResults();
      }
    } catch (error) {
      if (
        targetId?.stopped ||
        targetId?.paused ||
        generation !== targetId?.generation ||
        requestedId !== getTargetId()
      )
        return;
      console.error("Error checking run status:", error);
      if ((error as { response?: { status?: number } })?.response?.status === 404) {
        finish();
        if (isSelectedFlow(requestedId)) resultsStore.resetRunResults();
      } else if (targetId) {
        targetId.failures = (targetId.failures || 0) + 1;
        const limit = Number.isFinite(pollingConfig.maxAttempts)
          ? Math.max(1, pollingConfig.maxAttempts!)
          : 5;
        if (targetId.failures >= limit) {
          // The outcome is unknown, not completed. Retain the owner and its callbacks for Retry/Cancel.
          targetId.paused = true;
          targetId.generation = (targetId.generation || 0) + 1;
          if (options.persistPolling)
            state.pausePollingInterval(getPollingKey(pollingKeySuffix, requestedId));
          else localPollingEntry.value?.pause();
          if (isSelectedFlow(requestedId))
            showNotification(
              "Status unavailable",
              "Could not check the operation after repeated failures. Its outcome is unknown. Try Cancel again to resume checking.",
              "error",
            );
        }
      }
    }
  };

  const escapeHtml = (text: string): string => {
    const div = document.createElement("div");
    div.textContent = text;
    return div.innerHTML;
  };

  /**
   * Saves the open settings drawer so the run sees its edits; false when that save was refused.
   * A refusal arms the drawer's discard, so the next click away from it drops the edits.
   */
  const saveOpenDrawer = async (notStartedTitle: string): Promise<boolean> => {
    if (!editorStore.isDrawerOpen || nodeStore.nodeId === -1) return true;
    try {
      if (await editorStore.executeDrawCloseFunction()) return true;
    } catch (error) {
      console.error("Error saving the open node settings:", error);
    }
    editorStore.rememberRefusedSave(editorStore.drawCloseFunction);
    showNotification(
      notStartedTitle,
      "The settings in the open panel could not be saved. Fix them, or click away from the panel " +
        "to discard them, then run again.",
      "error",
    );
    return false;
  };

  const runFlow = () => startOnce(getPollingKey(), () => withTarget(startFlowRun));

  const startFlowRun = async (target: OperationTarget) => {
    if (editorStore.isRunning) {
      showNotification("Flow not started", "Flow is already running", "error");
      return;
    }
    if (!(await saveOpenDrawer("Flow not started")) || !isSelectedFlow(target.id)) return;
    const flowSettings: FlowSettings | null = await FlowApi.getFlowSettings(target.id);
    if (!isSelectedFlow(target.id)) return;
    if (!flowSettings) {
      throw new Error("Failed to retrieve flow settings");
    }

    state.setCancelled(target.id, false);
    freezeFlow();
    nodeStore.resetNodeResult();
    isExecuting.value = true;
    editorStore.hideLogViewerForThisRun = false;
    state.setStarting(target.id, true, target);
    target.startPolling = () => startStatusPolling(target);
    target.check = () => checkRunStatus(undefined, "", true, undefined, target);

    const executionLocationText = flowSettings.execution_location === "local" ? "Local" : "Remote";
    // display_name is the registration's name; settings.name is the file stem.
    const escapedFlowName = escapeHtml(flowSettings.display_name?.trim() || flowSettings.name);

    const notificationMessage = `
      <div style="line-height: 1.4;">
        <div><strong>Flow:</strong> "${escapedFlowName}"</div>
        <div><strong>Mode:</strong> ${flowSettings.execution_mode}</div>
        <div><strong>Location:</strong> ${executionLocationText}</div>
      </div>
    `;

    showNotification("Flow Started", notificationMessage, undefined, true, Promotion);

    try {
      await axios.post("/flow/run/", null, {
        params: { flow_id: target.id },
        headers: { accept: "application/json" },
      });
      if (target.stopped) return;
      state.setStarting(target.id, false);
      // Core claimed the run before answering: the log viewer opens its stream on this flag.
      if (isSelectedFlow(target.id) && !state.isTerminal(target.id)) {
        freezeFlow();
        editorStore.isRunning = true;
        useTutorialStore().notify({ type: "flow-run-started" });
        nodeStore.showLogViewer();
      }
      startStatusPolling(target);
    } catch (error: any) {
      if (target.stopped) return;
      state.setStarting(target.id, false);
      // Settings (or Cancel) already confirmed this operation exists on the server.
      if (target.confirmedRunning || target.polling) {
        if (!target.polling) target.startPolling?.();
        if (isSelectedFlow(target.id)) freezeFlow();
        return;
      }
      console.error("Error starting run:", error);
      if (!isSelectedFlow(target.id)) return;
      unFreezeFlow();
      isExecuting.value = false;

      // 404 means the in-memory flow_id doesn't match the backend — typically
      // a stale id after Save As or a backend restart. Tell the user
      // specifically so they reopen instead of retrying blindly.
      if (error?.response?.status === 404) {
        const detail =
          error?.response?.data?.detail ??
          "The flow id this tab is using no longer exists on the server.";
        showNotification("Flow id is stale", `${detail} Reopen the flow and try again.`, "error");
      } else {
        const detail = error?.response?.data?.detail ?? error?.message;
        showNotification(
          "Error",
          detail ? `Failed to start the flow: ${detail}` : "Failed to start the flow",
          "error",
        );
      }
    }
  };

  type NodeFetchOptions = {
    focusResultPanels?: boolean;
    performanceMode?: boolean;
    /** Fired once when the run reaches a terminal state, however it got there. */
    onComplete?: () => void;
  };

  const triggerNodeFetch = (nodeId: number, opts: NodeFetchOptions = {}) =>
    startOnce(getPollingKey(`node_${nodeId}`), () =>
      withTarget((target) => startNodeFetch(nodeId, opts, target), `node_${nodeId}`),
    );

  const startNodeFetch = async (
    nodeId: number,
    opts: NodeFetchOptions,
    target: OperationTarget,
  ) => {
    const pollingKeySuffix = `node_${nodeId}`;
    // Fetches driven from a panel (Data tab, Explore Data) opt out so that
    // panel keeps focus; canvas "Run node" keeps the default (show logs).
    const focusResultPanels = opts.focusResultPanels ?? true;
    // Explore Data never reads the example rows, so it skips storing the node's
    // result — on a large source that store is the whole cost of the fetch.
    const performanceMode = opts.performanceMode ?? false;
    // Shared with the polling entry so rekey collisions transfer ownership without completing early.
    const completionCallbacks = opts.onComplete ? [opts.onComplete] : [];

    if (isPollingActive(pollingKeySuffix)) {
      console.log(`Node ${nodeId} fetch already in progress`);
      return;
    }
    if (!(await saveOpenDrawer("Node not run")) || !isSelectedFlow(target.id)) return;

    state.setCancelled(target.id, false);
    freezeFlow();
    nodeStore.resetNodeResult();
    isExecuting.value = true;
    state.setStarting(target.id, true, target);
    target.check = () =>
      checkRunStatus(
        "Node data has been fetched successfully",
        pollingKeySuffix,
        focusResultPanels,
        () => completionCallbacks.splice(0).forEach((complete) => complete()),
        target,
      );
    target.startPolling = () => {
      if (target.polling) {
        state.resumeStatusPolling(target.id);
        localPollingEntry.value?.resume(pollingConfig.interval || 2000);
        return;
      }
      target.polling = startPolling(
        target.check!,
        pollingKeySuffix,
        target.id,
        target.stop,
        completionCallbacks,
        target,
      );
    };

    showNotification(
      "Fetching Node Data",
      `Starting data fetch for node ${nodeId}...`,
      undefined,
      false,
      DataLine,
    );

    try {
      await axios.post("/node/trigger_fetch_data", null, {
        params: {
          flow_id: target.id,
          node_id: nodeId,
          performance_mode: performanceMode,
        },
        headers: { accept: "application/json" },
      });
      if (target.stopped) return;
      state.setStarting(target.id, false);
      if (isSelectedFlow(target.id) && !state.isTerminal(target.id)) {
        freezeFlow();
        editorStore.isRunning = true;
        if (focusResultPanels) nodeStore.showLogViewer();
      }
      if (!target.polling) target.startPolling();
    } catch (error: any) {
      if (target.stopped) return;
      state.setStarting(target.id, false);
      // Settings (or Cancel) already confirmed this operation exists on the server.
      if (target.confirmedRunning || target.polling) {
        if (!target.polling) target.startPolling?.();
        if (isSelectedFlow(target.id)) freezeFlow();
        return;
      }
      console.error("Error triggering node fetch:", error);
      if (!isSelectedFlow(target.id)) return;
      unFreezeFlow();
      isExecuting.value = false;

      const errorMessage = error.response?.data?.detail || "Failed to fetch node data";
      showNotification("Error", errorMessage, "error");
      throw error;
    }
  };

  const cancelFlow = () => withTarget(cancelRun);

  const cancelRun = async (target: OperationTarget) => {
    if (isCancelling.value) return;
    const issuedOwner = state.getOperationOwner(target.id);
    target.cancelOwner = issuedOwner;
    if (issuedOwner) issuedOwner.pendingCancel = target;
    cancelTarget.value = target;
    const pending = cancelTarget.value;
    const ownsOperation = () => {
      const owner = issuedOwner && currentOwner(issuedOwner);
      return owner
        ? !owner.stopped && state.getOperationOwner(target.id) === owner
        : !state.getOperationOwner(target.id);
    };
    try {
      await axios.post("/flow/cancel/", null, {
        params: { flow_id: target.id },
        headers: { accept: "application/json" },
      });
      if (!ownsOperation()) return;
      const owner = issuedOwner && currentOwner(issuedOwner);
      state.setCancelled(target.id, true);
      target.silent = true;
      if (isSelectedFlow(target.id)) {
        editorStore.isCanceled = true;
        showNotification("Cancelling", "The operation is being cancelled");
      }
      if (cancelTarget.value === pending) cancelTarget.value = null;
      if (owner) {
        owner.startPolling?.();
        state.resumeStatusPolling(target.id);
        await owner.check?.();
        return;
      }
      startStatusPolling(target, false);
      if (!target.stopped) await checkRunStatus(undefined, "", false, undefined, target);
      else if (options.persistPolling) state.checkStatusPolling(target.id);
      else void localPollingEntry.value?.check();
    } catch (error) {
      if (!ownsOperation()) return;
      const owner = issuedOwner && currentOwner(issuedOwner);
      if (owner?.pendingCancel === target) owner.pendingCancel = undefined;
      if (owner?.paused) {
        owner.startPolling?.();
        state.resumeStatusPolling(target.id);
        await owner.check?.();
        if (owner.stopped) return;
      }
      if (isSelectedFlow(target.id)) editorStore.isCanceled = state.isCancelled(target.id);
      console.error("Error cancelling run:", error);
      if (isSelectedFlow(target.id))
        showNotification("Error", "Failed to cancel the operation", "error");
    } finally {
      if (cancelTarget.value === pending) cancelTarget.value = null;
    }
  };

  onUnmounted(() => {
    if (!options.persistPolling) stopPolling();
  });

  return {
    // State
    isExecuting,

    // Methods
    runFlow,
    triggerNodeFetch,
    cancelFlow,
    isCancelling,
    showNotification,
    startPolling,
    startStatusPolling,
    stopPolling,
    checkRunStatus,
    isPollingActive,
    checkStatusPolling: () => state.checkStatusPolling(getFlowId()),
    getOperationOwner: () => state.getOperationOwner(getFlowId()),

    // Expose flow control if needed
    freezeFlow,
    unFreezeFlow,
  };
}
