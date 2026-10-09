/**
 * One flow's log stream for the viewer (`GET /logs/{flow_id}`). Core owns a stream's end: opened
 * while the flow runs it ends when the run does, opened while the flow is idle it ends once the file
 * is sent. The viewer only opens streams — on mount, when the logs are shown, when a run starts here
 * (asking core to wait for the run it just posted, since that claim lands after the response) and on
 * a run signal from elsewhere (core rewrites the file per run, so it is read again from the start) —
 * and closes them only on unmount or when a newer one replaces them.
 */
import { onMounted, onUnmounted, ref, watch } from "vue";
import { useNodeStore } from "../../../stores/column-store";
import { useEditorStore } from "../../../stores/editor-store";
import { useFlowStore } from "../../../stores/flow-store";
import authService from "../../../services/auth.service";
import { streamFlowLogs } from "../../../services/logStreamClient";

export type LogConnectionStatus = "connected" | "disconnected" | "error";

const MAX_RETRIES = 5;
const TOKEN_CHECK_MS = 5 * 60 * 1000;

export function useLogStream() {
  const nodeStore = useNodeStore();
  const editorStore = useEditorStore();
  const flowStore = useFlowStore();
  const logs = ref("");
  const connectionStatus = ref<LogConnectionStatus>("disconnected");
  const connectionRetries = ref(0);
  const errorMessage = ref<string | null>(null);
  let streamController: AbortController | null = null;
  let retryTimer: ReturnType<typeof setTimeout> | null = null;
  let tokenCheck: ReturnType<typeof setInterval> | null = null;

  const cancelRetry = () => {
    if (retryTimer !== null) clearTimeout(retryTimer);
    retryTimer = null;
  };

  const markDisconnected = () => {
    if (connectionStatus.value === "connected") connectionStatus.value = "disconnected";
  };

  const stopStreamingLogs = () => {
    cancelRetry();
    streamController?.abort();
    streamController = null;
    markDisconnected();
  };

  const open = async (waitForRun: boolean, attempt: number): Promise<void> => {
    cancelRetry();
    streamController?.abort();
    const controller = new AbortController();
    streamController = controller;

    logs.value = "";
    connectionRetries.value = attempt;
    errorMessage.value = null;
    connectionStatus.value = "disconnected";

    const token = await authService.getToken();
    if (streamController !== controller) return;
    if (!token) {
      console.error("No auth token available for log streaming");
      errorMessage.value = "Authentication failed. Please log in again.";
      connectionStatus.value = "error";
      streamController = null;
      return;
    }

    let receivedLine = false;
    let streamError: unknown = null;
    try {
      await streamFlowLogs({
        flowId: nodeStore.flow_id,
        token,
        signal: controller.signal,
        waitForRun,
        onOpen: () => {
          connectionStatus.value = "connected";
        },
        onData: (data) => {
          receivedLine = true;
          try {
            logs.value += JSON.parse(data) + "\n";
          } catch (error) {
            console.error("Error parsing log data:", error);
          }
        },
      });
    } catch (error) {
      streamError = error;
    }
    // Stopped or replaced by a newer stream: its owner handles state.
    if (streamController !== controller) return;
    if (streamError) console.error("Log stream error:", streamError);
    streamController = null;
    markDisconnected();
    // Core sent what there was: the file, or the run to its end.
    if (receivedLine || !nodeStore.isRunning) return;

    // A run's stream that gave nothing (its log file was not there yet): try again after a pause.
    if (attempt >= MAX_RETRIES) {
      console.error("Max retries reached for log connection");
      errorMessage.value = "Failed to connect after multiple attempts. Try refreshing the page.";
      connectionStatus.value = "error";
      return;
    }
    const next = attempt + 1;
    errorMessage.value = `Connection failed. Retrying (${next}/${MAX_RETRIES})...`;
    connectionStatus.value = "error";
    retryTimer = setTimeout(() => {
      retryTimer = null;
      void open(waitForRun, next);
    }, 1000 * next);
  };

  /** Replace the stream; a re-read starts from the file's first line. */
  const startStreamingLogs = ({ waitForRun = false }: { waitForRun?: boolean } = {}) =>
    open(waitForRun, 0);

  const clearLogs = () => {
    logs.value = "";
  };

  // A run started here: core may not have claimed it yet (the POST queues it), so the stream waits.
  watch(
    () => nodeStore.isRunning,
    (isRunning) => {
      if (isRunning) void startStreamingLogs({ waitForRun: true });
    },
  );

  // Logs shown without a run: read the file once (a running flow's stream is already open).
  watch(
    () => editorStore.isShowingLogViewer,
    (show) => {
      if (show && !nodeStore.isRunning && !streamController) void startStreamingLogs();
    },
  );

  // A run started or ended elsewhere rewrote the log file: read it again from the start.
  watch(
    () => flowStore.pendingRunStateCounter,
    () => {
      if (editorStore.isShowingLogViewer || nodeStore.isRunning) void startStreamingLogs();
    },
  );

  onMounted(() => {
    // A dock opened for a data preview opens no stream; a run or the shown logs do.
    if (nodeStore.isRunning || editorStore.isShowingLogViewer) {
      void startStreamingLogs({ waitForRun: nodeStore.isRunning });
    }
    tokenCheck = setInterval(async () => {
      if (streamController && !authService.hasValidToken()) {
        stopStreamingLogs();
        await authService.getToken();
        void startStreamingLogs();
      }
    }, TOKEN_CHECK_MS);
  });

  onUnmounted(() => {
    stopStreamingLogs();
    if (tokenCheck !== null) clearInterval(tokenCheck);
    tokenCheck = null;
  });

  return {
    logs,
    connectionStatus,
    connectionRetries,
    errorMessage,
    startStreamingLogs,
    stopStreamingLogs,
    clearLogs,
  };
}
