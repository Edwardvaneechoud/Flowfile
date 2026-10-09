/**
 * The shared half of a pop-out window: one flow pinned from the URL, kept in step by core's change
 * feed. The kind's view adds what its panel needs (the notebook's unpushed-edits confirm, the data
 * preview's node) through the hooks.
 */
import { computed, ref, watch } from "vue";
import { useRoute, useRouter } from "vue-router";
import { ElMessage } from "element-plus";
import { FlowApi } from "../../api";
import { desktop } from "../../../lib/desktop";
import {
  flowLabel,
  parseFlowQuery,
  windowTitle,
  type PopoutKind,
} from "../../services/popoutWindow";
import { useEditorStore } from "../../stores/editor-store";
import { useFlowStore } from "../../stores/flow-store";
import { useFlowSyncStore } from "../../stores/flow-sync-store";

export type PopoutWindowState = "loading" | "ready" | "missing";

export interface PopoutWindowHostOptions {
  kind: PopoutKind;
  /** A reload request from the feed; there is no canvas here, so the view says what to refresh. */
  onReload?: () => void;
  /** The flow started or stopped running elsewhere (the re-read `is_running`). */
  onRunStateChanged?: (isRunning: boolean) => void;
  /** The flow moved to another id (a Save As); the URL follows after this. */
  onRekey?: (moved: { from: number; to: number }) => void;
  /** Resolve false to keep the window open on "Return to designer". */
  beforeReturn?: () => Promise<boolean> | boolean;
}

export function usePopoutWindowHost(options: PopoutWindowHostOptions) {
  const route = useRoute();
  const router = useRouter();
  const flowStore = useFlowStore();
  const editorStore = useEditorStore();

  // Pin the URL's flow before the feed starts: the flow store boots from the opener's copied
  // `last_flow_id`, and the sync store follows the store's flow from the moment it exists.
  const initialFlowId = parseFlowQuery(route.query.flow);
  if (initialFlowId > 0) flowStore.setFlowId(initialFlowId);
  const flowSync = useFlowSyncStore();

  const flowId = computed(() => parseFlowQuery(route.query.flow));
  const flowName = ref<string | null>(null);
  const state = ref<PopoutWindowState>("loading");
  const title = computed(() => windowTitle(options.kind, flowName.value));

  async function loadFlow(id: number): Promise<void> {
    state.value = "loading";
    const settings = await FlowApi.getFlowSettings(id);
    if (flowId.value !== id) return;
    if (!settings) {
      state.value = "missing";
      void desktop.setWindowTitle(windowTitle(options.kind, null));
      return;
    }
    flowName.value = flowLabel(settings);
    editorStore.isRunning = !!settings.is_running;
    void desktop.setWindowTitle(title.value);
    state.value = "ready";
  }

  // What the designer's header does on a run-state event: re-read whether the flow runs.
  async function refreshRunState(): Promise<void> {
    const settings = await FlowApi.getFlowSettings(flowId.value);
    if (!settings) return;
    editorStore.isRunning = !!settings.is_running;
    options.onRunStateChanged?.(editorStore.isRunning);
  }

  async function closeThisWindow(): Promise<void> {
    await desktop.closeCurrentWindow();
  }

  /** Hand the panel back to the designer, which reopens it on this flow, and close. */
  async function returnToDesigner(): Promise<void> {
    if (flowId.value <= 0) {
      await closeThisWindow();
      return;
    }
    if (options.beforeReturn && !(await options.beforeReturn())) return;
    await desktop.returnPopoutToDesigner(options.kind, flowId.value);
  }

  watch(
    flowId,
    (id) => {
      if (id > 0) {
        flowStore.setFlowId(id);
        void loadFlow(id);
      } else {
        state.value = "missing";
      }
    },
    { immediate: true },
  );

  watch(
    () => flowStore.pendingReloadCounter,
    () => options.onReload?.(),
  );

  watch(
    () => flowStore.pendingRunStateCounter,
    () => void refreshRunState(),
  );

  watch(
    () => flowSync.closeCount,
    () => {
      if (flowSync.closedFlowId !== flowId.value) return;
      ElMessage.info("The flow was closed in the designer.");
      void closeThisWindow();
    },
  );

  watch(
    () => flowSync.rekeyedTo,
    (moved) => {
      if (!moved || moved.from !== flowId.value) return;
      options.onRekey?.(moved);
      void router.replace({ query: { ...route.query, flow: String(moved.to) } });
    },
  );

  return { flowId, state, title, returnToDesigner, closeThisWindow };
}
