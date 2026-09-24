// Editor Store - Manages drawer, editor UI state, log viewer, and code generator
import { defineStore } from "pinia";
import { ElMessage } from "element-plus";
import { ref, shallowRef } from "vue";
import type { Component } from "vue";
import type { NodeTitleInfo } from "../types";
import type { DrawerCloseOptions } from "../composables/settingsDrawerSession";

// One leave attempt at a time: a double-click must not save (or refuse) the same drawer three times.
let leaveInFlight: Promise<boolean> | null = null;

export const useEditorStore = defineStore("editor", {
  state: () => ({
    // Drawer state
    isDrawerOpen: false,
    isAnalysisOpen: false,
    activeDrawerComponent: shallowRef<Component | null>(null),
    drawerProps: ref<Record<string, any>>({}),
    drawCloseFunction: null as any,
    drawerHasPendingEdits: null as (() => boolean) | null,
    // The close function whose save was refused on the last leave attempt; the next attempt discards.
    refusedCloseFunction: null as any,
    // Whether that refusal came during a run; once the run state changes, a refusal warns again first.
    refusedWhileRunning: false,

    // Editor state
    initialEditorData: "" as string,
    inputCode: "",

    // Log viewer state
    hideLogViewerForThisRun: false,
    isShowingLogViewer: false,
    isStreamingLogs: false,
    displayLogViewer: true,

    // Code generator state
    showCodeGenerator: false,

    // Edge label state
    showEdgeLabels: false,

    // Run state
    isRunning: false,
    showFlowResult: false,
    tableVisible: false,

    // AI assistant drawer. Independent panel pattern — coexists
    // with node settings; intentionally NOT routed through activeDrawerComponent.
    isAiOpen: false,

    // Incremented whenever a graph-mutating action completes. Consumers (e.g.
    // the flow tab strip) watch this to refresh dirty state.
    graphVersion: 0,

    // Incremented to request opening the Flow Settings modal from anywhere
    // (e.g. the Performance-mode notice). HeaderButtons watches this counter.
    flowSettingsOpenRequest: 0,

    // Request signals to open a node's panels from outside Canvas (the per-node
    // right-click menu in NodeWrapper). Canvas watches the `token`.
    nodeSettingsOpenRequest: { nodeId: -1 as number, token: 0 },
    nodeDataOpenRequest: { nodeId: -1 as number, token: 0 },

    // Request signal to open a flow by path from outside the header (the
    // run_flow node's "Go to Flow" menu). DesignerView watches the `token` and
    // routes it through reloadCanvas so the flow tab strip + header refresh.
    openFlowRequest: { flowPath: "", name: undefined as string | undefined, token: 0 },
  }),

  getters: {
    drawerOpen(): boolean {
      return !!this.activeDrawerComponent;
    },
  },

  actions: {
    /** Awaits the open drawer's save; resolves false when that save was refused. */
    async executeDrawCloseFunction(): Promise<boolean> {
      if (!this.drawCloseFunction) return true;
      const saved = (await this.drawCloseFunction()) !== false;
      if (saved) this.disarmRefusedSave();
      return saved;
    },

    /**
     * Saves the open settings drawer before something closes it or switches node; resolves false
     * when the save was refused. A second attempt on the same refused drawer discards its edits, so
     * an unfixable setting never traps the user.
     */
    saveDrawerBeforeLeave(): Promise<boolean> {
      if (!this.isDrawerOpen || !this.drawCloseFunction) return Promise.resolve(true);
      if (!leaveInFlight) {
        leaveInFlight = this.leaveOpenDrawer().finally(() => {
          leaveInFlight = null;
        });
      }
      return leaveInFlight;
    },

    async leaveOpenDrawer(): Promise<boolean> {
      const closeFunction = this.drawCloseFunction;
      let saved = false;
      try {
        saved = await this.executeDrawCloseFunction();
      } catch (error) {
        console.error("Error saving the open node settings:", error);
      }
      if (!saved) {
        const armed =
          this.refusedCloseFunction === closeFunction &&
          this.refusedWhileRunning === this.isRunning;
        if (!armed) {
          this.rememberRefusedSave(closeFunction);
          ElMessage.warning({
            message: this.isRunning
              ? "The flow is running, so these settings can't be saved yet. Your changes are kept " +
                "in the open panel: click Apply when the run finishes, or click away again to " +
                "discard them."
              : "The node settings could not be saved, so the panel stays open. " +
                "Fix them, or click away again to discard the changes.",
            showClose: true,
          });
          return false;
        }
        ElMessage.info({ message: "Unsaved node settings were discarded.", showClose: true });
      }
      this.clearCloseFunction();
      return true;
    },

    /** Arms the discard: the next attempt to leave this drawer, in the same run state, drops its edits. */
    rememberRefusedSave(closeFunction: any): void {
      this.refusedCloseFunction = closeFunction;
      this.refusedWhileRunning = this.isRunning;
    },

    /** A successful save leaves nothing to discard, so the next refusal warns first again. */
    disarmRefusedSave(): void {
      this.refusedCloseFunction = null;
      this.refusedWhileRunning = false;
    },

    /**
     * Run the pending close save once and forget it; false means refused or failed. With
     * `keepOnRefusal` a refused close stays armed (unless the drawer closed or re-registered meanwhile).
     */
    async executeDrawCloseFunctionOnce(
      options?: DrawerCloseOptions,
      keepOnRefusal = false,
    ): Promise<unknown> {
      const close = this.drawCloseFunction;
      const hasPendingEdits = this.drawerHasPendingEdits;
      this.clearCloseFunction();
      if (!close) return undefined;
      let result: unknown;
      try {
        result = await close(options);
      } catch (error) {
        console.error("Saving the open settings failed:", error);
        result = false;
      }
      const stillOpen = this.activeDrawerComponent !== null && this.drawCloseFunction === null;
      if (result === false && keepOnRefusal && stillOpen) {
        this.setCloseFunction(close, hasPendingEdits ?? undefined);
      }
      return result;
    },

    setCloseFunction(
      f: (options?: DrawerCloseOptions) => unknown,
      hasPendingEdits?: () => boolean,
    ): void {
      this.drawCloseFunction = f;
      this.drawerHasPendingEdits = hasPendingEdits ?? null;
      this.refusedCloseFunction = null;
      this.refusedWhileRunning = false;
    },

    clearCloseFunction(): void {
      this.drawCloseFunction = null;
      this.drawerHasPendingEdits = null;
      this.refusedCloseFunction = null;
      this.refusedWhileRunning = false;
    },

    /** Whether the open settings drawer holds user edits its close would save. */
    hasPendingDrawerEdits(): boolean {
      return !!this.drawCloseFunction && (this.drawerHasPendingEdits?.() ?? false);
    },

    openDrawer(
      component: Component,
      nodeTitleInfo: NodeTitleInfo,
      props: Record<string, any> = {},
    ) {
      this.activeDrawerComponent = component;
      this.drawerProps = { ...nodeTitleInfo, ...props };
      this.isDrawerOpen = true;
    },

    closeDrawer() {
      this.activeDrawerComponent = null;
      if (this.drawCloseFunction) {
        // Optionally push node data
      }
    },

    toggleDrawer() {
      if (this.isDrawerOpen && this.drawCloseFunction) {
        this.pushNodeData();
      }
      this.isDrawerOpen = !this.isDrawerOpen;
    },

    pushNodeData() {
      if (this.drawCloseFunction && !this.isRunning) {
        this.drawCloseFunction();
        this.clearCloseFunction();
      }
    },

    openAnalysisDrawer(closeFunction?: () => void) {
      console.log("openAnalysisDrawer in editor-store.ts");
      if (this.isAnalysisOpen) {
        this.pushNodeData();
      }
      if (closeFunction) {
        this.drawCloseFunction = closeFunction;
      }
      this.isAnalysisOpen = true;
    },

    closeAnalysisDrawer() {
      this.isAnalysisOpen = false;
      if (this.drawCloseFunction) {
        console.log("closeDrawer in editor-store.ts");
        this.pushNodeData();
      }
    },

    // ========== Code Generator ==========
    toggleCodeGenerator() {
      this.showCodeGenerator = !this.showCodeGenerator;
    },

    setCodeGeneratorVisibility(visible: boolean) {
      this.showCodeGenerator = visible;
    },

    // ========== Log Viewer ==========
    showLogViewer() {
      console.log("triggered show log viewer");
      this.isShowingLogViewer = this.displayLogViewer;
    },

    hideLogViewer() {
      this.isShowingLogViewer = false;
    },

    toggleLogViewer() {
      console.log("triggered toggle log viewer");
      this.isShowingLogViewer = !this.isShowingLogViewer;
    },

    updateLogViewerVisibility(showResult: boolean) {
      this.isShowingLogViewer =
        this.displayLogViewer && showResult && !this.hideLogViewerForThisRun;
    },

    // ========== Editor Data ==========
    setInitialEditorData(editorDataString: string) {
      this.initialEditorData = editorDataString;
    },

    getInitialEditorData() {
      return this.initialEditorData;
    },

    setInputCode(newCode: string) {
      this.inputCode = newCode;
    },

    // ========== Flow Result Display ==========
    setShowFlowResult(show: boolean) {
      this.showFlowResult = show;
    },

    setTableVisible(visible: boolean) {
      this.tableVisible = visible;
    },

    setIsRunning(running: boolean) {
      this.isRunning = running;
    },

    // ========== Graph version (dirty tracking) ==========
    bumpGraphVersion() {
      this.graphVersion += 1;
    },

    // Signal HeaderButtons to open the Flow Settings modal.
    requestOpenFlowSettings() {
      this.flowSettingsOpenRequest += 1;
    },

    // Ask Canvas to open + front a node's Settings / Data panels. Reassign the
    // whole object so the token watch fires reliably.
    requestNodeSettings(nodeId: number) {
      this.nodeSettingsOpenRequest = { nodeId, token: this.nodeSettingsOpenRequest.token + 1 };
    },

    requestNodeData(nodeId: number) {
      this.nodeDataOpenRequest = { nodeId, token: this.nodeDataOpenRequest.token + 1 };
    },

    // Ask DesignerView to open a flow by path (mirrors the catalog "open" path,
    // refreshing the flow tab strip and header, not just the canvas).
    requestOpenFlow(flowPath: string, name?: string) {
      this.openFlowRequest = { flowPath, name, token: this.openFlowRequest.token + 1 };
    },

    // ========== AI Assistant Drawer ==========
    openAiDrawer() {
      this.isAiOpen = true;
    },

    closeAiDrawer() {
      this.isAiOpen = false;
    },

    toggleAiDrawer() {
      this.isAiOpen = !this.isAiOpen;
    },

    // ========== Bulk panel control ==========
    // Closes every floating overlay (right-side and bottom). The left palette
    // (`dataActions`) is owned by the canvas component and stays visible.
    hideAllPanels() {
      this.showFlowResult = false;
      this.showCodeGenerator = false;
      this.activeDrawerComponent = null;
      this.isDrawerOpen = false;
      this.isShowingLogViewer = false;
      this.tableVisible = false;
      this.isAiOpen = false;
      // Lazy import closes the flow→editor→drawer→flow module cycle (no
      // eval-time edge); drawer-store is already loaded once any panel is open.
      import("./drawer-store")
        .then(({ useDrawerStore }) => {
          try {
            useDrawerStore().clearPreview();
          } catch {
            /* store unavailable in test contexts */
          }
        })
        .catch(() => {
          /* dynamic-import resolution failed; non-fatal */
        });
    },
  },
});
