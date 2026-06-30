// Editor Store - Manages drawer, editor UI state, log viewer, and code generator
import { defineStore } from "pinia";
import { ref, shallowRef } from "vue";
import type { Component } from "vue";
import type { NodeTitleInfo } from "../types";

export const useEditorStore = defineStore("editor", {
  state: () => ({
    // Drawer state
    isDrawerOpen: false,
    isAnalysisOpen: false,
    activeDrawerComponent: shallowRef<Component | null>(null),
    drawerProps: ref<Record<string, any>>({}),
    drawCloseFunction: null as any,

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
  }),

  getters: {
    drawerOpen(): boolean {
      return !!this.activeDrawerComponent;
    },
  },

  actions: {
    async executeDrawCloseFunction() {
      if (this.drawCloseFunction) {
        this.drawCloseFunction();
      }
    },

    setCloseFunction(f: () => void): void {
      this.drawCloseFunction = f;
    },

    clearCloseFunction(): void {
      this.drawCloseFunction = null;
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
        this.drawCloseFunction = null;
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
