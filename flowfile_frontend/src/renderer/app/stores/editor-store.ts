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

    // Run state
    isRunning: false,
    showFlowResult: false,
    tableVisible: false,
  }),

  getters: {
    drawerOpen(): boolean {
      return !!this.activeDrawerComponent;
    },
  },

  actions: {
    // ========== Drawer Management ==========
    async executeDrawCloseFunction() {
      console.log("Executing draw close function");
      if (this.drawCloseFunction) {
        this.drawCloseFunction();
      }
    },

    setCloseFunction(f: () => void): void {
      this.drawCloseFunction = f;
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
  },
});
