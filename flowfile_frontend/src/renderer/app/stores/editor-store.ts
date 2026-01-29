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

  getters: {},

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

  },
});
