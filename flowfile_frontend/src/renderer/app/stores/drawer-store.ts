/**
 * Drawer store: the small extra state the unified tabbed-drawer system needs — which tab is active
 * per drawer, which node the bottom dock previews, and what the dock and a flow's Data window hand
 * each other.
 */
import { defineStore } from "pinia";
import { useEditorStore } from "./editor-store";
import { useFlowStore } from "./flow-store";
import { useItemStore } from "../components/common/DraggableItem/stateStore";

export type DockTab = "data" | "logs";

/** A window's Return: open the dock on this tab once the flow is loaded (`nodeId` for Data). */
export interface DockRequest {
  flowId: number;
  tab: DockTab;
  nodeId?: number;
  token: number;
}

/** The node a flow's Data window shows and how often the designer sent one (a repeat is a re-read). */
export interface WindowPreview {
  nodeId: number;
  token: number;
}

const nextPreview = (previous: WindowPreview | undefined, nodeId: number): WindowPreview => ({
  nodeId,
  token: (previous?.token ?? 0) + 1,
});

export const useDrawerStore = defineStore("drawer", {
  state: () => ({
    activeTab: {} as Record<string, string>, // drawerId -> tabId
    previewNodeId: null as number | null, // node whose data the bottom dock shows (null = placeholder)
    previewRefreshToken: 0, // bump to force a re-fetch of the SAME node
    // Per flow: setPreviewNode writes here, not the dock, while that flow's Data is in its window.
    popoutPreview: {} as Record<number, WindowPreview>,
    // Canvas consumes it for the loaded flow only: the flow-switch watcher hides every panel first.
    dockRequest: null as DockRequest | null,
  }),
  actions: {
    setActiveTab(drawerId: string, tabId: string) {
      this.activeTab[drawerId] = tabId;
    },
    /**
     * Re-selecting the already-previewed node bumps the token (re-fetch) instead of being a no-op —
     * replaces Canvas's old `needsLoad`/`dataLength` check. While the flow's Data is in its own
     * window the node goes there and the dock stays as it is.
     */
    setPreviewNode(nodeId: number | null) {
      const flowId = useFlowStore().flowId;
      if (nodeId !== null && useEditorStore().isPoppedOut("table", flowId)) {
        this.popoutPreview[flowId] = nextPreview(this.popoutPreview[flowId], nodeId);
        return;
      }
      if (nodeId !== null && nodeId === this.previewNodeId) {
        this.previewRefreshToken++;
      } else {
        this.previewNodeId = nodeId;
      }
    },
    // A canvas deselect leaves the window on its node.
    clearPreview() {
      this.previewNodeId = null;
    },
    /** The Data tab moved to its window: the node it showed goes along; with none, nothing does. */
    divertPreviewToWindow(flowId: number) {
      if (this.previewNodeId !== null) {
        this.popoutPreview[flowId] = nextPreview(this.popoutPreview[flowId], this.previewNodeId);
      } else {
        delete this.popoutPreview[flowId];
      }
      this.previewNodeId = null;
    },
    /** A Save As: the window's node follows the flow to its new id. */
    movePopoutPreview(from: number, to: number) {
      const preview = this.popoutPreview[from];
      if (!preview) return;
      delete this.popoutPreview[from];
      this.popoutPreview[to] = preview;
    },
    /** The flow's Data window is gone. */
    forgetPopoutPreview(flowId: number) {
      delete this.popoutPreview[flowId];
    },
    requestDock(request: Omit<DockRequest, "token">) {
      this.dockRequest = { ...request, token: (this.dockRequest?.token ?? 0) + 1 };
    },
    consumeDockRequest() {
      this.dockRequest = null;
    },
    // Replaces Canvas's selectNodeExternally (FlowResults row click).
    selectNodeForPreview(nodeId: number) {
      this.setPreviewNode(nodeId);
      this.setActiveTab("bottomDock", "data");
      const vf = useFlowStore().vueFlowInstance;
      // Centre on the node without zooming out from where the user is, nor past 1:1.
      vf?.fitView?.({
        nodes: [String(nodeId)],
        padding: 0.3,
        duration: 300,
        maxZoom: Math.max(1, vf.getViewport?.().zoom ?? 1),
      });
      useItemStore().bringToFront("bottomDock");
    },
  },
});
