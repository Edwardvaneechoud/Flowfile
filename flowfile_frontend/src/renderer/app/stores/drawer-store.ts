// Drawer store - the small extra state the unified tabbed-drawer system needs: which tab is
// active per drawer, which node the bottom dock previews, and what the dock and a flow's Data
// window hand each other.
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

export const useDrawerStore = defineStore("drawer", {
  state: () => ({
    activeTab: {} as Record<string, string>, // drawerId -> tabId
    previewNodeId: null as number | null, // node whose data the bottom dock shows (null = placeholder)
    previewRefreshToken: 0, // bump to force a re-fetch of the SAME node
    // The node a flow's Data window shows: setPreviewNode writes here, not the dock, while it is out.
    popoutPreview: null as { flowId: number; nodeId: number; token: number } | null,
    // Canvas consumes it for the loaded flow only: the flow-switch watcher hides every panel first.
    dockRequest: null as DockRequest | null,
  }),
  actions: {
    setActiveTab(drawerId: string, tabId: string) {
      this.activeTab[drawerId] = tabId;
    },
    // Re-selecting the already-previewed node bumps the token (re-fetch) instead
    // of being a no-op — replaces Canvas's old `needsLoad`/`dataLength` check.
    // While the flow's Data is in its own window the node goes there and the dock stays as it is.
    setPreviewNode(nodeId: number | null) {
      const flowId = useFlowStore().flowId;
      if (nodeId !== null && useEditorStore().isPoppedOut("table", flowId)) {
        this.popoutPreview = { flowId, nodeId, token: (this.popoutPreview?.token ?? 0) + 1 };
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
    /** The Data tab moved to its window: the node it showed goes along. */
    divertPreviewToWindow(flowId: number) {
      if (this.previewNodeId !== null) {
        const token = (this.popoutPreview?.token ?? 0) + 1;
        this.popoutPreview = { flowId, nodeId: this.previewNodeId, token };
      }
      this.previewNodeId = null;
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
