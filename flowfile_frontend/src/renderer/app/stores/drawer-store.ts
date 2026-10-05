// Drawer store - the small extra state the unified tabbed-drawer system needs:
// which tab is active per drawer, and which node the bottom dock previews.
import { defineStore } from "pinia";
import { useFlowStore } from "./flow-store";
import { useItemStore } from "../components/common/DraggableItem/stateStore";

export const useDrawerStore = defineStore("drawer", {
  state: () => ({
    activeTab: {} as Record<string, string>, // drawerId -> tabId
    previewNodeId: null as number | null, // node whose data the bottom dock shows (null = placeholder)
    previewRefreshToken: 0, // bump to force a re-fetch of the SAME node
  }),
  actions: {
    setActiveTab(drawerId: string, tabId: string) {
      this.activeTab[drawerId] = tabId;
    },
    // Re-selecting the already-previewed node bumps the token (re-fetch) instead
    // of being a no-op — replaces Canvas's old `needsLoad`/`dataLength` check.
    setPreviewNode(nodeId: number | null) {
      if (nodeId !== null && nodeId === this.previewNodeId) {
        this.previewRefreshToken++;
      } else {
        this.previewNodeId = nodeId;
      }
    },
    clearPreview() {
      this.previewNodeId = null;
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
