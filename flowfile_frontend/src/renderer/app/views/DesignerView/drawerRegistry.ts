// Single source of truth for the designer's tabbed drawers. Adding/moving a view
// is a one-entry edit here — TabbedDrawer renders each from this declarative data.
import { markRaw } from "vue";
import NodeSettingsDrawer from "./NodeSettingsDrawer.vue";
import LogViewer from "./LogViewer/LogViewer.vue";
import FlowResults from "../../features/designer/editor/results.vue";
import AiAssistant from "../../features/ai/AiAssistant.vue";
import DataPreview from "../../features/designer/dataPreview.vue";
import type { DrawerDef } from "../../types/drawer.types";

export const drawers: DrawerDef[] = [
  {
    id: "rightDrawer",
    side: "right",
    initialWidth: 600,
    // Fixed width; height tracks the canvas (keeps its gap) instead of filling.
    // Flush to the canvas top; Canvas derives its default height so the bottom
    // edge meets the bottom dock.
    heightBehaviour: "scale",
    allowFullScreen: true,
    // Settings and results only; the code generator is DesignerView's split pane.
    visibleWhen: ({ editor }) => editor.isDrawerOpen || editor.showFlowResult,
    onMinimize: async ({ editor, node }) => {
      // Save first: closing unmounts the Settings tab before its nodeId watcher can.
      if (editor.isDrawerOpen && node.nodeId !== -1 && !(await editor.saveDrawerBeforeLeave())) {
        return false;
      }
      editor.isDrawerOpen = false;
      editor.activeDrawerComponent = null;
      editor.showFlowResult = false;
    },
    tabs: [
      {
        id: "settings",
        label: "Settings",
        component: markRaw(NodeSettingsDrawer),
        // Singleton (no remountKey): NodeSettingsDrawer's own watcher handles the
        // node switch + Apply/pushNodeData lifecycle while it stays mounted.
        visibleWhen: ({ editor }) => editor.isDrawerOpen,
      },
      {
        id: "results",
        label: "Results",
        component: markRaw(FlowResults),
        visibleWhen: ({ editor }) => editor.showFlowResult,
      },
    ],
  },
  {
    id: "aiDrawer",
    side: "right",
    initialWidth: 600,
    heightBehaviour: "scale",
    allowFullScreen: true,
    onMinimize: ({ editor }) => editor.closeAiDrawer(),
    tabs: [
      {
        id: "ai",
        label: "AI Assistant",
        component: markRaw(AiAssistant),
        visibleWhen: ({ editor }) => editor.isAiOpen,
      },
    ],
  },
  {
    id: "bottomDock",
    side: "bottom",
    // Width tracks the canvas (keeps its gap); height stays fixed px. The
    // left default comes from Canvas (the live width of the docked palette),
    // so the dock starts right next to Data actions.
    widthBehaviour: "scale",
    allowFullScreen: true,
    // Opens for a data preview or logs; Data is a permanent home tab (placeholder).
    visibleWhen: ({ drawer, editor }) => drawer.previewNodeId !== null || editor.isShowingLogViewer,
    onMinimize: ({ drawer, editor }) => {
      drawer.clearPreview();
      editor.hideLogViewerForThisRun = true;
      editor.hideLogViewer();
    },
    tabs: [
      {
        id: "data",
        label: "Data",
        component: markRaw(DataPreview),
        visibleWhen: () => true,
        props: ({ drawer }) => ({
          nodeId: drawer.previewNodeId,
          refreshToken: drawer.previewRefreshToken,
          // Fetch only while the Data tab (the default) is shown.
          active: (drawer.activeTab["bottomDock"] ?? "data") === "data",
        }),
      },
      {
        id: "logs",
        label: "Logs",
        component: markRaw(LogViewer),
        // Always a tab; the run/results signal only pulls focus to it.
        visibleWhen: ({ editor }) => editor.displayLogViewer,
        focusWhen: ({ editor }) => editor.isShowingLogViewer,
      },
    ],
  },
];
