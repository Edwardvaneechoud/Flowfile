// Single source of truth for the designer's tabbed drawers. Adding/moving a view
// is a one-entry edit here — TabbedDrawer renders each from this declarative data.
import { markRaw } from "vue";
import NodeSettingsDrawer from "./NodeSettingsDrawer.vue";
import CodeGenerator from "./CodeGenerator/CodeGenerator.vue";
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
    // Width stays fixed; height keeps its gap to the canvas edge instead of
    // snapping to full height on resize.
    heightBehaviour: "scale",
    allowFullScreen: true,
    // Opens for settings / results / code; Code is then always a tab (its own
    // visibleWhen is always-true), so it can't be the thing that opens it.
    visibleWhen: ({ editor }) =>
      editor.isDrawerOpen || editor.showFlowResult || editor.showCodeGenerator,
    onMinimize: ({ editor, node }) => {
      node.nodeId = -1; // closes the Settings tab via NodeSettingsDrawer's watch
      editor.isDrawerOpen = false;
      editor.activeDrawerComponent = null;
      editor.showFlowResult = false;
      editor.setCodeGeneratorVisibility(false);
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
      {
        id: "code",
        label: "Code",
        component: markRaw(CodeGenerator),
        // visibleWhen is always-true, so this tab never "appears" and the
        // auto-focus watcher can't focus it — focusWhen is what grabs Ctrl+G
        // focus (don't remove it). `active` defers CodeMirror creation until
        // visible (it breaks if built while hidden).
        visibleWhen: () => true,
        focusWhen: ({ editor }) => editor.showCodeGenerator,
        props: ({ drawer }) => ({ active: drawer.activeTab["rightDrawer"] === "code" }),
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
    initialLeft: 180,
    // Bottom dock keeps its fraction of the canvas width as the canvas resizes;
    // height stays fixed px (it gets a numeric initialHeight via heightOverride).
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
          // Gate the fetch to when the Data tab is actually shown (mirrors the
          // Code tab's `active`). undefined activeTab => default first tab "data".
          active: (drawer.activeTab["bottomDock"] ?? "data") === "data",
        }),
      },
      {
        id: "logs",
        label: "Logs",
        component: markRaw(LogViewer),
        visibleWhen: ({ editor }) => editor.isShowingLogViewer,
      },
    ],
  },
];
