// Getting Started Tutorial - guides users through creating their first flow.
// Every instructed action advances on the real app event; reading steps block
// stray clicks; steps auto-skip when their outcome already exists.
import {
  firstNodeOfItem,
  hasEdgeBetweenItems,
  hasNodeOfItem,
  type Tutorial,
  type TutorialContext,
} from "../tutorial-engine";
import { useEditorStore, useFlowStore, useNodeStore } from "../../../stores/column-store";

// Apply pending settings before closing: unmounting the drawer in the same
// tick kills the watcher that normally pushes settings on node deselect.
const closeSettingsDrawer = () => {
  const editorStore = useEditorStore();
  const nodeStore = useNodeStore();
  editorStore.pushNodeData();
  nodeStore.nodeId = -1;
  editorStore.isDrawerOpen = false;
  editorStore.activeDrawerComponent = null;
};

// Bring an off-view target node into view; skip when everything is visible —
// re-fitting a visible canvas can shove nodes under the floating palette.
const fitCanvas = () => {
  const vueFlow = useFlowStore().vueFlowInstance;
  if (!vueFlow?.fitView) return;
  const nodes = vueFlow.getNodes?.value ?? [];
  if (!nodes.length) return;
  const viewport = vueFlow.viewport?.value;
  const dims = vueFlow.dimensions?.value;
  if (!viewport || !dims || !(dims.width > 0)) return;
  const allVisible = nodes.every((node: any) => {
    const pos = node.computedPosition ?? node.position;
    const width = (node.dimensions?.width ?? 100) * viewport.zoom;
    const height = (node.dimensions?.height ?? 60) * viewport.zoom;
    const left = pos.x * viewport.zoom + viewport.x;
    const top = pos.y * viewport.zoom + viewport.y;
    return left >= 0 && top >= 0 && left + width <= dims.width && top + height <= dims.height;
  });
  if (allVisible) return;
  vueFlow.fitView({ padding: 0.3, duration: 300 });
};

const SAMPLE_ROWS = [
  { country: "USA", product: "Widget", revenue: 1000 },
  { country: "Germany", product: "Gadget", revenue: 2500 },
  { country: "France", product: "Widget", revenue: 1800 },
  { country: "USA", product: "Gadget", revenue: 3200 },
  { country: "Germany", product: "Widget", revenue: 1500 },
  { country: "France", product: "Gadget", revenue: 2100 },
];
const SAMPLE_JSON = `[${SAMPLE_ROWS.map((row) => JSON.stringify(row)).join(",\n")}]`;
const SAMPLE_JSON_HTML = `[${SAMPLE_ROWS.map((row) => JSON.stringify(row)).join(",<br>")}]`;

const nodeTarget = (item: string) => (ctx: TutorialContext) => {
  const id = firstNodeOfItem(ctx, item);
  return id === undefined ? null : `.vue-flow__node[data-id="${id}"]`;
};

const settingsOpenFor = (ctx: TutorialContext, item: string) =>
  ctx.openSettingsNodeId !== null &&
  (ctx.nodesByItem[item] ?? []).includes(ctx.openSettingsNodeId);

const wrongNodeHint = (wanted: string) => (ev: { type: string; nodeItem?: string }) =>
  ev.type === "node-added" && ev.nodeItem !== wanted
    ? `That's a different node — you can keep it or select it and press Delete. This step needs ${wanted === "output" ? "Write data" : wanted === "group_by" ? "Group By" : "Manual Input"}.`
    : null;

const wrongEdgeHint = (description: string) => (ev: { type: string }) =>
  ev.type === "edge-connected" ? `Connected — but not the right pair. ${description}` : null;

const wrongSettingsHint = (wanted: string) => (ev: { type: string }) =>
  ev.type === "node-settings-opened"
    ? `Those are another node's settings — click the highlighted ${wanted} node instead.`
    : null;

export const gettingStartedTutorial: Tutorial = {
  id: "getting-started",
  name: "Getting Started with Flowfile",
  description: "Learn how to create your first data flow - from input to output",
  steps: [
    {
      id: "welcome",
      title: "Welcome to Flowfile!",
      content: `
        <p>In this tutorial, you'll build your first data flow.</p>
        <p>We'll cover:</p>
        <ul style="margin: 12px 0; padding-left: 20px;">
          <li>Creating a new flow</li>
          <li>Adding data with Manual Input</li>
          <li>Aggregating with Group By</li>
          <li>Writing results to a CSV file</li>
          <li>Running your flow and exporting it as Python code</li>
        </ul>
      `,
      position: "center",
      interaction: "modal",
      centerInScreen: true,
      showPrevButton: false,
    },

    {
      id: "create-flow",
      title: "Create a New Flow",
      content: `
        <p>Click the <strong>Create</strong> button — it instantly makes a new flow you can start editing.</p>
        <p style="font-size: 12px; color: var(--color-text-secondary);">The small chevron next to it opens a dialog if you want to pick a specific location instead.</p>
      `,
      target: "[data-tutorial='quick-create-main-btn']",
      position: "bottom",
      interaction: "spotlight",
      advanceWhen: { event: "flow-created" },
      isSatisfied: (ctx) => (ctx.flowId ?? 0) > 0,
      hint: "Click the highlighted Create button",
      showNextButton: false,
      highlightPadding: 4,
    },

    {
      id: "orientation",
      title: "Your Workspace",
      content: `
        <p>The big area is your <strong>flow canvas</strong>. You build pipelines by dragging nodes onto it and connecting them.</p>
        <p>On the left is the <strong>node palette</strong> — every building block, grouped by what it does:</p>
        <ul style="margin: 12px 0; padding-left: 20px;">
          <li><strong>Input Sources</strong> - bring data in</li>
          <li><strong>Transformations</strong> - filter, select, sort, calculate</li>
          <li><strong>Combine / Aggregations</strong> - join and summarize</li>
          <li><strong>Output Operations</strong> - write results out</li>
        </ul>
      `,
      target: "[data-tutorial='node-list']",
      position: "right",
      interaction: "modal",
      highlightPadding: 4,
    },

    {
      id: "add-manual-input",
      title: "Add Your Data",
      content: `
        <p>Find <strong>Manual Input</strong> under Input Sources and <strong>drag it onto the canvas</strong>.</p>
        <p>It lets you type sample data straight into the flow — ideal for testing.</p>
      `,
      target: "[data-tutorial-node='manual_input']",
      position: "right",
      interaction: "free",
      requires: ["flow"],
      advanceWhen: { event: "node-added", nodeItem: "manual_input" },
      isSatisfied: (ctx) => hasNodeOfItem(ctx, "manual_input"),
      wrongActionHint: wrongNodeHint("manual_input"),
      hint: "Drag Manual Input onto the canvas",
      highlightPadding: 4,
    },

    {
      id: "open-input-settings",
      title: "Open the Node Settings",
      content: `
        <p><strong>Click the Manual Input node</strong> you just added.</p>
        <p>Its settings panel opens on the right, where you can define columns and rows.</p>
      `,
      target: nodeTarget("manual_input"),
      position: "left",
      interaction: "free",
      requires: ["flow", "manual_input"],
      advanceWhen: { event: "node-settings-opened", nodeOfItem: "manual_input" },
      isSatisfied: (ctx) => settingsOpenFor(ctx, "manual_input"),
      wrongActionHint: wrongSettingsHint("Manual Input"),
      hint: "Click the highlighted node",
      onEnter: fitCanvas,
    },

    {
      id: "enter-sample-data",
      title: "Add Sample Data",
      content: `
        <p><strong>Copy</strong> the sample data below, click <strong>Edit JSON</strong>, paste it, then click <strong>Apply JSON to Table</strong>:</p>
        <div style="background: var(--color-background-muted); padding: 8px; border-radius: 4px; margin: 8px 0 4px; font-family: monospace; font-size: 11px; max-height: 120px; overflow: auto;">${SAMPLE_JSON_HTML}</div>
        <button class="tutorial-copy-btn" data-copy="${encodeURIComponent(SAMPLE_JSON)}">
          <span class="material-icons">content_copy</span> Copy sample data
        </button>
        <p style="font-size: 12px; color: var(--color-text-secondary); margin-top: 10px;">Column types are detected automatically. If you type rows by hand instead, set each column's type in its header. Closed the panel? Click the node to reopen it.</p>
      `,
      target: "#rightDrawer",
      position: "left",
      interaction: "free",
      requires: ["flow", "manual_input"],
      nextLabel: "Done",
      highlightPadding: 4,
      onExit: closeSettingsDrawer,
    },

    {
      id: "add-group-by",
      title: "Add a Group By Node",
      content: `
        <p><strong>Grouping</strong> summarizes data by categories — like "total revenue per country".</p>
        <p>Drag the <strong>Group By</strong> node from Aggregations onto the canvas.</p>
      `,
      target: "[data-tutorial-node='group_by']",
      position: "right",
      interaction: "free",
      requires: ["flow"],
      advanceWhen: { event: "node-added", nodeItem: "group_by" },
      isSatisfied: (ctx) => hasNodeOfItem(ctx, "group_by"),
      wrongActionHint: wrongNodeHint("group_by"),
      hint: "Drag Group By onto the canvas",
      highlightPadding: 4,
    },

    {
      id: "connect-input-to-groupby",
      title: "Connect Your Nodes",
      content: `
        <p>Data flows between nodes through <strong>connections</strong>. Both nodes are highlighted on the canvas.</p>
        <p>Drag from the small circle on Manual Input's <strong>right side</strong> to the circle on Group By's <strong>left side</strong>.</p>
        <p style="font-size: 12px; color: var(--color-text-secondary);">Right-side handles send data out; left-side handles take data in.</p>
      `,
      target: nodeTarget("manual_input"),
      secondaryTarget: nodeTarget("group_by"),
      tooltipCorner: true,
      position: "right",
      interaction: "free",
      requires: ["flow", "manual_input", "group_by"],
      advanceWhen: { event: "edge-connected", betweenItems: ["manual_input", "group_by"] },
      isSatisfied: (ctx) => hasEdgeBetweenItems(ctx, "manual_input", "group_by"),
      wrongActionHint: wrongEdgeHint(
        "Drag from Manual Input's right handle to Group By's left handle.",
      ),
      hint: "Connect Manual Input to Group By",
      onEnter: fitCanvas,
    },

    {
      id: "open-groupby-settings",
      title: "Open Group By",
      content: `<p><strong>Click the Group By node</strong> to configure the aggregation.</p>`,
      target: nodeTarget("group_by"),
      position: "left",
      interaction: "free",
      requires: ["flow", "group_by"],
      advanceWhen: { event: "node-settings-opened", nodeOfItem: "group_by" },
      isSatisfied: (ctx) => settingsOpenFor(ctx, "group_by"),
      wrongActionHint: wrongSettingsHint("Group By"),
      hint: "Click the highlighted node",
      onEnter: fitCanvas,
    },

    {
      id: "configure-groupby",
      title: "Configure the Aggregation",
      content: `
        <p>In the column list, <strong>right-click country</strong> and choose <strong>Group by</strong>. Then <strong>right-click revenue</strong> and choose <strong>Sum</strong>.</p>
        <p style="font-size: 12px; color: var(--color-text-secondary);">You can also drag a column into the Settings table and pick its action there. Result: total revenue per country.</p>
      `,
      target: "#rightDrawer",
      position: "left",
      interaction: "free",
      requires: ["flow", "group_by"],
      nextLabel: "Done",
      highlightPadding: 4,
      onExit: closeSettingsDrawer,
    },

    {
      id: "add-write-data",
      title: "Add a Write Data Node",
      content: `
        <p>Time to save your results somewhere.</p>
        <p>Drag the <strong>Write data</strong> node from Output Operations onto the canvas.</p>
      `,
      target: "[data-tutorial-node='output']",
      position: "right",
      interaction: "free",
      requires: ["flow"],
      advanceWhen: { event: "node-added", nodeItem: "output" },
      isSatisfied: (ctx) => hasNodeOfItem(ctx, "output"),
      wrongActionHint: wrongNodeHint("output"),
      hint: "Drag Write data onto the canvas",
      highlightPadding: 4,
    },

    {
      id: "connect-groupby-to-write",
      title: "Connect the Output",
      content: `
        <p>Connect <strong>Group By → Write data</strong> (both highlighted): drag from Group By's right handle to Write data's left handle.</p>
      `,
      target: nodeTarget("group_by"),
      secondaryTarget: nodeTarget("output"),
      tooltipCorner: true,
      position: "right",
      interaction: "free",
      requires: ["flow", "group_by", "output"],
      advanceWhen: { event: "edge-connected", betweenItems: ["group_by", "output"] },
      isSatisfied: (ctx) => hasEdgeBetweenItems(ctx, "group_by", "output"),
      wrongActionHint: wrongEdgeHint(
        "Drag from Group By's right handle to Write data's left handle.",
      ),
      hint: "Connect Group By to Write data",
      onEnter: fitCanvas,
    },

    {
      id: "open-write-settings",
      title: "Open Write Data",
      content: `<p><strong>Click the Write data node</strong> to choose where results go.</p>`,
      target: nodeTarget("output"),
      position: "left",
      interaction: "free",
      requires: ["flow", "output"],
      advanceWhen: { event: "node-settings-opened", nodeOfItem: "output" },
      isSatisfied: (ctx) => settingsOpenFor(ctx, "output"),
      wrongActionHint: wrongSettingsHint("Write data"),
      hint: "Click the highlighted node",
      onEnter: fitCanvas,
    },

    {
      id: "configure-write-data",
      title: "Set the Output File",
      content: `
        <p>Pick a <strong>file name and location</strong> for your CSV — for example <code>revenue_per_country.csv</code>.</p>
      `,
      target: "#rightDrawer",
      position: "left",
      interaction: "free",
      requires: ["flow", "output"],
      nextLabel: "Done",
      highlightPadding: 4,
      onExit: closeSettingsDrawer,
    },

    {
      id: "open-flow-settings",
      title: "Check the Flow Settings",
      content: `
        <p>Before running, click the <strong>gear icon</strong> to open the Flow Settings.</p>
      `,
      target: "[data-tutorial='settings-btn']",
      position: "bottom",
      interaction: "spotlight",
      requires: ["flow"],
      advanceWhen: { event: "flow-settings-opened" },
      isSatisfied: (ctx) => ctx.flowSettingsOpened,
      hint: "Click the gear icon",
      showNextButton: false,
      highlightPadding: 4,
    },

    {
      id: "execution-settings",
      title: "Execution Settings",
      content: `
        <p>Set <strong>Execution Mode</strong> to <strong>Development</strong> for this tutorial — it runs every node and keeps its result so you can inspect each step. <strong>Performance</strong> computes only what the outputs need: faster, but no previews.</p>
        <p style="font-size: 12px; color: var(--color-text-secondary);"><strong>Execution location</strong> picks where the flow runs (local or remote worker); <strong>Parallel workers</strong> caps how many independent nodes run at once on remote runs. More in the
        <a href="https://edwardvaneechoud.github.io/Flowfile/users/build-flows-visually.html?h=build+ship+performanc+development#4-build-in-development-ship-in-performance" target="_blank" rel="noopener" style="color: var(--color-accent); text-decoration: underline;">docs</a>.</p>
        <p>Close the settings when you're done.</p>
      `,
      position: "center",
      corner: "bottom-left",
      interaction: "free",
      veil: false,
      requires: ["flow"],
      nextLabel: "Done",
    },

    {
      id: "run-flow",
      title: "Run Your Flow",
      content: `
        <p>Click <strong>Run</strong> to execute the flow.</p>
        <p style="font-size: 12px; color: var(--color-text-secondary);">In Development mode every node's result is cached, so you can preview each step afterwards.</p>
      `,
      target: "[data-tutorial='run-btn']",
      position: "bottom",
      interaction: "spotlight",
      requires: ["flow"],
      advanceWhen: { event: "flow-run-started" },
      isSatisfied: (ctx) => ctx.runStarted,
      hint: "Click the Run button",
      showNextButton: false,
      highlightPadding: 4,
    },

    {
      id: "watch-run",
      title: "Watch It Execute",
      content: `
        <p>Flowfile processes your data node by node:</p>
        <ul style="margin: 12px 0; padding-left: 20px;">
          <li><strong>Blue</strong> - currently running</li>
          <li><strong>Green</strong> - completed successfully</li>
          <li><strong>Red</strong> - error occurred</li>
        </ul>
      `,
      position: "center",
      interaction: "free",
      advanceWhen: { event: "flow-run-completed" },
      isSatisfied: (ctx) => ctx.runCompleted,
      hint: "Waiting for the run to finish…",
    },

    {
      id: "view-results",
      title: "Explore Your Results",
      content: `
        <p><strong>Click any node</strong> to inspect its output in the data preview.</p>
        <p>Compare Manual Input (raw rows) with Group By (one row per country).</p>
        <p style="font-size: 12px; color: var(--color-text-secondary);">If a node turned red, open it, finish its settings, and Run again. No preview data? Make sure the flow runs in <strong>Development</strong> mode (gear icon → Execution Mode) — Performance mode skips caching previews.</p>
      `,
      position: "center",
      interaction: "free",
    },

    {
      id: "layout-controls",
      title: "Tidy Up with Layout Controls",
      content: `
        <p>Panels piling up? <strong>Click the highlighted button</strong> to open the <strong>Layout Controls</strong> and try them:</p>
        <ul style="margin: 12px 0; padding-left: 20px;">
          <li><strong>Reset Layout Graph</strong> - auto-arrange the nodes on your canvas</li>
          <li><strong>Reset window Layout</strong> - put the panels back in their default spots</li>
          <li><strong>Tile / Cascade</strong> - organize the open panels</li>
        </ul>
      `,
      target: "[data-tutorial='layout-trigger-btn']",
      position: "beside",
      // clears the Layout Controls panel (60px offset + 238px wide, see
      // layoutControls.vue) so the row reads [button][panel][card]
      sideOffset: 314,
      compact: true,
      interaction: "free",
      spotlightShape: "circle",
      highlightPadding: 6,
    },

    {
      id: "save-flow",
      title: "Save Your Flow",
      content: `
        <p>Click the <strong>▼</strong> next to Save and choose <strong>Save As…</strong> to pick a name and folder yourself. (Plain <strong>Save</strong> stores it in Flowfile's default location.)</p>
        <p>Flows are saved as <strong>YAML</strong> — human-readable, Git-friendly, and runnable from the command line.</p>
      `,
      target: "[data-tutorial='save-btn']",
      position: "bottom",
      interaction: "free",
      requires: ["flow"],
      advanceWhen: [{ event: "save-dialog-opened" }, { event: "flow-saved" }],
      isSatisfied: (ctx) => ctx.flowSaved || ctx.saveDialogOpen,
      hint: "Save your flow",
      showNextButton: false,
      highlightPadding: 4,
    },

    {
      id: "save-location",
      title: "Pick a Location",
      content: `
        <p>Choose a folder and file name, then click <strong>Create New File Here</strong>.</p>
        <p style="font-size: 12px; color: var(--color-text-secondary);">The <strong>Catalog</strong> tab registers the flow in the Data Catalog instead — handy for tracking and managing flows from one central place. For now, just save it on your local machine. Closed the dialog? Use ▼ → Save As… again, or plain Save for the default location.</p>
      `,
      position: "center",
      corner: "bottom-left",
      interaction: "free",
      veil: false,
      requires: ["flow"],
      advanceWhen: { event: "flow-saved" },
      isSatisfied: (ctx) => ctx.flowSaved,
      hint: "Choose where to save, then create the file",
      showNextButton: false,
    },

    {
      id: "generate-code",
      title: "Generate Python Code",
      content: `
        <p>Click <strong>Code</strong> to see your flow as Python/Polars code — great for Python projects, learning Polars, or production pipelines.</p>
      `,
      target: "[data-tutorial='generate-code-btn']",
      position: "bottom",
      interaction: "spotlight",
      advanceWhen: { event: "code-generator-opened" },
      isSatisfied: (ctx) => ctx.codeGeneratorOpened,
      hint: "Click the Code button",
      showNextButton: false,
      highlightPadding: 4,
    },

    {
      id: "code-preview",
      title: "Your Python Code",
      content: `
        <p>This is your flow as Python code. It opens on the <strong>FlowFrame</strong> tab (Flowfile's Python API) — switch to the <strong>Polars</strong> tab for pure Polars code with no Flowfile dependency.</p>
        <p><strong>Export Code</strong> saves it as a .py file; <strong>Refresh</strong> regenerates it after changes.</p>
      `,
      position: "center",
      interaction: "free",
      veil: false,
    },

    {
      id: "completion",
      title: "Congratulations!",
      content: `
        <p>You've built, run, saved, and exported your first flow.</p>
        <p>Handy shortcuts for next time:</p>
        <ul style="margin: 12px 0; padding-left: 20px;">
          <li><strong>Ctrl+S</strong> - save flow</li>
          <li><strong>Ctrl+E</strong> - run flow</li>
          <li><strong>Ctrl+G</strong> - generate code</li>
          <li><strong>Ctrl+C/V</strong> - copy/paste nodes</li>
          <li><strong>Delete</strong> - remove selected node</li>
        </ul>
        <p style="margin-top: 16px;">
          <strong>Learn more:</strong> Visit the
          <a href="https://edwardvaneechoud.github.io/Flowfile/" target="_blank" style="color: var(--color-accent); text-decoration: underline;">documentation</a>
          for advanced features and more tutorials.
        </p>
      `,
      position: "center",
      interaction: "modal",
      centerInScreen: true,
      showPrevButton: false,
      canSkip: false,
    },
  ],
};

export default gettingStartedTutorial;
