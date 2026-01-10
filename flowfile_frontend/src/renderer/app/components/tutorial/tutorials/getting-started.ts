// Getting Started Tutorial - Guides users through creating their first flow
import type { Tutorial } from "../../../stores/tutorial-store";

export const gettingStartedTutorial: Tutorial = {
  id: "getting-started",
  name: "Getting Started with Flowfile",
  description: "Learn how to create your first data flow - from input to output",
  steps: [
    // Step 1: Welcome
    {
      id: "welcome",
      title: "Welcome to Flowfile!",
      content: `
        <p>In this tutorial, you'll learn how to build your first data flow.</p>
        <p>We'll cover:</p>
        <ul style="margin: 12px 0; padding-left: 20px;">
          <li>Creating a new flow</li>
          <li>Adding data with Manual Input</li>
          <li>Transforming data with Group By</li>
          <li>Writing results to a CSV file</li>
          <li>Running your flow</li>
          <li>Saving as YAML</li>
        </ul>
        <p>Let's get started!</p>
      `,
      position: "center",
      action: "observe",
      showNextButton: true,
      showPrevButton: false,
    },

    // Step 2: Click Quick Create button
    {
      id: "click-quick-create",
      title: "Create a New Flow",
      content: `
        <p>First, let's create a new flow.</p>
        <p>Click the <strong>Quick Create</strong> button to open the flow creation dialog.</p>
      `,
      target: "[data-tutorial='quick-create-btn']",
      position: "bottom",
      action: "observe",
      showNextButton: true,
      highlightPadding: 4,
    },

    // Step 3: Click Create Flow in modal
    {
      id: "confirm-create-flow",
      title: "Confirm Flow Creation",
      content: `
        <p>A dialog appeared where you can optionally name your flow.</p>
        <p>Click <strong>Create Flow</strong> to create the flow with an auto-generated name, or enter a custom name first.</p>
      `,
      target: "[data-tutorial='create-flow-confirm-btn']",
      position: "top",
      action: "observe",
      waitForElement: "[data-tutorial='create-flow-confirm-btn']",
      showNextButton: true,
      highlightPadding: 4,
    },

    // Step 4: Explore the canvas
    {
      id: "explore-canvas",
      title: "Your Flow Canvas",
      content: `
        <p>This is your <strong>flow canvas</strong> - the workspace where you'll build your data pipelines.</p>
        <p>On the left, you'll see the <strong>Data Actions</strong> panel with all available nodes organized by category.</p>
      `,
      target: ".vue-flow",
      position: "center",
      action: "observe",
      showNextButton: true,
      highlightPadding: 0,
    },

    // Step 4: Explore the node list
    {
      id: "node-list",
      title: "Available Nodes",
      content: `
        <p>The <strong>Data Actions</strong> panel contains all the nodes you can use:</p>
        <ul style="margin: 12px 0; padding-left: 20px;">
          <li><strong>Input Sources</strong> - Load data from files, databases, or manual input</li>
          <li><strong>Transformations</strong> - Filter, select, sort, and modify data</li>
          <li><strong>Combine Operations</strong> - Join and merge datasets</li>
          <li><strong>Aggregations</strong> - Group and summarize data</li>
          <li><strong>Output Operations</strong> - Write results to files or databases</li>
        </ul>
      `,
      target: "[data-tutorial='node-list']",
      position: "right",
      action: "observe",
      showNextButton: true,
      highlightPadding: 4,
    },

    // Step 5: Find Manual Input
    {
      id: "find-manual-input",
      title: "Input Sources",
      content: `
        <p>Let's add some data to work with!</p>
        <p>The <strong>Input Sources</strong> category contains nodes for bringing data into your flow:</p>
        <ul style="margin: 12px 0; padding-left: 20px;">
          <li><strong>Read</strong> - Load CSV, Parquet, or other files</li>
          <li><strong>Manual Input</strong> - Enter sample data directly</li>
          <li><strong>Database Reader</strong> - Query databases</li>
          <li><strong>External Source</strong> - Connect to APIs</li>
        </ul>
      `,
      target: "[data-tutorial-category='input']",
      position: "right",
      action: "observe",
      showNextButton: true,
      highlightPadding: 4,
    },

    // Step 6: Highlight Manual Input node
    {
      id: "drag-manual-input",
      title: "Drag Manual Input to Canvas",
      content: `
        <p>Find <strong>Manual Input</strong> and <strong>drag it</strong> onto the canvas.</p>
        <p>This node lets you define sample data directly in the flow - perfect for testing!</p>
      `,
      target: "[data-tutorial-node='manual_input']",
      position: "right",
      action: "drag",
      actionTarget: ".vue-flow",
      showNextButton: true,
      highlightPadding: 4,
    },

    // Step 7: Node added - click to configure
    {
      id: "configure-manual-input",
      title: "Configure Your Data",
      content: `
        <p>Once you've added the node, <strong>click on it</strong> to open its settings.</p>
        <p>The settings panel will appear on the right where you can define your data columns and values.</p>
      `,
      target: ".vue-flow__node",
      position: "left",
      action: "click",
      waitForElement: ".vue-flow__node",
      showNextButton: true,
      highlightPadding: 8,
    },

    // Step 8: Show node settings
    {
      id: "node-settings",
      title: "Node Settings Panel",
      content: `
        <p>The <strong>Node Settings</strong> panel lets you configure each node.</p>
        <p>For Manual Input, you can:</p>
        <ul style="margin: 12px 0; padding-left: 20px;">
          <li>Define column names and types</li>
          <li>Enter sample data rows</li>
          <li>Import data from clipboard</li>
        </ul>
        <p>Try adding columns like "category" and "value"!</p>
      `,
      target: "#nodeSettings",
      position: "left",
      action: "observe",
      waitForElement: "#nodeSettings",
      showNextButton: true,
      highlightPadding: 4,
    },

    // Step 9: Explore transformations
    {
      id: "transformations-overview",
      title: "Transformation Nodes",
      content: `
        <p>Now let's explore the <strong>transformation</strong> options available:</p>
        <ul style="margin: 12px 0; padding-left: 20px;">
          <li><strong>Filter</strong> - Keep only rows matching conditions</li>
          <li><strong>Select</strong> - Choose and rename columns</li>
          <li><strong>Sort</strong> - Order rows by column values</li>
          <li><strong>Formula</strong> - Create calculated columns</li>
          <li><strong>Polars Code</strong> - Write custom Polars code</li>
        </ul>
      `,
      target: "[data-tutorial-category='transform']",
      position: "right",
      action: "observe",
      showNextButton: true,
      highlightPadding: 4,
    },

    // Step 10: Aggregations
    {
      id: "aggregations-overview",
      title: "Aggregation Nodes",
      content: `
        <p>The <strong>Aggregations</strong> section contains:</p>
        <ul style="margin: 12px 0; padding-left: 20px;">
          <li><strong>Group By</strong> - Aggregate data by categories</li>
          <li><strong>Pivot</strong> - Reshape data (rows to columns)</li>
          <li><strong>Unpivot</strong> - Reshape data (columns to rows)</li>
        </ul>
        <p>Drag a <strong>Group By</strong> node to the canvas to aggregate your data!</p>
      `,
      target: "[data-tutorial-category='aggregate']",
      position: "right",
      action: "observe",
      showNextButton: true,
      highlightPadding: 4,
    },

    // Step 11: Connect nodes
    {
      id: "connect-nodes",
      title: "Connect Your Nodes",
      content: `
        <p>To make data flow between nodes, you need to <strong>connect them</strong>.</p>
        <p>Look for the small circles (handles) on each node:</p>
        <ul style="margin: 12px 0; padding-left: 20px;">
          <li><strong>Right side</strong> - Output handle (data goes out)</li>
          <li><strong>Left side</strong> - Input handle (data comes in)</li>
        </ul>
        <p>Click and drag from an output to an input to create a connection!</p>
      `,
      target: ".vue-flow",
      position: "center",
      action: "observe",
      showNextButton: true,
      highlightPadding: 0,
    },

    // Step 12: Output operations
    {
      id: "output-overview",
      title: "Output Nodes",
      content: `
        <p>The <strong>Output Operations</strong> section lets you save your results:</p>
        <ul style="margin: 12px 0; padding-left: 20px;">
          <li><strong>Output</strong> - Write to CSV, Parquet, or Excel</li>
          <li><strong>Database Writer</strong> - Save to a database</li>
          <li><strong>Cloud Storage</strong> - Upload to S3, GCS, etc.</li>
        </ul>
        <p>Drag an <strong>Output</strong> node and connect it to see your results!</p>
      `,
      target: "[data-tutorial-category='output']",
      position: "right",
      action: "observe",
      showNextButton: true,
      highlightPadding: 4,
    },

    // Step 13: Run the flow
    {
      id: "run-flow",
      title: "Run Your Flow",
      content: `
        <p>Once your nodes are connected, click <strong>Run</strong> to execute your flow!</p>
        <p>Flowfile will process your data through each node in sequence.</p>
        <p>Watch the nodes change color as they execute:</p>
        <ul style="margin: 12px 0; padding-left: 20px;">
          <li><strong>Blue</strong> - Currently running</li>
          <li><strong>Green</strong> - Completed successfully</li>
          <li><strong>Red</strong> - Error occurred</li>
        </ul>
      `,
      target: "[data-tutorial='run-btn']",
      position: "bottom",
      action: "observe",
      showNextButton: true,
      highlightPadding: 4,
    },

    // Step 14: Table preview
    {
      id: "table-preview",
      title: "Preview Your Data",
      content: `
        <p>Click on any node to see its output in the <strong>Table Preview</strong> panel at the bottom.</p>
        <p>This lets you inspect your data at each step of the pipeline!</p>
        <p>You can:</p>
        <ul style="margin: 12px 0; padding-left: 20px;">
          <li>Scroll through rows and columns</li>
          <li>See data types for each column</li>
          <li>Resize the panel by dragging</li>
        </ul>
      `,
      position: "center",
      action: "observe",
      showNextButton: true,
    },

    // Step 15: Save the flow
    {
      id: "save-flow",
      title: "Save Your Flow",
      content: `
        <p>Let's save your flow so you can use it again!</p>
        <p>Click the <strong>Save</strong> button to choose where to save your flow file.</p>
      `,
      target: "[data-tutorial='save-btn']",
      position: "bottom",
      action: "observe",
      showNextButton: true,
      highlightPadding: 4,
    },

    // Step 16: YAML format explanation
    {
      id: "yaml-format",
      title: "Flows are Saved as YAML",
      content: `
        <p>Flowfile saves your flows in <strong>YAML format</strong> (.yaml or .yml).</p>
        <p>This means your flows are:</p>
        <ul style="margin: 12px 0; padding-left: 20px;">
          <li><strong>Human-readable</strong> - Open and edit in any text editor</li>
          <li><strong>Version-control friendly</strong> - Track changes with Git</li>
          <li><strong>Portable</strong> - Share flows with your team</li>
          <li><strong>Scriptable</strong> - Run from command line</li>
        </ul>
        <p>You can even edit your flows manually if needed!</p>
      `,
      position: "center",
      action: "observe",
      showNextButton: true,
    },

    // Step 17: Generate code
    {
      id: "generate-code",
      title: "Generate Python Code",
      content: `
        <p>Want to use your flow in Python?</p>
        <p>Click <strong>Generate Code</strong> to export your flow as Python/Polars code!</p>
        <p>This is great for:</p>
        <ul style="margin: 12px 0; padding-left: 20px;">
          <li>Integrating with Python projects</li>
          <li>Learning how Polars works</li>
          <li>Creating production pipelines</li>
        </ul>
      `,
      target: "[data-tutorial='generate-code-btn']",
      position: "bottom",
      action: "observe",
      showNextButton: true,
      highlightPadding: 4,
    },

    // Step 18: Keyboard shortcuts
    {
      id: "keyboard-shortcuts",
      title: "Keyboard Shortcuts",
      content: `
        <p>Speed up your workflow with keyboard shortcuts:</p>
        <ul style="margin: 12px 0; padding-left: 20px;">
          <li><strong>Ctrl+S</strong> - Save flow</li>
          <li><strong>Ctrl+E</strong> - Run flow</li>
          <li><strong>Ctrl+G</strong> - Generate code</li>
          <li><strong>Ctrl+N</strong> - Quick create new flow</li>
          <li><strong>Ctrl+C/V</strong> - Copy/paste nodes</li>
          <li><strong>Delete</strong> - Remove selected node</li>
        </ul>
      `,
      position: "center",
      action: "observe",
      showNextButton: true,
    },

    // Step 19: Completion
    {
      id: "completion",
      title: "Congratulations!",
      content: `
        <p>You've completed the Getting Started tutorial!</p>
        <p>You now know how to:</p>
        <ul style="margin: 12px 0; padding-left: 20px;">
          <li>Create a new flow</li>
          <li>Add and configure nodes</li>
          <li>Connect nodes to build pipelines</li>
          <li>Run your flow and preview data</li>
          <li>Save and export your work</li>
        </ul>
        <p><strong>Tip:</strong> Click the Tutorial button anytime to replay this guide!</p>
      `,
      position: "center",
      action: "observe",
      showNextButton: true,
      showPrevButton: false,
      canSkip: false,
    },
  ],
};

export default gettingStartedTutorial;
