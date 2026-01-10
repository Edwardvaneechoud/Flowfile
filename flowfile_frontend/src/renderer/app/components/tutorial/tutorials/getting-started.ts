// Getting Started Tutorial - Guides users through creating their first flow
import type { Tutorial } from "../../../stores/tutorial-store";
import { useNodeStore } from "../../../stores/column-store";

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
      action: "click",
      showNextButton: false,
      highlightPadding: 4,
    },

    // Step 3: Click Create Flow in modal (no highlight - tooltip stays in corner)
    {
      id: "confirm-create-flow",
      title: "Confirm Flow Creation",
      content: `
        <p>A dialog appeared where you can optionally name your flow.</p>
        <p>Click <strong>Create Flow</strong> in the dialog to continue.</p>
      `,
      position: "center",
      action: "observe",
      showNextButton: false,
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

    // Step 5: Explore the node list
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

    // Step 8: Show node settings with sample data suggestion
    {
      id: "node-settings",
      title: "Add Sample Data",
      content: `
        <p>The <strong>Node Settings</strong> panel lets you configure your data.</p>
        <p><strong>Quick option:</strong> Click <strong>Edit JSON</strong> button, paste this sample data, then click <strong>Apply JSON to table</strong>:</p>
        <div style="background: var(--color-background-muted); padding: 8px; border-radius: 4px; margin: 8px 0; font-family: monospace; font-size: 11px; max-height: 120px; overflow: auto;">
[{"country":"USA","product":"Widget","revenue":1000},<br>
{"country":"Germany","product":"Gadget","revenue":2500},<br>
{"country":"France","product":"Widget","revenue":1800},<br>
{"country":"USA","product":"Gadget","revenue":3200},<br>
{"country":"Germany","product":"Widget","revenue":1500},<br>
{"country":"France","product":"Gadget","revenue":2100}]
        </div>
        <p style="font-size: 12px; color: var(--color-text-secondary);">Or manually add columns (country, product, revenue) and rows. Make sure to change the data type of revenue to "integer".</p>
      `,
      target: "#nodeSettings",
      position: "left",
      action: "observe",
      waitForElement: "#nodeSettings",
      showNextButton: true,
      highlightPadding: 4,
      onExit: () => {
        // Close the node settings panel by deselecting the node
        const nodeStore = useNodeStore();
        nodeStore.nodeId = -1;
      },
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

    // Step 10: Aggregations - More prominent Group By explanation
    {
      id: "aggregations-overview",
      title: "Let's Aggregate Your Data!",
      content: `
        <p style="font-size: 15px; margin-bottom: 16px;"><strong>Grouping</strong> is one of the most powerful data operations. It lets you summarize data by categories.</p>
        <p><strong>Example:</strong> With your sales data, you can answer questions like:</p>
        <ul style="margin: 12px 0; padding-left: 20px;">
          <li>"What is the <strong>total revenue per country</strong>?"</li>
          <li>"How many products were sold in each region?"</li>
          <li>"What's the average order value by product type?"</li>
        </ul>
      `,
      target: "[data-tutorial-category='aggregate']",
      position: "right",
      action: "observe",
      showNextButton: true,
      highlightPadding: 4,
    },

    // Step 11: Drag Group By node
    {
      id: "drag-group-by",
      title: "Add Group By Node",
      content: `
        <p style="font-size: 15px; padding: 12px; background: var(--color-accent-subtle); color: #1a1a1a; border-radius: 6px; margin-bottom: 12px;">
          <strong style="color: inherit;">Drag the Group By node</strong> onto the canvas now!
        </p>
        <p>This will let you calculate the <strong>total revenue per country</strong> from your sales data.</p>
      `,
      target: "[data-tutorial-node='group_by']",
      position: "right",
      action: "drag",
      actionTarget: ".vue-flow",
      showNextButton: true,
      highlightPadding: 4,
    },

    // Step 12: Connect nodes with grouping context
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
        <p><strong>Click and drag</strong> from Manual Input's output to Group By's input.</p>
        <p style="margin-top: 12px;">Once connected, click on Group By to configure it - select <strong>country</strong> as the group column and <strong>sum of revenue</strong> as the aggregation!</p>
      `,
      target: ".vue-flow",
      position: "center",
      action: "observe",
      showNextButton: true,
      highlightPadding: 0,
      onExit: () => {
        // Close the node settings panel by deselecting the node
        const nodeStore = useNodeStore();
        nodeStore.nodeId = -1;
      },
    },

    // Step 13: Write data (output operations)
    {
      id: "output-overview",
      title: "Write Your Results",
      content: `
        <p>The <strong>Output Operations</strong> section lets you save your results:</p>
        <ul style="margin: 12px 0; padding-left: 20px;">
          <li><strong>Write data</strong> - Save to CSV, Parquet, or Excel files</li>
          <li><strong>Database Writer</strong> - Save to a database</li>
          <li><strong>Cloud Storage</strong> - Upload to S3, GCS, etc.</li>
        </ul>
      `,
      target: "[data-tutorial-category='output']",
      position: "right",
      action: "observe",
      showNextButton: true,
      highlightPadding: 4,
    },

    // Step 13: Drag Write data node
    {
      id: "drag-write-data",
      title: "Add Write Data Node",
      content: `
        <p>Now <strong>drag the Write data node</strong> onto the canvas.</p>
        <p>This will let you save your aggregated revenue per country to a CSV file!</p>
        <p>After adding it, connect it to the Group By node's output.</p>
      `,
      target: "[data-tutorial-node='output']",
      position: "right",
      action: "drag",
      actionTarget: ".vue-flow",
      showNextButton: true,
      highlightPadding: 4,
    },

    // Step 14: Connect and configure nodes
    {
      id: "configure-write-data",
      title: "Connect and Configure Your Nodes",
      content: `
        <p>Now it's time to wire everything together!</p>
        <p><strong>Steps to complete:</strong></p>
        <ol style="margin: 12px 0; padding-left: 20px;">
          <li>Connect <strong>Group By → Write data</strong></li>
          <li>Click on <strong>Write data</strong> to set the output file path</li>
        </ol>
        <p style="font-size: 12px; color: var(--color-text-secondary);">Drag from output handles (right side) to input handles (left side) to connect nodes.</p>
      `,
      position: "center",
      action: "observe",
      showNextButton: true,
      onExit: () => {
        // Close the node settings panel by deselecting the node
        const nodeStore = useNodeStore();
        nodeStore.nodeId = -1;
      },
    },

    // Step 15: Execution settings
    {
      id: "execution-settings",
      title: "Configure Execution Mode",
      content: `
        <p>Before running, let's check the <strong>execution settings</strong>.</p>
        <p>Click the <strong>Settings</strong> button to open execution options.</p>
        <p>Make sure <strong>Development</strong> mode is selected - this gives you detailed feedback and is perfect for building and testing flows!</p>
      `,
      target: "[data-tutorial='settings-btn']",
      position: "bottom",
      action: "observe",
      showNextButton: true,
      highlightPadding: 4,
    },

    // Step 16: Run the flow
    {
      id: "run-flow",
      title: "Run Your Flow",
      content: `
        <p>Now click <strong>Run</strong> to execute your flow!</p>
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

    // Step 16: View results and explore pipeline
    {
      id: "view-results",
      title: "Explore Your Results",
      content: `
        <p style="font-size: 15px;">Wait for the success message and then your flow has finished running!</p>
        <p><strong>Click on any node</strong> to see its output in the Table Preview below.</p>
        <p>Try clicking on different nodes:</p>
        <ul style="margin: 12px 0; padding-left: 20px;">
          <li><strong>Manual Input</strong> - Your raw sales data</li>
          <li><strong>Group By</strong> - Total revenue per country</li>
          <li><strong>Write data</strong> - Final output to be saved</li>
        </ul>
        <p style="font-size: 12px; color: var(--color-text-secondary);"><strong>Tip:</strong> If you don't see any data, make sure you have set up the flow to run in Development mode.</p>
      `,
      position: "center",
      action: "observe",
      showNextButton: true,
    },

    // Step 17: Save the flow
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

    // Step 18: YAML format explanation
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
      centerInScreen: true,
    },

    // Step 19: Generate code
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
      action: "click",
      showNextButton: true,
      highlightPadding: 4,
    },

    // Step 20: Code preview
    {
      id: "code-preview",
      title: "Your Python Code",
      content: `
        <p>Here's your flow as <strong>Python/Polars code</strong>!</p>
        <p>You can:</p>
        <ul style="margin: 12px 0; padding-left: 20px;">
          <li>Click <strong>Export Code</strong> to save as a .py file</li>
          <li>Copy the code directly from the editor</li>
          <li>Click <strong>Refresh</strong> to regenerate after changes</li>
        </ul>
        <p>The code is fully functional and can run standalone!</p>
      `,
      position: "center",
      action: "observe",
      showNextButton: true,
    },

    // Step 21: Keyboard shortcuts
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

    // Step 22: Completion
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
        <p style="margin-top: 16px;">
          <strong>Learn more:</strong> Visit our
          <a href="https://docs.flowfile.io" target="_blank" style="color: var(--color-accent); text-decoration: underline;">documentation</a>
          for advanced features and tutorials.
        </p>
      `,
      position: "center",
      action: "observe",
      showNextButton: true,
      showPrevButton: false,
      canSkip: false,
      centerInScreen: true,
    },
  ],
};

export default gettingStartedTutorial;
