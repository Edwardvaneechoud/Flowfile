# Building Flows

Flowfile allows you to create data pipelines visually by connecting nodes that represent different data operations. This guide will walk you through the process of creating and running flows.


## Interface Overview

![Flowfile Interface Overview](../../assets/images/ui/full_ui.png){ width="800px" }
*The complete Flowfile interface showing:*
- **Left sidebar**: Browse and select from available nodes 
- **Center canvas**: Build your flow by arranging and connecting nodes
- **Right sidebar**: Configure node settings and parameters
- **Bottom panel**: Preview data at each step


## Creating a Flow

  ![Flowfile Landing](../../assets/images/ui/landing.png){ width="800px" }
  <figcaption>The Flowfile landing page when no flows are active, showing options to create a new flow or open an existing one</figcaption>

### Starting a New Flow
1. Click the **Create** button in the top toolbar
2. A new empty canvas will open
3. Save your flow at any time using the **Save** button
4. Files are saved with the `.flowfile` extension

### Adding Nodes
1. Browse nodes in the left sidebar, organized by category:
     - Input Sources (for loading data)
     - Transformations (for modifying data)
     - Combine Operations (for joining data)
     - Aggregations (for summarizing data)
     - Output Destinations (for saving data)
2. Drag any node onto the canvas
3. Connect nodes to create a flow

## Configuring Nodes

### Node Settings
![Flowfile Interface Overview](../../assets/images/ui/node_settings.png){ width="800px" }
1. Click any node to open its settings in the right sidebar
2. Each node type has specific configuration options:

### Data Preview
1. After configuration, each node shows the output schema of the action
2. Click on the **run** button to execute the node
3. The preview panel will show the output data

## Running Your Flow

### 1. Execution Options
Choose your xecution mode from the dropdown:
     - **Development**: Lets you view the data in every step of the process, at the cost of performance
     - **Performance**: Only executes steps needed for the output (e.g., writing data), allowing for query optimizations and better performance

### 2. Running the Flow
1. Click the **Run** button in the top toolbar
2. Watch the execution progress:
     - 🟢 Green: Success
     - 🔴 Red: Error
     - 🟡 Yellow: Warning
     - ⚪ White: Not executed

### 3. Viewing Results
1. Click any node after execution to see its output data
2. Review the results in the preview panel
3. Check for any errors or warnings
4. Export results using output nodes

## Example Flow

Here's a typical flow that demonstrates common operations:
![graph](../../assets/images/ui/graph.png){ width="800px" }

## Best Practices

### Organization
  - Give your flows clear, descriptive names
  - Arrange nodes logically from left to right
  - Group related operations together
  - Use comments or node labels for documentation

### Development
  - Save your work frequently
  - Test with a subset of data first
  - Use the auto-run mode during development
  - Break complex flows into smaller, manageable parts

### Troubleshooting
  - Check node configurations if errors occur
  - Review data previews to understand transformations
  - Ensure data types match between connected nodes
  - Look for error messages in node status

## Tips and Tricks

- **Node Management**:
      - Double-click canvas to pan
      - Use mouse wheel to zoom
      - Hold Shift to select multiple nodes
      - Right-click for context menu
      - Right click on the text to add notes

- **Data Handling**:
    - Use sample nodes during development
    - Preview data frequently
    - Check column types early
    - Monitor memory usage

---

## Next Steps

After mastering basic flows, explore:

  - [Input sources](nodes/input.md)
  - [Complex transformations](nodes/transform.md)
  - [Data aggregation techniques](nodes/aggregate.md)
  - [Advanced joining methods](nodes/combine.md)
  - [Output options](nodes/output.md)