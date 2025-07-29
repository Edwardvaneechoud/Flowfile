# Flowfile Core

Flowfile Core is the central engine of Flowfile, providing a powerful ETL (Extract, Transform, Load) framework built on top of Polars. It manages data transformations through a directed acyclic graph (DAG) structure, enabling complex data processing workflows.

## 🚀 Features

- **DAG-based ETL Engine**
  - Visual flow creation and management
  - Node-based data transformations
  - Dependency tracking and execution ordering
  - Intelligent caching system

- **Rich Node Library**
  - Data Input Nodes (CSV, Excel, Parquet, JSON)
  - Transformation Nodes (Filter, Join, Group By, Sort)
  - Formula and Expression Nodes
  - Output and Export Nodes

- **Advanced Data Operations**
  - Cross Joins and Fuzzy Matching
  - Text to Rows Transformation
  - Pivot and Unpivot Operations
  - Record ID Generation
  - Data Sampling

- **External Data Sources**
  - Custom Source Extensions

## 🔧 Core Components

### FlowGraph
The main class handling the ETL workflow:
```python
from flowfile_core.flowfile.handler import FlowfileHandler

# Create a new flow
handler = FlowfileHandler()
flow = handler.add_flow(name="my_flow")

# Add nodes and connections
flow.add_read(input_file)
flow.add_filter(filter_settings)
flow.add_formula(formula_settings)
```

### Node Types
```python
# Available node types include:
- read            # Data reading
- filter          # Data filtering
- formula         # Custom formulas
- group_by        # Aggregations
- join            # Data joining
- pivot           # Pivot operations
- sort            # Data sorting
- output          # Data output
# And many more...
```

## 📋 API Overview

```python
# Flow Management
flow.add_node(...)            # Add a new node
flow.delete_node(...)         # Remove a node
flow.add_connection(...)      # Connect nodes
flow.run_graph()             # Execute the flow

# Data Operations
flow.add_formula(...)         # Add calculations
flow.add_filter(...)          # Add data filters
flow.add_join(...)           # Join datasets

# Flow Control
flow.get_execution_location() # Get execution context
flow.save_flow(...)          # Save flow configuration
flow.reset()                 # Reset flow state
```

## 🤝 Contributing

Contributions are welcome! Please read our contributing guidelines before submitting pull requests.

## 📝 License

[MIT License](LICENSE)