<h1 align="center">
  <img src="https://raw.githubusercontent.com/Edwardvaneechoud/Flowfile/main/.github/images/logo.png" alt="Flowfile Logo" width="100">
  <br>
  Flowfile
</h1>

<p align="center">
  <b>Main Repository</b>: <a href="https://github.com/Edwardvaneechoud/Flowfile">Edwardvaneechoud/Flowfile</a><br>
  <b>Documentation</b>: 
  <a href="https://edwardvaneechoud.github.io/Flowfile/">Website</a> - 
  <a href="https://github.com/Edwardvaneechoud/Flowfile/blob/main/flowfile_core/README.md">Core</a> - 
  <a href="https://github.com/Edwardvaneechoud/Flowfile/blob/main/flowfile_worker/README.md">Worker</a> - 
  <a href="https://github.com/Edwardvaneechoud/Flowfile/blob/main/flowfile_frontend/README.md">Frontend</a> - 
  <a href="https://dev.to/edwardvaneechoud/building-flowfile-architecting-a-visual-etl-tool-with-polars-576c">Technical Architecture</a>
</p>

<p>
Flowfile is a visual ETL tool and Python library suite that combines drag-and-drop workflow building with the speed of Polars dataframes. Build data pipelines visually, transform data using powerful nodes, or define data flows programmatically with Python and analyze results - all with high-performance data processing.
</p>

## 🚀 Getting Started

### Installation

Install Flowfile directly from PyPI:

```bash
pip install Flowfile
```

### Quick Start: Web UI

The easiest way to get started is by launching the web-based UI:

```bash
# Start the Flowfile web UI with integrated services
flowfile run ui
```

This will:
- Start the combined core and worker services
- Launch a web interface in your browser
- Provide access to the full visual ETL capabilities

**Options:**
```bash
# Customize host
flowfile run ui --host 0.0.0.0

# Start without opening a browser
flowfile run ui --no-browser
```

You can also start the web UI programmatically:

```python
import flowfile

# Start with default settings
flowfile.start_web_ui()

# Or customize
flowfile.start_web_ui(host="0.0.0.0", port=8080, open_browser=False)
```

### Using the FlowFrame API

Flowfile provides a Polars-like API for defining data pipelines programmatically:

```python
import flowfile as ff
from flowfile import col, open_graph_in_editor

# Create a data pipeline
df = ff.from_dict({
    "id": [1, 2, 3, 4, 5],
    "category": ["A", "B", "A", "C", "B"],
    "value": [100, 200, 150, 300, 250]
})

# Process the data
result = df.filter(col("value") > 150).with_columns([
    (col("value") * 2).alias("double_value")
])

# Open the graph in the web UI (starts the server if needed)
open_graph_in_editor(result.flow_graph)
```

## 📦 Package Components

The `Flowfile` PyPI package includes:

- **Core Service (`flowfile_core`)**: The main ETL engine using Polars
- **Worker Service (`flowfile_worker`)**: Handles computation-intensive tasks
- **Web UI**: Browser-based visual ETL interface
- **FlowFrame API (`flowfile_frame`)**: Polars-like API for Python coding

## ✨ Key Features

### Visual ETL with Web UI

- **No Installation Required**: Launch directly from the pip package
- **Drag-and-Drop Interface**: Build data pipelines visually
- **Integrated Services**: Combined core and worker services
- **Browser-Based**: Access from any device on your network

### FlowFrame API

- **Familiar Syntax**: Polars-like API makes it easy to learn
- **ETL Graph Generation**: Automatically builds visual workflows
- **Lazy Evaluation**: Operations are not executed until needed
- **Interoperability**: Move between code and visual interfaces

### Data Operations

- **Data Cleaning & Transformation**: Complex joins, filtering, etc.
- **High Performance**: Built on Polars for efficient processing
- **Data Integration**: Handle various file formats
- **ETL Pipeline Building**: Create reusable workflows

## 🔄 Common FlowFrame Operations

```python

import flowfile as ff
from flowfile import col, when, lit

# Read data
df = ff.from_dict({
    "id": [1, 2, 3, 4, 5],
    "category": ["A", "B", "A", "C", "B"],
    "value": [100, 200, 150, 300, 250]
})
# df_parquet = ff.read_parquet("data.parquet")
# df_csv = ff.read_csv("data.csv")

other_df = ff.from_dict({
    "product_id": [1, 2, 3, 4, 6],
    "product_name": ["WidgetA", "WidgetB", "WidgetC", "WidgetD", "WidgetE"],
    "supplier": ["SupplierX", "SupplierY", "SupplierX", "SupplierZ", "SupplierY"]
}, flow_graph=df.flow_graph  # Assign the data to the same graph
)

# Filter
filtered = df.filter(col("value") > 150)

# Transform
result = df.select(
    col("id"),
    (col("value") * 2).alias("double_value")
)

# Conditional logic
with_status = df.with_columns([
    when(col("value") > 200).then(lit("High")).otherwise(lit("Low")).alias("status")
])

# Group and aggregate
by_category = df.group_by("category").agg([
    col("value").sum().alias("total"),
    col("value").mean().alias("average")
])

# Join data
joined = df.join(other_df, left_on="id", right_on="product_id")

joined.flow_graph.flow_settings.execution_location = "auto"
joined.flow_graph.flow_settings.execution_mode = "Development"
ff.open_graph_in_editor(joined.flow_graph)  # opens the graph in the UI!

```

## 🧰 Command-Line Interface

```bash
# Show help and version info
flowfile

# Start the web UI
flowfile run ui [options]

# Run individual services
flowfile run core --host 0.0.0.0 --port 63578
flowfile run worker --host 0.0.0.0 --port 63579
```

## 📚 Resources

- **[Main Repository](https://github.com/Edwardvaneechoud/Flowfile)**: Latest code and examples
- **[Documentation](https://edwardvaneechoud.github.io/Flowfile/)**: Comprehensive guides
- **[Technical Architecture](https://dev.to/edwardvaneechoud/building-flowfile-architecting-a-visual-etl-tool-with-polars-576c)**: Design overview

## 🖥️ Full Application Options

For the complete visual ETL experience, you have additional options:

- **Desktop Application**: Download from the [main repository](https://github.com/Edwardvaneechoud/Flowfile#-getting-started)
- **Docker Setup**: Run with Docker Compose
- **Manual Setup**: For development environments

## 📋 Development Roadmap

See the [main repository](https://github.com/Edwardvaneechoud/Flowfile#-todo) for the latest development roadmap and TODO list.