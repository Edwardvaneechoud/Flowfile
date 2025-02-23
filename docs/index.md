# Flowfile

<div style="display: flex; align-items: center; gap: 20px;">
  <img src="assets/images/logo.png" alt="Flowfile Logo" width="100px">
  <p>Flowfile is a visual ETL tool that combines drag-and-drop workflow building with the speed of Polars dataframes. Build data pipelines visually, transform data using powerful nodes, and analyze results - all without writing code.</p>
</div>

<figure markdown>
  ![Flowfile Interface](assets/images/generic_screenshot.png){ width="800px" }
</figure>

## ⚡ Technical Design

Flowfile operates as three interconnected services:

- **Designer** (Electron + Vue): Visual interface for building data flows
- **Core** (FastAPI): ETL engine using Polars for data transformations (`:63578`)
- **Worker** (FastAPI): Handles computation and caching of data operations (`:63579`)

Each flow is represented as a directed acyclic graph (DAG), where nodes represent data operations and edges represent data flow between operations.

## 🔥 Example Use Cases

### Data Cleaning & Transformation
- Complex joins (fuzzy matching)
- Text to rows transformations
- Advanced filtering and grouping
- Custom formulas and expressions
- Filter data based on conditions

### Data Integration
- Combine data from multiple sources
- Fuzzy matching of customer records
- Standardize data formats
- Handle messy Excel files

### ETL Operations
- Excel to database pipelines
- Data quality checks

## 🚀 Getting Started

### Building Your First Flow

Flowfile allows you to create data pipelines visually:

1. Click **Create** to start a new `.flowfile`
2. Drag nodes from the left sidebar onto the canvas
3. Connect nodes by dragging between them
4. Configure each node by clicking on it
5. Click **Run** to execute your flow
6. Click on nodes after running to preview results

[Learn more about building flows](flows/building.md)

### Prerequisites
- Python 3.10+
- Node.js 16+
- Poetry (Python package manager)
- Make (optional, for build automation)

### Installation & Running

Using Make:
```bash
make all               # Install and build everything
make clean            # Clean build artifacts
```

Manual installation:
```bash
# Install Python dependencies
poetry install

# Start services (in separate terminals)
poetry run flowfile_worker  # Starts worker on :63579
poetry run flowfile_core   # Starts core on :63578

# Start desktop app
cd flowfile_frontend
npm install
npm run dev               # Starts Electron app (frontend on :3000)
```
## 👉 Next Steps

- [Quickstart Guide](quickstart.md)
- [Building Flows](flows/building.md)
- [Node Overview](nodes/index.md)