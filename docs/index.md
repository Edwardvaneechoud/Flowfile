# FlowFile

<figure markdown>
  ![FlowFile Logo](assets/images/logo.png){ width="100px" }
</figure>

FlowFile is a visual ETL tool that combines drag-and-drop workflow building with the speed of Polars dataframes. Build data pipelines visually, transform data using powerful nodes, and analyze results - all without writing code.

<figure markdown>
  ![FlowFile Interface](assets/images/group_by_screenshot.png){ width="800px" }
</figure>

## ⚡ Technical Design

FlowFile operates as three interconnected services:

- **Designer** (Electron + Vue): Visual interface for building data flows
- **Core** (FastAPI): ETL engine using Polars for data transformations (`:5667`)
- **Worker** (FastAPI): Handles computation and caching of data operations (`:8000`)

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
poetry run flowfile_worker  # Starts worker on :8000
poetry run flowfile_core   # Starts core on :5667

# Start desktop app
cd flowfile_frontend
npm install
npm run dev               # Starts Electron app (frontend on :3000)
```