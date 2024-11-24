<h1 align="center">
  <img src=".github/images/logo.png" alt="Flowfile Logo" width="100">
  <br>
  Flowfile
</h1>
<p align="center">
  <b>Documentation</b>:
  <a href="https://edwardvaneechoud.github.io/Flowfile/">Website</a>
  -
  <a href="flowfile_core/README.md">Core</a>
  -
  <a href="flowfile_worker/README.md">Worker</a>
  -
  <a href="flowfile_frontend/README.md">Frontend</a>
</p>
<p>
Flowfile is a visual ETL tool that combines drag-and-drop workflow building with the speed of Polars dataframes. Build data pipelines visually, transform data using powerful nodes, and analyze results - all without writing code.
</p>

<div align="center">
  <img src=".github/images/group_by_screenshot.png" alt="Flowfile Interface" width="800"/>
</div>

## ⚡ Technical Design

Flowfile operates as three interconnected services:

- **Designer** (Electron + Vue): Visual interface for building data flows
- **Core** (FastAPI): ETL engine using Polars for data transformations (`:63578`)
- **Worker** (FastAPI): Handles computation and caching of data operations (`:63579`)

Each flow is represented as a directed acyclic graph (DAG), where nodes represent data operations and edges represent data flow between operations.

## 🔥 Example Use Cases

- **Data Cleaning & Transformation**
  - Complex joins (fuzzy matching)
  - Text to rows transformations
  - Advanced filtering and grouping
  - Custom formulas and expressions
  - Filter data based on conditions

- **Data Integration**
  - Combine data from multiple sources
  - Fuzzy matching of customer records
  - Standardize data formats
  - Handle messy Excel files

- **ETL Operations**
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
poetry run flowfile_worker  # Starts worker on :63579
poetry run flowfile_core   # Starts core on :63578

# Start desktop app
cd flowfile_frontend
npm install
npm run dev               # Starts Electron app (frontend on :3000)
```

### Development

```bash
# Python dependencies
poetry shell
poetry add <package>

# Build desktop app
npm run build        # All platforms
npm run build:win    # Windows
npm run build:mac    # macOS
npm run build:linux  # Linux
```

## 📋 TODO

### Core Features
- [ ] Add cloud storage support
  - S3 integration
  - Azure Data Lake Storage (ADLS)
- [ ] Multi-flow execution support
- [ ] Polars code reverse engineering
  - Generate Polars code from visual flows
  - Import existing Polars scripts

### Documentation
- [ ] Add comprehensive docstrings
- [ ] Create detailed node documentation
- [ ] Add architectural documentation
- [ ] Improve inline code comments
- [ ] Create user guides and tutorials

### Infrastructure
- [ ] Implement proper testing
- [ ] Add CI/CD pipeline
- [ ] Improve error handling
- [ ] Add monitoring and logging

## 📝 License

[MIT License](LICENSE)

## Acknowledgments

Built with Polars, Vue.js, FastAPI, Vueflow and Electron.
