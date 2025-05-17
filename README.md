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
  -
  <a href="https://dev.to/edwardvaneechoud/building-flowfile-architecting-a-visual-etl-tool-with-polars-576c">Technical Architecture</a>
</p>
<p>
Flowfile is a visual ETL tool that combines drag-and-drop workflow building with the speed of Polars dataframes. Build data pipelines visually, transform data using powerful nodes, and analyze results - all without writing code. It also includes flowfile_frame, a Python API for defining data flows programmatically with a Polars-like syntax.
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

For a deeper dive into the technical architecture, check out [this article](https://dev.to/edwardvaneechoud/building-flowfile-architecting-a-visual-etl-tool-with-polars-576c) on how Flowfile leverages Polars for efficient data processing.

## 🔥 Example Use Cases

- **Data Cleaning & Transformation**
  - Complex joins (fuzzy matching)
  - Text to rows transformations
  - Advanced filtering and grouping
  - Custom formulas and expressions
  - Filter data based on conditions

<div align="center">
  <img src=".github/images/flowfile_demo_1.gif" alt="Flowfile Layout" width="800"/>
</div>

---

- **Performance**
  - Build to scale out of core
  - Using polars for data processing

<div align="center">
  <img src=".github/images/demo_flowfile_write.gif" alt="Flowfile Layout" width="800"/>
</div>

---

### **Data Integration**
  - Standardize data formats
  - Handle messy Excel files


<div align="center">
  <img src=".github/images/read_excel_flowfile.gif" alt="Flowfile Layout" width="800"/>
</div>


---

- **ETL Operations**
  - Data quality checks


## 🚀 Getting Started

### Prerequisites
- Python 3.10+
- Node.js 16+
- Poetry (Python package manager)
- Docker & Docker Compose (option, for Docker setup)
- Make (optional, for build automation)

### Installation Options

#### 1. Desktop Application
The desktop version offers the best experience with a native interface and integrated services. You can either:

**Option A: Download Pre-built Application** 
- Download the latest release from [GitHub Releases](https://github.com/Edwardvaneechoud/Flowfile/releases)
- Run the installer for your platform (Windows, macOS, or Linux)
  - Note: You may see security warnings since the installer isn't signed. On Windows, click "More info" then "Run anyway". On macOS, right-click the app, select "Open", then confirm. These warnings appear because the app isn't signed with a developer certificate.

**Option B: Build from Source:**
```bash
git clone https://github.com/edwardvaneechoud/Flowfile.git
cd Flowfile

# Build packaged executable
make    # Creates platform-specific executable

# Or manually:
poetry install
poetry run build_backends
cd flowfile_frontend
npm install
npm run build      # All platforms
```

#### 4. Manual Setup (Development)
Ideal for development work when you need direct access to all services and hot-reloading:

```bash
git clone https://github.com/edwardvaneechoud/Flowfile.git
cd Flowfile

# Install Python dependencies
poetry install

# Start backend services
poetry run flowfile_worker  # Starts worker on :63579
poetry run flowfile_core   # Starts core on :63578

# Start web frontend
cd flowfile_frontend
npm install
npm run dev:web  # Starts web interface on :8080
```

#### 5. Python Package (PyPI)
Install Flowfile directly from PyPI to use both the visual UI and the programmatic API:

```bash
pip install Flowfile
```

##### Launch the Web UI
Start the web-based UI with a single command:

```bash
# Start the Flowfile web UI with integrated services
flowfile run ui
```

##### FlowFrame API
The package includes `flowfile_frame`, a Python module that provides a Polars-like API for creating data pipelines programmatically:

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

# Open the graph in the web UI
open_graph_in_editor(result.flow_graph)
```

For more details, see the [flowfile_frame documentation](flowfile_frame/readme.md).

### Visualizing and Sharing Pipelines

One of the most powerful features is the ability to visualize your data transformation pipelines:

- **Inspect Data Flow**: See exactly how your data is transformed step by step
- **Debugging**: Identify issues in your data pipeline visually
- **Documentation**: Share your data transformation logic with teammates visually
- **Iteration**: Modify your pipeline in the Designer UI and export it back to code

For more details on using the FlowFrame API, see the [flowfile_frame documentation](flowfile_frame/README.md).


## 📋 TODO

### Core Features
- [ ] Add cloud storage support
  - S3 integration
  - Azure Data Lake Storage (ADLS)
- [x] Multi-flow execution support
- [ ] Polars code reverse engineering
  - Generate Polars code from visual flows
  - Import existing Polars scripts

### Documentation
- [ ] Add comprehensive docstrings
- [x] Create detailed node documentation
- [x] Add architectural documentation
- [ ] Improve inline code comments
- [ ] Create user guides and tutorials

### Infrastructure
- [ ] Implement proper testing
- [x] Add CI/CD pipeline
- [x] Improve error handling
- [x] Add monitoring and logging

## 📝 License

[MIT License](LICENSE)

## Acknowledgments

Built with Polars, Vue.js, FastAPI, Vueflow and Electron.
