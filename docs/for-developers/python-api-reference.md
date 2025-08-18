# Flowfile Core API Reference

This section provides a detailed API reference for the core Python objects, data models, and API routes in `flowfile-core`. The documentation is generated directly from the source code docstrings.

---

## Core Components

This section covers the fundamental classes that manage the state and execution of data pipelines. These are the main "verbs" of the library.

### FlowGraph
The `FlowGraph` is the central object that orchestrates the execution of data transformations. It is built incrementally as you chain operations. This DAG (Directed Acyclic Graph) represents the entire pipeline.

::: flowfile_core.flowfile.flow_graph.FlowGraph
    options:
      show_root_heading: true
      show_signature: true
      show_source: true
      heading_level: 4
      show_symbol_type_heading: true 
      show_root_members_full_path: false
      summary: true
      unwrap_annotated: true
      show_symbol_type_toc: true

### FlowNode
The `FlowNode` represents a single operation in the `FlowGraph`. Each node corresponds to a specific transformation or action, such as filtering or grouping data.

::: flowfile_core.flowfile.flow_node.flow_node.FlowNode
    options:
      show_root_heading: true
      show_signature: true
      show_source: true
      heading_level: 4
      show_symbol_type_heading: true 
      show_root_members_full_path: false
      summary: true
      unwrap_annotated: true
      show_symbol_type_toc: true

### The FlowDataEngine
The `FlowDataEngine` is the primary engine of the library, providing a rich API for data manipulation, I/O, and transformation. Its methods are grouped below by functionality.

::: flowfile_core.flowfile.flow_data_engine.flow_data_engine.FlowDataEngine
    options:
      show_root_heading: true
      show_signature: true
      show_source: true
      heading_level: 4
      show_symbol_type_heading: true 
      show_root_members_full_path: false
      summary: true
      unwrap_annotated: true
      show_symbol_type_toc: true

#### FlowfileColumn

### `FlowfileColumn`
The `FlowfileColumn` is a data class that holds the schema and rich metadata for a single column managed by the `FlowDataEngine`.

::: flowfile_core.flowfile.flow_data_engine.flow_file_column.main.FlowfileColumn
    options:
      show_root_heading: true
      show_signature: true
      show_source: true
      heading_level: 5
      show_symbol_type_heading: true
      show_root_members_full_path: false
      summary: true
      unwrap_annotated: true
      show_symbol_type_toc: true

---

## Data Modeling (Schemas)

This section documents the Pydantic models that define the structure of settings and data.

### `schemas`
::: flowfile_core.schemas.schemas
    options:
      show_root_heading: true
      show_signature: true
      show_source: true
      heading_level: 4
      show_symbol_type_heading: true 
      show_root_members_full_path: false
      summary: true
      unwrap_annotated: true
      show_symbol_type_toc: true


### `input_schema`
::: flowfile_core.schemas.input_schema
    options:
      show_root_heading: true
      show_signature: true
      show_source: true
      heading_level: 4
      show_symbol_type_heading: true 
      show_root_members_full_path: false
      summary: true
      unwrap_annotated: true
      show_symbol_type_toc: true


### `transform_schema`
::: flowfile_core.schemas.transform_schema
    options:
      show_root_heading: true
      show_signature: true
      show_source: true
      heading_level: 4
      show_symbol_type_heading: true 
      show_root_members_full_path: false
      summary: true
      unwrap_annotated: true
      show_symbol_type_toc: true


### `cloud_storage_schemas`
::: flowfile_core.schemas.cloud_storage_schemas
    options:
      show_root_heading: true
      show_signature: true
      show_source: true
      heading_level: 4
      show_symbol_type_heading: true 
      show_root_members_full_path: false
      summary: true
      unwrap_annotated: true
      show_symbol_type_toc: true


### `output_model`
::: flowfile_core.schemas.output_model
    options:
      show_root_heading: true
      show_signature: true
      show_source: true
      heading_level: 4
      show_symbol_type_heading: true 
      show_root_members_full_path: false
      summary: true
      unwrap_annotated: true
      show_symbol_type_toc: true

---

## Web API

This section documents the FastAPI routes that expose `flowfile-core`'s functionality over HTTP.

### `routes`
::: flowfile_core.routes.routes
    options:
      show_root_heading: true
      show_signature: true
      show_source: true
      heading_level: 4
      show_symbol_type_heading: true 
      show_root_members_full_path: false
      summary: true
      unwrap_annotated: true
      show_symbol_type_toc: true

### `auth`
::: flowfile_core.routes.auth
    options:
      show_root_heading: true
      show_signature: true
      show_source: true
      heading_level: 4
      show_symbol_type_heading: true 
      show_root_members_full_path: false
      summary: true
      unwrap_annotated: true
      show_symbol_type_toc: true


### `cloud_connections`
::: flowfile_core.routes.cloud_connections
    options:
      show_root_heading: true
      show_signature: true
      show_source: true
      heading_level: 4
      show_symbol_type_heading: true 
      show_root_members_full_path: false
      summary: true
      unwrap_annotated: true
      show_symbol_type_toc: true

### `logs`
::: flowfile_core.routes.logs
    options:
      show_root_heading: true
      show_signature: true
      show_source: true
      heading_level: 4
      show_symbol_type_heading: true 
      show_root_members_full_path: false
      summary: true
      unwrap_annotated: true
      show_symbol_type_toc: true

### `public`
::: flowfile_core.routes.public
    options:
      show_root_heading: true
      show_signature: true
      show_source: true
      heading_level: 4
      show_symbol_type_heading: true 
      show_root_members_full_path: false
      summary: true
      unwrap_annotated: true
      show_symbol_type_toc: true

### `secrets`
::: flowfile_core.routes.secrets
    options:
      show_root_heading: true
      show_signature: true
      show_source: true
      heading_level: 4
      show_symbol_type_heading: true 
      show_root_members_full_path: false
      summary: true
      unwrap_annotated: true
      show_symbol_type_toc: true
---