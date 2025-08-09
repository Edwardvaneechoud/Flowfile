# Python API Reference

This section provides a detailed API reference for the core Python objects in `flowfile-core`. The documentation is generated directly from the source code docstrings.

---
## ::: flowfile_core.flowfile.flow_graph.FlowGraph
    options:
      show_root_heading: true
      show_source: false
      heading_level: 3
---
## ::: flowfile_core.flowfile.flow_node.flow_node.FlowNode
    options:
      show_root_heading: true
      show_source: false
      heading_level: 3
---

## ::: flowfile_core.flowfile.flow_data_engine.flow_data_engine.FlowDataEngine
    options:
      show_root_heading: true
      show_source: false
      heading_level: 3
      # We will select members manually to group them logically
      members:
        - __init__

### 📈 Core Properties & State

These properties provide access to the underlying data and metadata of the `FlowDataEngine`.

::: flowfile_core.flowfile.flow_data_engine.flow_data_engine.FlowDataEngine
    options:
      show_root_heading: false
      show_source: false
      members:
        - data_frame
        - schema
        - columns
        - name
        - number_of_records
        - number_of_fields
        - errors
        - has_errors
        - lazy
        - is_future
        - is_collected
        - external_source
        - get_number_of_records
        - count
        - cache

### 🏭 Creation Methods

Use these class methods to create a `FlowDataEngine` from various sources.

::: flowfile_core.flowfile.flow_data_engine.flow_data_engine.FlowDataEngine
    options:
      show_root_heading: false
      show_source: false
      members:
        - from_cloud_storage_obj
        - create_from_external_source
        - create_from_sql
        - create_from_schema
        - create_from_path
        - create_random
        - generate_enumerator

### 💾 Data I/O and Conversion

Methods for reading from and writing to external systems, or converting the data to other formats.

::: flowfile_core.flowfile.flow_data_engine.flow_data_engine.FlowDataEngine
    options:
      show_root_heading: false
      show_source: false
      members:
        - to_cloud_storage_obj
        - save
        - output
        - to_pylist
        - to_arrow
        - to_raw_data
        - to_dict

### 🔬 Data Sampling & Access

Methods for inspecting the data by collecting all or a subset of records.

::: flowfile_core.flowfile.flow_data_engine.flow_data_engine.FlowDataEngine
    options:
      show_root_heading: false
      show_source: false
      members:
        - collect
        - get_sample
        - get_subset
        - get_output_sample
        - iter_batches

### ✨ Data Transformations

These methods form the core of your data manipulation pipelines.

#### Shaping & Restructuring
::: flowfile_core.flowfile.flow_data_engine.flow_data_engine.FlowDataEngine
    options:
      show_root_heading: false
      show_source: false
      members:
        - do_group_by
        - do_pivot
        - unpivot
        - do_sort
        - split
        - make_unique
        - concat

#### Joining
::: flowfile_core.flowfile.flow_data_engine.flow_data_engine.FlowDataEngine
    options:
      show_root_heading: false
      show_source: false
      members:
        - join
        - do_cross_join
        - do_fuzzy_join
        - fuzzy_match
        - start_fuzzy_join

#### Filtering & Column Selection
::: flowfile_core.flowfile.flow_data_engine.flow_data_engine.FlowDataEngine
    options:
      show_root_heading: false
      show_source: false
      members:
        - do_filter
        - select_columns
        - do_select
        - drop_columns
        - reorganize_order

#### Column Operations & Formulas
::: flowfile_core.flowfile.flow_data_engine.flow_data_engine.FlowDataEngine
    options:
      show_root_heading: false
      show_source: false
      members:
        - apply_flowfile_formula
        - apply_sql_formula
        - add_record_id
        - change_column_types
        - add_new_values

### 🌐 Graph Operations
::: flowfile_core.flowfile.flow_data_engine.flow_data_engine.FlowDataEngine
    options:
      show_root_heading: false
      show_source: false
      members:
        - solve_graph

### 🛠️ Utility & Testing
::: flowfile_core.flowfile.flow_data_engine.flow_data_engine.FlowDataEngine
    options:
      show_root_heading: false
      show_source: false
      members:
        - get_schema_column
        - get_estimated_file_size
        - assert_equal
        - set_streamable

---
## External Functions

### ::: flowfile_core.flowfile.flow_data_engine.flow_data_engine.execute_polars_code
    options:
      show_root_heading: true
      show_source: false
      heading_level: 3