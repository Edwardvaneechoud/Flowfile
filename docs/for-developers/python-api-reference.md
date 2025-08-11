# Flowfile Core API Reference

This section provides a detailed API reference for the core Python objects, data models, and API routes in `flowfile-core`. The documentation is generated directly from the source code docstrings.


## 🏛️ Core Components

This section covers the fundamental classes that manage the state and execution of data pipelines. These are the main "verbs" of the library.


### ::: flowfile_core.main

    options:
      show_root_heading: true
      show_source: true
      heading_level: 3


### ::: flowfile_core.flowfile.flow_graph.FlowGraph

    options:
      show_root_heading: true
      show_source: true
      heading_level: 3


### ::: flowfile_core.flowfile.flow_node.flow_node.FlowNode

    options:
      show_root_heading: true
      show_source: true
      heading_level: 3


## 🚀 The FlowDataEngine

The `FlowDataEngine` is the primary workhorse of the library, providing a rich API for data manipulation, I/O, and transformation. Its methods are grouped below by functionality.


### ::: flowfile_core.flowfile.flow_data_engine.flow_data_engine.FlowDataEngine

    options:
      show_root_heading: true
      show_source: true
      heading_level: 3
      members:
        - __init__


#### Core Properties & State

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


#### Creation Methods

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


#### Data I/O and Conversion

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


#### Data Sampling & Access

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


#### Data Transformations

::: flowfile_core.flowfile.flow_data_engine.flow_data_engine.FlowDataEngine

options:

show_root_heading: false

show_source: false

heading_level: 4

members:

- do_group_by

- do_pivot

- unpivot

- do_sort

- split

- make_unique

- concat

- join

- do_cross_join

- do_fuzzy_join

- fuzzy_match

- start_fuzzy_join

- do_filter

- select_columns

- do_select

- drop_columns

- reorganize_order

- apply_flowfile_formula

- apply_sql_formula

- add_record_id

- change_column_types

- add_new_values


#### Graph Operations

::: flowfile_core.flowfile.flow_data_engine.flow_data_engine.FlowDataEngine

options:

show_root_heading: false

show_source: false

members:

- solve_graph


#### Utility & Testing

::: flowfile_core.flowfile.flow_data_engine.flow_data_engine.FlowDataEngine

options:

show_root_heading: false

show_source: false

members:

- get_schema_column

- get_estimated_file_size

- assert_equal

- set_streamable


## 🧱 Data Modeling (Schemas)

This section documents the Pydantic models that define the structure of settings and data. These models are the "nouns" of the system—they represent the configuration objects that are passed to `FlowDataEngine` methods and define the structure of nodes in the `FlowGraph`.


### ::: flowfile_core.schemas.schemas

### ::: flowfile_core.schemas.input_schema

### ::: flowfile_core.schemas.transform_schema

### ::: flowfile_core.schemas.cloud_storage_schemas

### ::: flowfile_core.schemas.output_model

    options:
      show_root_heading: true
      show_source: true
      heading_level: 3


## 🌐 Web API

This section documents the FastAPI routes that expose the functionality of `flowfile-core` over HTTP.


### ::: flowfile_core.routes.routes

### ::: flowfile_core.routes.auth

### ::: flowfile_core.routes.cloud_connections

### ::: flowfile_core.routes.logs

### ::: flowfile_core.routes.public

### ::: flowfile_core.routes.secrets

    options:
      show_root_heading: true
      show_source: true
      heading_level: 3


## 🛠️ Utility Functions

This section covers standalone utility functions that can be useful for advanced operations.


### ::: flowfile_core.flowfile.flow_data_engine.flow_data_engine.execute_polars_code

    options:
      show_root_heading: true
      show_source: true
      heading_level: 3
