# Auto-generated stub for flowfile_frame.enums — do not edit.
# Run `make stubs` to regenerate from the Python source.
from __future__ import annotations

from enum import Enum
from typing import Literal, TypeAlias
from flowfile_core.flowfile import param_types
from flowfile_core.schemas import transform_schema

GateOperatorLiteral: TypeAlias = transform_schema.GateOperator
ParamTypeLiteral: TypeAlias = param_types.ParamType
NodeType: TypeAlias = Literal['manual_input', 'filter', 'formula', 'multi_field_formula', 'dynamic_rename', 'data_cleansing', 'select', 'sort', 'record_id', 'sample', 'random_split', 'unique', 'group_by', 'window_functions', 'pivot', 'unpivot', 'text_to_rows', 'graph_solver', 'python_script', 'polars_code', 'sql_query', 'join', 'cross_join', 'fuzzy_match', 'record_count', 'explore_data', 'union', 'gate', 'output', 'api_response', 'read', 'list_files', 'database_reader', 'database_writer', 'cloud_storage_reader', 'cloud_storage_writer', 'catalog_reader', 'catalog_writer', 'kafka_source', 'google_analytics_reader', 'rest_api_reader', 'external_source', 'train_model', 'apply_model', 'evaluate_model', 'wait_for', 'flow_input', 'flow_output', 'run_flow']

class GateOperator(str, Enum):
    EQUALS = 'equals'
    NOT_EQUALS = 'not_equals'
    IN = 'in'
    NOT_IN = 'not_in'
    IS_TRUE = 'is_true'
    IS_FALSE = 'is_false'
    IS_SET = 'is_set'

class ParamType(str, Enum):
    STRING = 'string'
    INTEGER = 'integer'
    FLOAT = 'float'
    BOOLEAN = 'boolean'
    ENUM = 'enum'

class NodeTypes(str, Enum):
    MANUAL_INPUT = 'manual_input'
    FILTER = 'filter'
    FORMULA = 'formula'
    MULTI_FIELD_FORMULA = 'multi_field_formula'
    DYNAMIC_RENAME = 'dynamic_rename'
    DATA_CLEANSING = 'data_cleansing'
    SELECT = 'select'
    SORT = 'sort'
    RECORD_ID = 'record_id'
    SAMPLE = 'sample'
    RANDOM_SPLIT = 'random_split'
    UNIQUE = 'unique'
    GROUP_BY = 'group_by'
    WINDOW_FUNCTIONS = 'window_functions'
    PIVOT = 'pivot'
    UNPIVOT = 'unpivot'
    TEXT_TO_ROWS = 'text_to_rows'
    GRAPH_SOLVER = 'graph_solver'
    PYTHON_SCRIPT = 'python_script'
    POLARS_CODE = 'polars_code'
    SQL_QUERY = 'sql_query'
    JOIN = 'join'
    CROSS_JOIN = 'cross_join'
    FUZZY_MATCH = 'fuzzy_match'
    RECORD_COUNT = 'record_count'
    EXPLORE_DATA = 'explore_data'
    UNION = 'union'
    GATE = 'gate'
    OUTPUT = 'output'
    API_RESPONSE = 'api_response'
    READ = 'read'
    LIST_FILES = 'list_files'
    DATABASE_READER = 'database_reader'
    DATABASE_WRITER = 'database_writer'
    CLOUD_STORAGE_READER = 'cloud_storage_reader'
    CLOUD_STORAGE_WRITER = 'cloud_storage_writer'
    CATALOG_READER = 'catalog_reader'
    CATALOG_WRITER = 'catalog_writer'
    KAFKA_SOURCE = 'kafka_source'
    GOOGLE_ANALYTICS_READER = 'google_analytics_reader'
    REST_API_READER = 'rest_api_reader'
    EXTERNAL_SOURCE = 'external_source'
    TRAIN_MODEL = 'train_model'
    APPLY_MODEL = 'apply_model'
    EVALUATE_MODEL = 'evaluate_model'
    WAIT_FOR = 'wait_for'
    FLOW_INPUT = 'flow_input'
    FLOW_OUTPUT = 'flow_output'
    RUN_FLOW = 'run_flow'

