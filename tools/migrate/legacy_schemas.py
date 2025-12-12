"""
Legacy schema definitions for loading old flowfile pickles.

These dataclass definitions mirror the OLD schema structure from transform_schema.py
BEFORE the migration to Pydantic BaseModel. They are ONLY used by the migration tool
to deserialize old pickle files.

NOTE: Only transform_schema.py had dataclasses. Everything in schemas.py and
input_schema.py was already Pydantic BaseModel and loads normally.

DO NOT USE THESE IN PRODUCTION CODE.
"""

from dataclasses import dataclass, field
from typing import List, Optional, Set, Any, Literal


# =============================================================================
# TRANSFORM SCHEMAS (flowfile_core/schemas/transform_schema.py)
# These are the ONLY classes that changed from @dataclass to BaseModel
# =============================================================================

@dataclass
class SelectInput:
    """Defines how a single column should be selected, renamed, or type-cast."""
    old_name: str
    original_position: Optional[int] = None
    new_name: Optional[str] = None
    data_type: Optional[str] = None
    data_type_change: Optional[bool] = False
    join_key: Optional[bool] = False
    is_altered: Optional[bool] = False
    position: Optional[int] = None
    is_available: Optional[bool] = True
    keep: Optional[bool] = True

    def __post_init__(self):
        if self.new_name is None:
            self.new_name = self.old_name


@dataclass
class FieldInput:
    """Represents a single field with its name and data type."""
    name: str
    data_type: Optional[str] = None


@dataclass
class FunctionInput:
    """Defines a formula to be applied, including the output field information."""
    field: FieldInput = None
    function: str = ''


@dataclass
class BasicFilter:
    """Defines a simple, single-condition filter."""
    field: str = ''
    filter_type: str = ''
    filter_value: str = ''


@dataclass
class FilterInput:
    """Defines the settings for a filter operation."""
    advanced_filter: str = ''
    basic_filter: BasicFilter = None
    filter_type: str = 'basic'


@dataclass
class SelectInputs:
    """A container for a list of SelectInput objects."""
    renames: List[SelectInput] = field(default_factory=list)

    @property
    def old_cols(self) -> Set:
        return set(v.old_name for v in self.renames if v.keep)

    @property
    def new_cols(self) -> Set:
        return set(v.new_name for v in self.renames if v.keep)


@dataclass
class JoinInputs:
    """Extends SelectInputs with functionality specific to join operations."""
    renames: List[SelectInput] = field(default_factory=list)


@dataclass
class JoinMap:
    """Defines a single mapping between a left and right column for a join key."""
    left_col: str = None
    right_col: str = None


@dataclass
class CrossJoinInput:
    """Defines the settings for a cross join operation."""
    left_select: Any = None  # SelectInputs or JoinInputs
    right_select: Any = None


@dataclass
class JoinInput:
    """Defines the settings for a standard SQL-style join."""
    join_mapping: List[JoinMap] = field(default_factory=list)
    left_select: Any = None  # JoinInputs
    right_select: Any = None  # JoinInputs
    how: str = 'inner'


@dataclass
class FuzzyMapping:
    """Defines a fuzzy match column mapping with threshold."""
    left_col: str = None
    right_col: str = None
    threshold_score: int = 80
    fuzzy_type: str = 'levenshtein'


@dataclass
class FuzzyMatchInput:
    """Extends JoinInput with settings specific to fuzzy matching."""
    join_mapping: List[FuzzyMapping] = field(default_factory=list)
    left_select: Any = None
    right_select: Any = None
    how: str = 'inner'
    aggregate_output: bool = False


@dataclass
class AggColl:
    """Represents a single aggregation operation for a group by operation."""
    old_name: str = None
    agg: str = None
    new_name: Optional[str] = None
    output_type: Optional[str] = None


@dataclass
class GroupByInput:
    """Represents the input for a group by operation."""
    agg_cols: List[AggColl] = field(default_factory=list)


@dataclass
class PivotInput:
    """Defines the settings for a pivot (long-to-wide) operation."""
    index_columns: List[str] = field(default_factory=list)
    pivot_column: str = None
    value_col: str = None
    aggregations: List[str] = field(default_factory=list)


@dataclass
class SortByInput:
    """Defines a single sort condition on a column."""
    column: str = None
    how: str = 'asc'


@dataclass
class RecordIdInput:
    """Defines settings for adding a record ID column."""
    output_column_name: str = 'record_id'
    offset: int = 1
    group_by: Optional[bool] = False
    group_by_columns: Optional[List[str]] = field(default_factory=list)


@dataclass
class TextToRowsInput:
    """Defines settings for splitting a text column into multiple rows."""
    column_to_split: str = None
    output_column_name: Optional[str] = None
    split_by_fixed_value: Optional[bool] = True
    split_fixed_value: Optional[str] = ','
    split_by_column: Optional[str] = None


@dataclass
class UnpivotInput:
    """Defines settings for an unpivot (wide-to-long) operation."""
    index_columns: Optional[List[str]] = field(default_factory=list)
    value_columns: Optional[List[str]] = field(default_factory=list)
    data_type_selector: Optional[Literal['float', 'all', 'date', 'numeric', 'string']] = None
    data_type_selector_mode: Optional[Literal['data_type', 'column']] = 'column'

    def __post_init__(self):
        if self.index_columns is None:
            self.index_columns = []
        if self.value_columns is None:
            self.value_columns = []


@dataclass
class UnionInput:
    """Defines settings for a union (concatenation) operation."""
    mode: Literal['selective', 'relaxed'] = 'relaxed'


@dataclass
class UniqueInput:
    """Defines settings for a uniqueness operation."""
    columns: Optional[List[str]] = None
    strategy: str = "any"


@dataclass
class GraphSolverInput:
    """Defines settings for a graph-solving operation."""
    col_from: str = None
    col_to: str = None
    output_column_name: Optional[str] = 'graph_group'


@dataclass
class PolarsCodeInput:
    """A simple container for user-provided Polars code."""
    polars_code: str = ''


# =============================================================================
# CLASS NAME MAPPING for pickle.Unpickler.find_class
# ONLY includes transform_schema.py classes that changed from dataclass to BaseModel
# =============================================================================

LEGACY_CLASS_MAP = {
    'SelectInput': SelectInput,
    'FieldInput': FieldInput,
    'FunctionInput': FunctionInput,
    'BasicFilter': BasicFilter,
    'FilterInput': FilterInput,
    'SelectInputs': SelectInputs,
    'JoinInputs': JoinInputs,
    'JoinMap': JoinMap,
    'CrossJoinInput': CrossJoinInput,
    'JoinInput': JoinInput,
    'FuzzyMapping': FuzzyMapping,
    'FuzzyMatchInput': FuzzyMatchInput,
    'AggColl': AggColl,
    'GroupByInput': GroupByInput,
    'PivotInput': PivotInput,
    'SortByInput': SortByInput,
    'RecordIdInput': RecordIdInput,
    'TextToRowsInput': TextToRowsInput,
    'UnpivotInput': UnpivotInput,
    'UnionInput': UnionInput,
    'UniqueInput': UniqueInput,
    'GraphSolverInput': GraphSolverInput,
    'PolarsCodeInput': PolarsCodeInput,
}