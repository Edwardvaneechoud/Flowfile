from typing import List, Dict, Tuple, Set, Optional, Literal, Callable
from dataclasses import asdict
import polars as pl
from polars import selectors
from copy import deepcopy
from pydantic import BaseModel, ConfigDict, model_validator, Field
from typing import NamedTuple, Union, Any
from flowfile_core.schemas.yaml_types import (
    SelectInputYaml, JoinInputsYaml, JoinInputYaml,
    CrossJoinInputYaml, FuzzyMatchInputYaml
)
from pl_fuzzy_frame_match.models import FuzzyMapping

from flowfile_core.types import DataType, DataTypeStr

FuzzyMap = FuzzyMapping

AUTO_DATA_TYPE = "Auto"


def get_func_type_mapping(func: str):
    """Infers the output data type of common aggregation functions."""
    if func in ["mean", "avg", "median", "std", "var"]:
        return "Float64"
    elif func in ['min', 'max', 'first', 'last', "cumsum", "sum"]:
        return None
    elif func in ['count', 'n_unique']:
        return "Int64"
    elif func in ['concat']:
        return "Utf8"


def string_concat(*column: str):
    """A simple wrapper to concatenate string columns in Polars."""
    return pl.col(column).cast(pl.Utf8).str.concat(delimiter=',')


SideLit = Literal["left", "right"]
JoinStrategy = Literal['inner', 'left', 'right', 'full', 'semi', 'anti', 'cross', 'outer']
FuzzyTypeLiteral = Literal['levenshtein', 'jaro', 'jaro_winkler', 'hamming', 'damerau_levenshtein', 'indel']


def construct_join_key_name(side: SideLit, column_name: str) -> str:
    """Creates a temporary, unique name for a join key column."""
    return "_FLOWFILE_JOIN_KEY_" + side.upper() + "_" + column_name


class JoinKeyRename(NamedTuple):
    """Represents the renaming of a join key from its original to a temporary name."""
    original_name: str
    temp_name: str


class JoinKeyRenameResponse(NamedTuple):
    """Contains a list of join key renames for one side of a join."""
    side: SideLit
    join_key_renames: List[JoinKeyRename]


class FullJoinKeyResponse(NamedTuple):
    """Holds the join key rename responses for both sides of a join."""
    left: JoinKeyRenameResponse
    right: JoinKeyRenameResponse


class SelectInput(BaseModel):
    """Defines how a single column should be selected, renamed, or type-cast.

    This is a core building block for any operation that involves column manipulation.
    It holds all the configuration for a single field in a selection operation.
    """
    model_config = ConfigDict(frozen=False)

    old_name: str
    original_position: Optional[int] = None
    new_name: Optional[str] = None
    data_type: Optional[str] = None
    data_type_change: bool = False
    join_key: bool = False
    is_altered: bool = False
    position: Optional[int] = None
    is_available: bool = True
    keep: bool = True

    def __init__(self, old_name: str = None, new_name: str = None, **data):
        if old_name is not None:
            data['old_name'] = old_name
        if new_name is not None:
            data['new_name'] = new_name
        super().__init__(**data)

    def to_yaml_dict(self) -> SelectInputYaml:
        """Serialize for YAML output - only user-relevant fields."""
        result: SelectInputYaml = {"old_name": self.old_name}
        if self.new_name != self.old_name:
            result["new_name"] = self.new_name
        if not self.keep:
            result["keep"] = self.keep
        if self.data_type_change and self.data_type:
            result["data_type"] = self.data_type
        return result

    @classmethod
    def from_yaml_dict(cls, data: dict) -> "SelectInput":
        """Load from slim YAML format."""
        old_name = data["old_name"]
        new_name = data.get("new_name", old_name)
        return cls(
            old_name=old_name,
            new_name=new_name,
            keep=data.get("keep", True),
            data_type=data.get("data_type"),
            data_type_change=data.get("data_type") is not None,
            is_altered=old_name != new_name,
        )

    @model_validator(mode='after')
    def set_default_new_name(self):
        """If new_name is None, default it to old_name."""
        if self.new_name is None:
            self.new_name = self.old_name
        if self.old_name != self.new_name:
            self.is_altered = True
        return self

    def __hash__(self):
        """Allow SelectInput to be used in sets and as dict keys."""
        return hash(self.old_name)

    def __eq__(self, other):
        """Required when implementing __hash__."""
        if not isinstance(other, SelectInput):
            return False
        return self.old_name == other.old_name

    @property
    def polars_type(self) -> str:
        """Translates a user-friendly type name to a Polars data type string."""
        data_type_lower = self.data_type.lower()
        if data_type_lower == 'string':
            return 'Utf8'
        elif data_type_lower == 'integer':
            return 'Int64'
        elif data_type_lower == 'double':
            return 'Float64'
        return self.data_type


class FieldInput(BaseModel):
    """Represents a single field with its name and data type, typically for defining an output column."""
    name: str
    data_type: DataType | Literal["Auto"] | DataTypeStr | None = AUTO_DATA_TYPE


class FunctionInput(BaseModel):
    """Defines a formula to be applied, including the output field information."""
    field: FieldInput
    function: str

    def __init__(self, field: FieldInput = None, function: str = None, **data):
        if field is not None:
            data['field'] = field
        if function is not None:
            data['function'] = function
        super().__init__(**data)


class BasicFilter(BaseModel):
    """Defines a simple, single-condition filter (e.g., 'column' 'equals' 'value')."""
    field: Optional[str] = ''
    filter_type: Optional[str] = ''
    filter_value: Optional[str] = ''

    def __init__(self, field: str = None, filter_type: str = None, filter_value: str = None, **data):
        if field is not None:
            data['field'] = field
        if filter_type is not None:
            data['filter_type'] = filter_type
        if filter_value is not None:
            data['filter_value'] = filter_value
        super().__init__(**data)


class FilterInput(BaseModel):
    """Defines the settings for a filter operation, supporting basic or advanced (expression-based) modes."""
    advanced_filter: Optional[str] = ''
    basic_filter: Optional[BasicFilter] = None
    filter_type: Optional[str] = 'basic'

    def __init__(self, advanced_filter: str = None, basic_filter: BasicFilter = None,
                 filter_type: str = None, **data):
        if advanced_filter is not None:
            data['advanced_filter'] = advanced_filter
        if basic_filter is not None:
            data['basic_filter'] = basic_filter
        if filter_type is not None:
            data['filter_type'] = filter_type
        super().__init__(**data)


class SelectInputs(BaseModel):
    """A container for a list of `SelectInput` objects (pure data, no logic)."""
    renames: List[SelectInput] = Field(default_factory=list)

    def __init__(self, renames: List[SelectInput] = None, **kwargs):
        if renames is not None:
            kwargs['renames'] = renames
        else:
            kwargs['renames'] = []
        super().__init__(**kwargs)

    def to_yaml_dict(self) -> JoinInputsYaml:
        """Serialize for YAML output."""
        return {"select": [r.to_yaml_dict() for r in self.renames]}

    @classmethod
    def from_yaml_dict(cls, data: dict) -> "SelectInputs":
        """Load from slim YAML format. Supports both 'select' (new) and 'renames' (internal)."""
        items = data.get("select", data.get("renames", []))
        return cls(renames=[SelectInput.from_yaml_dict(item) for item in items])

    @classmethod
    def create_from_list(cls, col_list: List[str]) -> "SelectInputs":
        """Creates a SelectInputs object from a simple list of column names."""
        return cls(renames=[SelectInput(old_name=c) for c in col_list])

    @classmethod
    def create_from_pl_df(cls, df: pl.DataFrame | pl.LazyFrame) -> "SelectInputs":
        """Creates a SelectInputs object from a Polars DataFrame's columns."""
        return cls(renames=[SelectInput(old_name=c) for c in df.columns])


class JoinInputs(SelectInputs):
    """Data model for join-specific select inputs (extends SelectInputs)."""

    def __init__(self, renames: List[SelectInput] = None, **kwargs):
        if renames is not None:
            kwargs['renames'] = renames
        else:
            kwargs['renames'] = []
        super().__init__(**kwargs)


class JoinMap(BaseModel):
    """Defines a single mapping between a left and right column for a join key."""
    left_col: Optional[str] = None
    right_col: Optional[str] = None

    def __init__(self, left_col: str = None, right_col: str = None, **data):
        if left_col is not None:
            data['left_col'] = left_col
        if right_col is not None:
            data['right_col'] = right_col
        super().__init__(**data)

    @model_validator(mode='after')
    def set_default_right_col(self):
        """If right_col is None, default it to left_col."""
        if self.right_col is None:
            self.right_col = self.left_col
        return self


class CrossJoinInput(BaseModel):
    """Data model for cross join operations."""
    left_select: JoinInputs
    right_select: JoinInputs

    @model_validator(mode='before')
    @classmethod
    def parse_inputs(cls, data: Any) -> Any:
        """Parse flexible input formats before validation."""
        if isinstance(data, dict):
            # Parse join_mapping
            if 'join_mapping' in data:
                data['join_mapping'] = cls._parse_join_mapping(data['join_mapping'])

            # Parse left_select
            if 'left_select' in data:
                data['left_select'] = cls._parse_select(data['left_select'])

            # Parse right_select
            if 'right_select' in data:
                data['right_select'] = cls._parse_select(data['right_select'])

        return data

    @staticmethod
    def _parse_join_mapping(join_mapping: Any) -> List[JoinMap]:
        """Parse various join_mapping formats."""
        # Already a list of JoinMaps
        if isinstance(join_mapping, list):
            result = []
            for jm in join_mapping:
                if isinstance(jm, JoinMap):
                    result.append(jm)
                elif isinstance(jm, dict):
                    result.append(JoinMap(**jm))
                elif isinstance(jm, (tuple, list)) and len(jm) == 2:
                    result.append(JoinMap(left_col=jm[0], right_col=jm[1]))
                elif isinstance(jm, str):
                    result.append(JoinMap(left_col=jm, right_col=jm))
                else:
                    raise ValueError(f"Invalid join mapping item: {jm}")
            return result

        # Single JoinMap
        if isinstance(join_mapping, JoinMap):
            return [join_mapping]

        # String: same column on both sides
        if isinstance(join_mapping, str):
            return [JoinMap(left_col=join_mapping, right_col=join_mapping)]

        # Tuple: (left, right)
        if isinstance(join_mapping, tuple) and len(join_mapping) == 2:
            return [JoinMap(left_col=join_mapping[0], right_col=join_mapping[1])]

        raise ValueError(f"Invalid join_mapping format: {type(join_mapping)}")

    @staticmethod
    def _parse_select(select: Any) -> JoinInputs:
        """Parse various select input formats."""
        # Already JoinInputs
        if isinstance(select, JoinInputs):
            return select

        # List of SelectInput objects
        if isinstance(select, list):
            if all(isinstance(s, SelectInput) for s in select):
                return JoinInputs(renames=select)
            elif all(isinstance(s, str) for s in select):
                return JoinInputs(renames=[SelectInput(old_name=s) for s in select])
            elif all(isinstance(s, dict) for s in select):
                return JoinInputs(renames=[SelectInput(**s) for s in select])

        # Dict with 'select' (new YAML) or 'renames' (internal) key
        if isinstance(select, dict):
            if 'select' in select:
                return JoinInputs(renames=[SelectInput.from_yaml_dict(s) for s in select['select']])
            if 'renames' in select:
                return JoinInputs(**select)

        raise ValueError(f"Invalid select format: {type(select)}")

    def __init__(self,
                 left_select: Union[JoinInputs, List[SelectInput], List[str]] = None,
                 right_select: Union[JoinInputs, List[SelectInput], List[str]] = None,
                 **data):
        """Custom init for backward compatibility with positional arguments."""
        if left_select is not None:
            data['left_select'] = left_select
        if right_select is not None:
            data['right_select'] = right_select
        super().__init__(**data)

    def to_yaml_dict(self) -> CrossJoinInputYaml:
        """Serialize for YAML output."""
        return {
            "left_select": self.left_select.to_yaml_dict(),
            "right_select": self.right_select.to_yaml_dict(),
        }


class JoinInput(BaseModel):
    """Data model for standard SQL-style join operations."""
    join_mapping: List[JoinMap]
    left_select: JoinInputs
    right_select: JoinInputs
    how: JoinStrategy = 'inner'

    @model_validator(mode='before')
    @classmethod
    def parse_inputs(cls, data: Any) -> Any:
        """Parse flexible input formats before validation."""
        if isinstance(data, dict):
            # Parse join_mapping
            if 'join_mapping' in data:
                data['join_mapping'] = cls._parse_join_mapping(data['join_mapping'])

            # Parse left_select
            if 'left_select' in data:
                data['left_select'] = cls._parse_select(data['left_select'])

            # Parse right_select
            if 'right_select' in data:
                data['right_select'] = cls._parse_select(data['right_select'])

        return data

    @staticmethod
    def _parse_join_mapping(join_mapping: Any) -> List[JoinMap]:
        """Parse various join_mapping formats."""
        # Already a list of JoinMaps
        if isinstance(join_mapping, list):
            result = []
            for jm in join_mapping:
                if isinstance(jm, JoinMap):
                    result.append(jm)
                elif isinstance(jm, dict):
                    result.append(JoinMap(**jm))
                elif isinstance(jm, (tuple, list)) and len(jm) == 2:
                    result.append(JoinMap(left_col=jm[0], right_col=jm[1]))
                elif isinstance(jm, str):
                    result.append(JoinMap(left_col=jm, right_col=jm))
                else:
                    raise ValueError(f"Invalid join mapping item: {jm}")
            return result

        # Single JoinMap
        if isinstance(join_mapping, JoinMap):
            return [join_mapping]

        # String: same column on both sides
        if isinstance(join_mapping, str):
            return [JoinMap(left_col=join_mapping, right_col=join_mapping)]

        # Tuple: (left, right)
        if isinstance(join_mapping, tuple) and len(join_mapping) == 2:
            return [JoinMap(left_col=join_mapping[0], right_col=join_mapping[1])]

        raise ValueError(f"Invalid join_mapping format: {type(join_mapping)}")

    @staticmethod
    def _parse_select(select: Any) -> JoinInputs:
        """Parse various select input formats."""
        # Already JoinInputs
        if isinstance(select, JoinInputs):
            return select

        # List of SelectInput objects
        if isinstance(select, list):
            if all(isinstance(s, SelectInput) for s in select):
                return JoinInputs(renames=select)
            elif all(isinstance(s, str) for s in select):
                return JoinInputs(renames=[SelectInput(old_name=s) for s in select])
            elif all(isinstance(s, dict) for s in select):
                return JoinInputs(renames=[SelectInput(**s) for s in select])

        # Dict with 'select' (new YAML) or 'renames' (internal) key
        if isinstance(select, dict):
            if 'select' in select:
                return JoinInputs(renames=[SelectInput.from_yaml_dict(s) for s in select['select']])
            if 'renames' in select:
                return JoinInputs(**select)

        raise ValueError(f"Invalid select format: {type(select)}")

    def __init__(self,
                 join_mapping: Union[List[JoinMap], JoinMap, Tuple[str, str], str, List[Tuple], List[str]] = None,
                 left_select: Union[JoinInputs, List[SelectInput], List[str]] = None,
                 right_select: Union[JoinInputs, List[SelectInput], List[str]] = None,
                 how: JoinStrategy = 'inner',
                 **data):
        """Custom init for backward compatibility with positional arguments."""
        if join_mapping is not None:
            data['join_mapping'] = join_mapping
        if left_select is not None:
            data['left_select'] = left_select
        if right_select is not None:
            data['right_select'] = right_select
        if how is not None:
            data['how'] = how

        super().__init__(**data)

    def to_yaml_dict(self) -> JoinInputYaml:
        """Serialize for YAML output."""
        return {
            "join_mapping": [{"left_col": jm.left_col, "right_col": jm.right_col} for jm in self.join_mapping],
            "left_select": self.left_select.to_yaml_dict(),
            "right_select": self.right_select.to_yaml_dict(),
            "how": self.how,
        }


class FuzzyMatchInput(BaseModel):
    """Data model for fuzzy matching join operations."""
    join_mapping: List[FuzzyMapping]
    left_select: JoinInputs
    right_select: JoinInputs
    how: JoinStrategy = 'inner'
    aggregate_output: bool = False

    def __init__(self,
                 left_select: Union[JoinInputs, List[SelectInput], List[str]] = None,
                 right_select: Union[JoinInputs, List[SelectInput], List[str]] = None,
                 **data):
        """Custom init for backward compatibility with positional arguments."""
        if left_select is not None:
            data['left_select'] = left_select
        if right_select is not None:
            data['right_select'] = right_select

        super().__init__(**data)

    def to_yaml_dict(self) -> FuzzyMatchInputYaml:
        """Serialize for YAML output."""
        return {
            "join_mapping": [asdict(jm) for jm in self.join_mapping],
            "left_select": self.left_select.to_yaml_dict(),
            "right_select": self.right_select.to_yaml_dict(),
            "how": self.how,
            "aggregate_output": self.aggregate_output,
        }

    @staticmethod
    def _parse_select(select: Any) -> JoinInputs:
        """Parse various select input formats."""
        # Already JoinInputs
        if isinstance(select, JoinInputs):
            return select

        # List of SelectInput objects
        if isinstance(select, list):
            if all(isinstance(s, SelectInput) for s in select):
                return JoinInputs(renames=select)
            elif all(isinstance(s, str) for s in select):
                return JoinInputs(renames=[SelectInput(old_name=s) for s in select])
            elif all(isinstance(s, dict) for s in select):
                return JoinInputs(renames=[SelectInput(**s) for s in select])

        # Dict with 'select' (new YAML) or 'renames' (internal) key
        if isinstance(select, dict):
            if 'select' in select:
                return JoinInputs(renames=[SelectInput.from_yaml_dict(s) for s in select['select']])
            if 'renames' in select:
                return JoinInputs(**select)

        raise ValueError(f"Invalid select format: {type(select)}")

    @model_validator(mode='before')
    @classmethod
    def parse_inputs(cls, data: Any) -> Any:
        """Parse flexible input formats before validation."""
        if isinstance(data, dict):
            # Parse left_select
            if 'left_select' in data:
                data['left_select'] = cls._parse_select(data['left_select'])

            # Parse right_select
            if 'right_select' in data:
                data['right_select'] = cls._parse_select(data['right_select'])

        return data


class AggColl(BaseModel):
    """
    A data class that represents a single aggregation operation for a group by operation.

    Attributes
    ----------
    old_name : str
        The name of the column in the original DataFrame to be aggregated.

    agg : str
        The aggregation function to use. This can be a string representing a built-in function or a custom function.

    new_name : Optional[str]
        The name of the resulting aggregated column in the output DataFrame. If not provided, it will default to the
        old_name appended with the aggregation function.

    output_type : Optional[str]
        The type of the output values of the aggregation. If not provided, it is inferred from the aggregation function
        using the `get_func_type_mapping` function.

    Example
    --------
    agg_col = AggColl(
        old_name='col1',
        agg='sum',
        new_name='sum_col1',
        output_type='float'
    )
    """
    old_name: str
    agg: str
    new_name: Optional[str] = None
    output_type: Optional[str] = None

    def __init__(self, old_name: str, agg: str, new_name: Optional[str] = None, output_type: Optional[str] = None):
        data = {'old_name': old_name, 'agg': agg}
        if new_name is not None:
            data['new_name'] = new_name
        if output_type is not None:
            data['output_type'] = output_type

        super().__init__(**data)

    @model_validator(mode='after')
    def set_defaults(self):
        """Set default new_name and output_type based on agg function."""
        # Set new_name
        if self.new_name is None:
            if self.agg != 'groupby':
                self.new_name = self.old_name + "_" + self.agg
            else:
                self.new_name = self.old_name

        # Set output_type
        if self.output_type is None:
            self.output_type = get_func_type_mapping(self.agg)

        # Ensure old_name is a string
        self.old_name = str(self.old_name)

        return self

    @property
    def agg_func(self):
        """Returns the corresponding Polars aggregation function from the `agg` string."""
        if self.agg == 'groupby':
            return self.agg
        elif self.agg == 'concat':
            return string_concat
        else:
            return getattr(pl, self.agg) if isinstance(self.agg, str) else self.agg


class GroupByInput(BaseModel):
    """
    A data class that represents the input for a group by operation.

    Attributes
    ----------
    agg_cols : List[AggColl]
        A list of `AggColl` objects that specify the aggregation operations to perform on the DataFrame columns
        after grouping. Each `AggColl` object should specify the column to be aggregated and the aggregation
        function to use.

    Example
    --------
    group_by_input = GroupByInput(
        agg_cols=[AggColl(old_name='ix', agg='groupby'), AggColl(old_name='groups', agg='groupby'),
                  AggColl(old_name='col1', agg='sum'), AggColl(old_name='col2', agg='mean')]
    )
    """
    agg_cols: List[AggColl]

    def __init__(self, agg_cols: List[AggColl]):
        """Backwards compatibility implementation"""
        super().__init__(agg_cols=agg_cols)


class PivotInput(BaseModel):
    """Defines the settings for a pivot (long-to-wide) operation."""
    index_columns: List[str]
    pivot_column: str
    value_col: str
    aggregations: List[str]

    @property
    def grouped_columns(self) -> List[str]:
        """Returns the list of columns to be used for the initial grouping stage of the pivot."""
        return self.index_columns + [self.pivot_column]

    def get_group_by_input(self) -> GroupByInput:
        """Constructs the `GroupByInput` needed for the pre-aggregation step of the pivot."""
        group_by_cols = [AggColl(old_name=c, agg='groupby') for c in self.grouped_columns]
        agg_cols = [AggColl(old_name=self.value_col, agg=aggregation, new_name=aggregation)
                    for aggregation in self.aggregations]
        return GroupByInput(agg_cols=group_by_cols + agg_cols)

    def get_index_columns(self) -> List[pl.col]:
        """Returns the index columns as Polars column expressions."""
        return [pl.col(c) for c in self.index_columns]

    def get_pivot_column(self) -> pl.Expr:
        """Returns the pivot column as a Polars column expression."""
        return pl.col(self.pivot_column)

    def get_values_expr(self) -> pl.Expr:
        """Creates the struct expression used to gather the values for pivoting."""
        return pl.struct([pl.col(c) for c in self.aggregations]).alias('vals')


class SortByInput(BaseModel):
    """Defines a single sort condition on a column, including the direction."""
    column: str
    how: Optional[str] = 'asc'


class RecordIdInput(BaseModel):
    """Defines settings for adding a record ID (row number) column to the data."""
    output_column_name: str = 'record_id'
    offset: int = 1
    group_by: Optional[bool] = False
    group_by_columns: Optional[List[str]] = Field(default_factory=list)


class TextToRowsInput(BaseModel):
    """Defines settings for splitting a text column into multiple rows based on a delimiter."""
    column_to_split: str
    output_column_name: Optional[str] = None
    split_by_fixed_value: Optional[bool] = True
    split_fixed_value: Optional[str] = ','
    split_by_column: Optional[str] = None


class UnpivotInput(BaseModel):
    """Defines settings for an unpivot (wide-to-long) operation."""
    model_config = ConfigDict(arbitrary_types_allowed=True)

    index_columns: List[str] = Field(default_factory=list)
    value_columns: List[str] = Field(default_factory=list)
    data_type_selector: Optional[Literal['float', 'all', 'date', 'numeric', 'string']] = None
    data_type_selector_mode: Literal['data_type', 'column'] = 'column'

    @property
    def data_type_selector_expr(self) -> Optional[Callable]:
        """Returns a Polars selector function based on the `data_type_selector` string."""
        if self.data_type_selector_mode == 'data_type':
            if self.data_type_selector is not None:
                try:
                    return getattr(selectors, self.data_type_selector)
                except Exception as e:
                    print(f'Could not find the selector: {self.data_type_selector}')
                    return selectors.all
            return selectors.all
        return None


class UnionInput(BaseModel):
    """Defines settings for a union (concatenation) operation."""
    mode: Literal['selective', 'relaxed'] = 'relaxed'


class UniqueInput(BaseModel):
    """Defines settings for a uniqueness operation, specifying columns and which row to keep."""
    columns: Optional[List[str]] = None
    strategy: Literal["first", "last", "any", "none"] = "any"


class GraphSolverInput(BaseModel):
    """Defines settings for a graph-solving operation (e.g., finding connected components)."""
    col_from: str
    col_to: str
    output_column_name: Optional[str] = 'graph_group'


class PolarsCodeInput(BaseModel):
    """A simple container for a string of user-provided Polars code to be executed."""
    polars_code: str


class SelectInputsManager:
    """Manager class that provides all query and mutation operations."""

    def __init__(self, select_inputs: SelectInputs):
        self.select_inputs = select_inputs

    # === Query Methods (read-only) ===

    def get_old_cols(self) -> Set[str]:
        """Returns a set of original column names to be kept in the selection."""
        return set(v.old_name for v in self.select_inputs.renames if v.keep)

    def get_new_cols(self) -> Set[str]:
        """Returns a set of new (renamed) column names to be kept in the selection."""
        return set(v.new_name for v in self.select_inputs.renames if v.keep)

    def get_rename_table(self) -> dict[str, str]:
        """Generates a dictionary for use in Polars' `.rename()` method."""
        return {v.old_name: v.new_name for v in self.select_inputs.renames
                if v.is_available and (v.keep or v.join_key)}

    def get_select_cols(self, include_join_key: bool = True) -> List[str]:
        """Gets a list of original column names to select from the source DataFrame."""
        return [v.old_name for v in self.select_inputs.renames
                if v.keep or (v.join_key and include_join_key)]

    def has_drop_cols(self) -> bool:
        """Checks if any column is marked to be dropped from the selection."""
        return any(not v.keep for v in self.select_inputs.renames)

    def get_drop_columns(self) -> List[SelectInput]:
        """Returns a list of SelectInput objects that are marked to be dropped."""
        return [v for v in self.select_inputs.renames if not v.keep and v.is_available]

    def get_non_jk_drop_columns(self) -> List[SelectInput]:
        """Returns drop columns that are not join keys."""
        return [v for v in self.select_inputs.renames
                if not v.keep and v.is_available and not v.join_key]

    def find_by_old_name(self, old_name: str) -> Optional[SelectInput]:
        """Find SelectInput by original column name."""
        return next((v for v in self.select_inputs.renames if v.old_name == old_name), None)

    def find_by_new_name(self, new_name: str) -> Optional[SelectInput]:
        """Find SelectInput by new column name."""
        return next((v for v in self.select_inputs.renames if v.new_name == new_name), None)

    # === Mutation Methods ===

    def append(self, other: SelectInput) -> None:
        """Appends a new SelectInput to the list of renames."""
        self.select_inputs.renames.append(other)

    def remove_select_input(self, old_key: str) -> None:
        """Removes a SelectInput from the list based on its original name."""
        self.select_inputs.renames = [
            rename for rename in self.select_inputs.renames
            if rename.old_name != old_key
        ]

    def unselect_field(self, old_key: str) -> None:
        """Marks a field to be dropped from the final selection by setting `keep` to False."""
        for rename in self.select_inputs.renames:
            if old_key == rename.old_name:
                rename.keep = False

    # === Backward Compatibility Properties ===

    @property
    def old_cols(self) -> Set[str]:
        """Backward compatibility: Returns set of old column names."""
        return self.get_old_cols()

    @property
    def new_cols(self) -> Set[str]:
        """Backward compatibility: Returns set of new column names."""
        return self.get_new_cols()

    @property
    def rename_table(self) -> dict[str, str]:
        """Backward compatibility: Returns rename table dictionary."""
        return self.get_rename_table()

    @property
    def drop_columns(self) -> List[SelectInput]:
        """Backward compatibility: Returns list of columns to drop."""
        return self.get_drop_columns()

    @property
    def non_jk_drop_columns(self) -> List[SelectInput]:
        """Backward compatibility: Returns non-join-key columns to drop."""
        return self.get_non_jk_drop_columns()

    @property
    def renames(self) -> List[SelectInput]:
        """Backward compatibility: Direct access to renames list."""
        return self.select_inputs.renames

    def get_select_input_on_old_name(self, old_name: str) -> Optional[SelectInput]:
        """Backward compatibility alias: Find SelectInput by original column name."""
        return self.find_by_old_name(old_name)

    def get_select_input_on_new_name(self, new_name: str) -> Optional[SelectInput]:
        """Backward compatibility alias: Find SelectInput by new column name."""
        return self.find_by_new_name(new_name)

    def __add__(self, other: SelectInput) -> "SelectInputsManager":
        """Backward compatibility: Support += operator for appending."""
        self.append(other)
        return self


class JoinInputsManager(SelectInputsManager):
    """Manager for join-specific operations, extends SelectInputsManager."""

    def __init__(self, join_inputs: JoinInputs):
        super().__init__(join_inputs)
        self.join_inputs = join_inputs

    # === Query Methods ===

    def get_join_key_selects(self) -> List[SelectInput]:
        """Returns only the `SelectInput` objects that are marked as join keys."""
        return [v for v in self.join_inputs.renames if v.join_key]

    def get_join_key_renames(self, side: SideLit, filter_drop: bool = False) -> JoinKeyRenameResponse:
        """Gets the temporary rename mapping for all join keys on one side of a join."""
        join_key_selects = self.get_join_key_selects()
        join_key_list = [
            JoinKeyRename(jk.new_name, construct_join_key_name(side, jk.new_name))
            for jk in join_key_selects
            if jk.keep or not filter_drop
        ]
        return JoinKeyRenameResponse(side, join_key_list)

    def get_join_key_rename_mapping(self, side: SideLit) -> Dict[str, str]:
        """Returns a dictionary mapping original join key names to their temporary names."""
        join_key_response = self.get_join_key_renames(side)
        return {jkr.original_name: jkr.temp_name for jkr in join_key_response.join_key_renames}

    @property
    def join_key_selects(self) -> List[SelectInput]:
        """Backward compatibility: Returns join key SelectInputs."""
        return self.get_join_key_selects()


class JoinSelectManagerMixin:
    """Mixin providing common methods for join-like operations."""

    left_manager: JoinInputsManager
    right_manager: JoinInputsManager
    input: Union[CrossJoinInput, JoinInput, FuzzyMatchInput]

    @staticmethod
    def parse_select(select: Union[List[SelectInput], List[str], List[Dict], Dict]) -> JoinInputs:
        """Parses various input formats into a standardized `JoinInputs` object."""
        if not select:
            return JoinInputs(renames=[])

        if all(isinstance(c, SelectInput) for c in select):
            return JoinInputs(renames=select)
        elif all(isinstance(c, dict) for c in select):
            return JoinInputs(renames=[SelectInput(**c) for c in select])
        elif isinstance(select, dict):
            renames = select.get('renames')
            if renames:
                return JoinInputs(renames=[SelectInput(**c) for c in renames])
            return JoinInputs(renames=[])
        elif all(isinstance(c, str) for c in select):
            return JoinInputs(renames=[SelectInput(old_name=s, new_name=s) for s in select])

        raise ValueError(f"Unable to parse select input: {type(select)}")

    def get_overlapping_columns(self) -> Set[str]:
        """Finds column names that would conflict after the join."""
        return self.left_manager.get_new_cols() & self.right_manager.get_new_cols()

    def auto_generate_new_col_name(self, old_col_name: str, side: str) -> str:
        """Generates a new, non-conflicting column name by adding a suffix if necessary."""
        current_names = self.get_overlapping_columns()
        if old_col_name not in current_names:
            return old_col_name

        new_name = old_col_name
        while new_name in current_names:
            new_name = f'{side}_{new_name}'
        return new_name

    def add_new_select_column(self, select_input: SelectInput, side: str) -> None:
        """Adds a new column to the selection for either the left or right side."""
        target_input = self.input.right_select if side == 'right' else self.input.left_select

        select_input.new_name = self.auto_generate_new_col_name(
            select_input.old_name, side=side
        )

        target_input.renames.append(select_input)


class CrossJoinInputManager(JoinSelectManagerMixin):
    """Manager for cross join operations."""

    def __init__(self, cross_join_input: CrossJoinInput):
        self.input = deepcopy(cross_join_input)
        self.left_manager = JoinInputsManager(self.input.left_select)
        self.right_manager = JoinInputsManager(self.input.right_select)

    @classmethod
    def create(cls, left_select: Union[List[SelectInput], List[str]],
               right_select: Union[List[SelectInput], List[str]]) -> "CrossJoinInputManager":
        """Factory method to create CrossJoinInput from various input formats."""
        left_inputs = cls.parse_select(left_select)
        right_inputs = cls.parse_select(right_select)

        cross_join = CrossJoinInput(
            left_select=left_inputs,
            right_select=right_inputs
        )
        return cls(cross_join)

    def get_overlapping_records(self) -> Set[str]:
        """Finds column names that would conflict after the join."""
        return self.get_overlapping_columns()

    def auto_rename(self, rename_mode: Literal["suffix", "prefix"] = "prefix") -> None:
        """Automatically renames columns on the right side to prevent naming conflicts."""
        overlapping_records = self.get_overlapping_records()

        while len(overlapping_records) > 0:
            for right_col in self.input.right_select.renames:
                if right_col.new_name in overlapping_records:
                    if rename_mode == "prefix":
                        right_col.new_name = 'right_' + right_col.new_name
                    elif rename_mode == "suffix":
                        right_col.new_name = right_col.new_name + '_right'
                    else:
                        raise ValueError(f'Unknown rename_mode: {rename_mode}')
            overlapping_records = self.get_overlapping_records()

    # === Backward Compatibility Properties ===

    @property
    def left_select(self) -> JoinInputsManager:
        """Backward compatibility: Access left_manager as left_select."""
        return self.left_manager

    @property
    def right_select(self) -> JoinInputsManager:
        """Backward compatibility: Access right_manager as right_select."""
        return self.right_manager

    @property
    def overlapping_records(self) -> Set[str]:
        """Backward compatibility: Returns overlapping column names."""
        return self.get_overlapping_records()

    def to_cross_join_input(self) -> CrossJoinInput:
        """Creates a new CrossJoinInput instance based on the current manager settings.

        This is useful when you've modified the manager (e.g., via auto_rename) and
        want to get a fresh CrossJoinInput with all the current settings applied.

        Returns:
            A new CrossJoinInput instance with current settings
        """
        return CrossJoinInput(
            left_select=JoinInputs(renames=self.input.left_select.renames.copy()),
            right_select=JoinInputs(renames=self.input.right_select.renames.copy())
        )


class JoinInputManager(JoinSelectManagerMixin):
    """Manager for standard SQL-style join operations."""

    def __init__(self, join_input: JoinInput):
        self.input = deepcopy(join_input)
        self.left_manager = JoinInputsManager(self.input.left_select)
        self.right_manager = JoinInputsManager(self.input.right_select)
        self.set_join_keys()

    @classmethod
    def create(cls, join_mapping: Union[List[JoinMap], Tuple[str, str], str],
               left_select: Union[List[SelectInput], List[str]],
               right_select: Union[List[SelectInput], List[str]],
               how: JoinStrategy = 'inner') -> "JoinInputManager":
        """Factory method to create JoinInput from various input formats."""
        # Use JoinInput's own create method for parsing
        join_input = JoinInput(
            join_mapping=join_mapping,
            left_select=left_select,
            right_select=right_select,
            how=how
        )

        manager = cls(join_input)
        manager.set_join_keys()
        return manager

    def set_join_keys(self) -> None:
        """Marks the `SelectInput` objects corresponding to join keys."""
        left_join_keys = self._get_left_join_keys_set()
        right_join_keys = self._get_right_join_keys_set()

        for select_input in self.input.left_select.renames:
            select_input.join_key = select_input.old_name in left_join_keys

        for select_input in self.input.right_select.renames:
            select_input.join_key = select_input.old_name in right_join_keys

    def _get_left_join_keys_set(self) -> Set[str]:
        """Internal: Returns a set of the left-side join key column names."""
        return {jm.left_col for jm in self.input.join_mapping}

    def _get_right_join_keys_set(self) -> Set[str]:
        """Internal: Returns a set of the right-side join key column names."""
        return {jm.right_col for jm in self.input.join_mapping}

    def get_left_join_keys(self) -> Set[str]:
        """Returns a set of the left-side join key column names."""
        return self._get_left_join_keys_set()

    def get_right_join_keys(self) -> Set[str]:
        """Returns a set of the right-side join key column names."""
        return self._get_right_join_keys_set()

    def get_left_join_keys_list(self) -> List[str]:
        """Returns an ordered list of the left-side join key column names."""
        return [jm.left_col for jm in self.used_join_mapping]

    def get_right_join_keys_list(self) -> List[str]:
        """Returns an ordered list of the right-side join key column names."""
        return [jm.right_col for jm in self.used_join_mapping]

    def get_overlapping_records(self) -> Set[str]:
        """Finds column names that would conflict after the join."""
        return self.get_overlapping_columns()

    def auto_rename(self) -> None:
        """Automatically renames columns on the right side to prevent naming conflicts."""
        self.set_join_keys()
        overlapping_records = self.get_overlapping_records()

        while len(overlapping_records) > 0:
            for right_col in self.input.right_select.renames:
                if right_col.new_name in overlapping_records:
                    right_col.new_name = right_col.new_name + '_right'
            overlapping_records = self.get_overlapping_records()

    def get_join_key_renames(self, filter_drop: bool = False) -> FullJoinKeyResponse:
        """Gets the temporary rename mappings for the join keys on both sides."""
        left_renames = self.left_manager.get_join_key_renames(side="left", filter_drop=filter_drop)
        right_renames = self.right_manager.get_join_key_renames(side="right", filter_drop=filter_drop)
        return FullJoinKeyResponse(left_renames, right_renames)

    def get_names_for_table_rename(self) -> List[JoinMap]:
        """Gets join mapping with renamed columns applied."""
        new_mappings: List[JoinMap] = []
        left_rename_table = self.left_manager.get_rename_table()
        right_rename_table = self.right_manager.get_rename_table()

        for join_map in self.input.join_mapping:
            new_left = left_rename_table.get(join_map.left_col, join_map.left_col)
            new_right = right_rename_table.get(join_map.right_col, join_map.right_col)
            new_mappings.append(JoinMap(left_col=new_left, right_col=new_right))

        return new_mappings

    def get_used_join_mapping(self) -> List[JoinMap]:
        """Returns the final join mapping after applying all renames and transformations."""
        new_mappings: List[JoinMap] = []
        left_rename_table = self.left_manager.get_rename_table()
        right_rename_table = self.right_manager.get_rename_table()
        left_join_rename_mapping = self.left_manager.get_join_key_rename_mapping("left")
        right_join_rename_mapping = self.right_manager.get_join_key_rename_mapping("right")
        for join_map in self.input.join_mapping:
            left_col = left_rename_table.get(join_map.left_col, join_map.left_col)
            right_col = right_rename_table.get(join_map.right_col, join_map.left_col)

            final_left = left_join_rename_mapping.get(left_col, None)
            final_right = right_join_rename_mapping.get(right_col, None)

            new_mappings.append(JoinMap(left_col=final_left, right_col=final_right))

        return new_mappings

    def to_join_input(self) -> JoinInput:
        """Creates a new JoinInput instance based on the current manager settings.

        This is useful when you've modified the manager (e.g., via auto_rename) and
        want to get a fresh JoinInput with all the current settings applied.

        Returns:
            A new JoinInput instance with current settings
        """
        return JoinInput(
            join_mapping=self.input.join_mapping,
            left_select=JoinInputs(renames=self.input.left_select.renames.copy()),
            right_select=JoinInputs(renames=self.input.right_select.renames.copy()),
            how=self.input.how
        )

    @property
    def left_select(self) -> JoinInputsManager:
        """Backward compatibility: Access left_manager as left_select.

        This returns the MANAGER, not the data model.
        Usage: manager.left_select.join_key_selects
        """
        return self.left_manager

    @property
    def right_select(self) -> JoinInputsManager:
        """Backward compatibility: Access right_manager as right_select.

        This returns the MANAGER, not the data model.
        Usage: manager.right_select.join_key_selects
        """
        return self.right_manager

    @property
    def how(self) -> JoinStrategy:
        """Backward compatibility: Access join strategy."""
        return self.input.how

    @property
    def join_mapping(self) -> List[JoinMap]:
        """Backward compatibility: Access join mapping."""
        return self.input.join_mapping

    @property
    def overlapping_records(self) -> Set[str]:
        """Backward compatibility: Returns overlapping column names."""
        return self.get_overlapping_records()

    @property
    def used_join_mapping(self) -> List[JoinMap]:
        """Backward compatibility: Returns used join mapping.

        This property is critical - it's used by left_join_keys and right_join_keys.
        """
        return self.get_used_join_mapping()

    @property
    def left_join_keys(self) -> List[str]:
        """Backward compatibility: Returns left join keys list.

        IMPORTANT: Uses the used_join_mapping PROPERTY (not method).
        """
        return [jm.left_col for jm in self.used_join_mapping]

    @property
    def right_join_keys(self) -> List[str]:
        """Backward compatibility: Returns right join keys list.

        IMPORTANT: Uses the used_join_mapping PROPERTY (not method).
        """
        return [jm.right_col for jm in self.used_join_mapping]

    @property
    def _left_join_keys(self) -> Set[str]:
        """Backward compatibility: Private property for left join key set."""
        return self._get_left_join_keys_set()

    @property
    def _right_join_keys(self) -> Set[str]:
        """Backward compatibility: Private property for right join key set."""
        return self._get_right_join_keys_set()


class FuzzyMatchInputManager(JoinInputManager):
    """Manager for fuzzy matching join operations."""

    def __init__(self, fuzzy_input: FuzzyMatchInput):
        self.fuzzy_input = deepcopy(fuzzy_input)
        super().__init__(JoinInput(
            join_mapping=[JoinMap(left_col=fm.left_col, right_col=fm.right_col)
                          for fm in self.fuzzy_input.join_mapping],
            left_select=self.fuzzy_input.left_select,
            right_select=self.fuzzy_input.right_select,
            how=self.fuzzy_input.how
        ))

    @classmethod
    def create(cls, join_mapping: Union[List[FuzzyMapping], Tuple[str, str], str],
               left_select: Union[List[SelectInput], List[str]],
               right_select: Union[List[SelectInput], List[str]],
               aggregate_output: bool = False,
               how: JoinStrategy = 'inner') -> "FuzzyMatchInputManager":
        """Factory method to create FuzzyMatchInput from various input formats."""
        parsed_mapping = cls.parse_fuzz_mapping(join_mapping)
        left_inputs = cls.parse_select(left_select)
        right_inputs = cls.parse_select(right_select)

        fuzzy_input = FuzzyMatchInput(
            join_mapping=parsed_mapping,
            left_select=left_inputs,
            right_select=right_inputs,
            how=how,
            aggregate_output=aggregate_output
        )

        manager = cls(fuzzy_input)

        right_old_names = {v.old_name for v in fuzzy_input.right_select.renames}
        left_old_names = {v.old_name for v in fuzzy_input.left_select.renames}

        for jm in parsed_mapping:
            if jm.right_col not in right_old_names:
                manager.right_manager.append(
                    SelectInput(old_name=jm.right_col, keep=False, join_key=True)
                )
            if jm.left_col not in left_old_names:
                manager.left_manager.append(
                    SelectInput(old_name=jm.left_col, keep=False, join_key=True)
                )

        manager.set_join_keys()
        return manager

    @staticmethod
    def parse_fuzz_mapping(fuzz_mapping: Union[List[FuzzyMapping], Tuple[str, str],
                                               str, FuzzyMapping, List[Dict]]) -> List[FuzzyMapping]:
        """Parses various input formats into a list of FuzzyMapping objects."""
        if isinstance(fuzz_mapping, (tuple, list)):
            if len(fuzz_mapping) == 0:
                raise ValueError("Fuzzy mapping cannot be empty")

            if all(isinstance(fm, dict) for fm in fuzz_mapping):
                return [FuzzyMapping(**fm) for fm in fuzz_mapping]

            if all(isinstance(fm, FuzzyMapping) for fm in fuzz_mapping):
                return fuzz_mapping

            if len(fuzz_mapping) <= 2:
                if len(fuzz_mapping) == 2:
                    if isinstance(fuzz_mapping[0], str) and isinstance(fuzz_mapping[1], str):
                        return [FuzzyMapping(left_col=fuzz_mapping[0], right_col=fuzz_mapping[1])]
                elif len(fuzz_mapping) == 1 and isinstance(fuzz_mapping[0], str):
                    return [FuzzyMapping(left_col=fuzz_mapping[0], right_col=fuzz_mapping[0])]

        elif isinstance(fuzz_mapping, str):
            return [FuzzyMapping(left_col=fuzz_mapping, right_col=fuzz_mapping)]

        elif isinstance(fuzz_mapping, FuzzyMapping):
            return [fuzz_mapping]

        raise ValueError(f'No valid fuzzy mapping as input: {type(fuzz_mapping)}')

    def get_fuzzy_maps(self) -> List[FuzzyMapping]:
        """Returns the final fuzzy mappings after applying all column renames."""
        new_mappings = []
        left_rename_table = self.left_manager.get_rename_table()
        right_rename_table = self.right_manager.get_rename_table()

        for org_fuzzy_map in self.fuzzy_input.join_mapping:
            right_col = right_rename_table.get(org_fuzzy_map.right_col, org_fuzzy_map.right_col)
            left_col = left_rename_table.get(org_fuzzy_map.left_col, org_fuzzy_map.left_col)

            if right_col != org_fuzzy_map.right_col or left_col != org_fuzzy_map.left_col:
                new_mapping = deepcopy(org_fuzzy_map)
                new_mapping.left_col = left_col
                new_mapping.right_col = right_col
                new_mappings.append(new_mapping)
            else:
                new_mappings.append(org_fuzzy_map)

        return new_mappings

    # === Backward Compatibility Properties ===

    @property
    def fuzzy_maps(self) -> List[FuzzyMapping]:
        """Backward compatibility: Returns fuzzy mappings."""
        return self.get_fuzzy_maps()

    @property
    def join_mapping(self) -> List[FuzzyMapping]:
        """Backward compatibility: Access fuzzy join mapping."""
        return self.get_fuzzy_maps()

    @property
    def aggregate_output(self) -> bool:
        """Backward compatibility: Access aggregate_output setting."""
        return self.fuzzy_input.aggregate_output

    def to_fuzzy_match_input(self) -> FuzzyMatchInput:
        """Creates a new FuzzyMatchInput instance based on the current manager settings.

        This is useful when you've modified the manager (e.g., via auto_rename) and
        want to get a fresh FuzzyMatchInput with all the current settings applied.

        Returns:
            A new FuzzyMatchInput instance with current settings
        """
        return FuzzyMatchInput(
            join_mapping=self.fuzzy_input.join_mapping,
            left_select=JoinInputs(renames=self.input.left_select.renames.copy()),
            right_select=JoinInputs(renames=self.input.right_select.renames.copy()),
            how=self.fuzzy_input.how,
            aggregate_output=self.fuzzy_input.aggregate_output
        )