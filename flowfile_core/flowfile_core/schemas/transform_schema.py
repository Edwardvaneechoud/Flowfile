from typing import List, Dict, Tuple, Set, Optional, Literal, Callable
from dataclasses import dataclass, field
import polars as pl
from polars import selectors
from copy import deepcopy

from typing import NamedTuple

from pl_fuzzy_frame_match.models import FuzzyMapping

FuzzyMap = FuzzyMapping  # For backwards compatibility

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


@dataclass
class SelectInput:
    """Defines how a single column should be selected, renamed, or type-cast.

    This is a core building block for any operation that involves column manipulation.
    It holds all the configuration for a single field in a selection operation.
    """
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

    def __hash__(self):
        return hash(self.old_name)

    def __init__(self, old_name: str, new_name: str = None, keep: bool = True, data_type: str = None,
                 data_type_change: bool = False, join_key: bool = False, is_altered: bool = False,
                 is_available: bool = True, position: int = None):
        self.old_name = old_name
        if new_name is None:
            new_name = old_name
        self.new_name = new_name
        self.keep = keep
        self.data_type = data_type
        self.data_type_change = data_type_change
        self.join_key = join_key
        self.is_altered = is_altered
        self.is_available = is_available
        self.position = position

    @property
    def polars_type(self) -> str:
        """Translates a user-friendly type name to a Polars data type string."""
        if self.data_type.lower() == 'string':
            return 'Utf8'
        elif self.data_type.lower() == 'integer':
            return 'Int64'
        elif self.data_type.lower() == 'double':
            return 'Float64'
        return self.data_type


@dataclass
class FieldInput:
    """Represents a single field with its name and data type, typically for defining an output column."""
    name: str
    data_type: Optional[str] = None

    def __init__(self, name: str, data_type: str = None):
        self.name = name
        self.data_type = data_type


@dataclass
class FunctionInput:
    """Defines a formula to be applied, including the output field information."""
    field: FieldInput
    function: str


@dataclass
class BasicFilter:
    """Defines a simple, single-condition filter (e.g., 'column' 'equals' 'value')."""
    field: str = ''
    filter_type: str = ''
    filter_value: str = ''


@dataclass
class FilterInput:
    """Defines the settings for a filter operation, supporting basic or advanced (expression-based) modes."""
    advanced_filter: str = ''
    basic_filter: BasicFilter = None
    filter_type: str = 'basic'


@dataclass
class SelectInputs:
    """A container for a list of `SelectInput` objects, providing helper methods for managing selections."""
    renames: List[SelectInput]

    @property
    def old_cols(self) -> Set:
        """Returns a set of original column names to be kept in the selection."""
        return set(v.old_name for v in self.renames if v.keep)

    @property
    def new_cols(self) -> Set:
        """Returns a set of new (renamed) column names to be kept in the selection."""
        return set(v.new_name for v in self.renames if v.keep)

    @property
    def rename_table(self):
        """Generates a dictionary for use in Polars' `.rename()` method."""
        return {v.old_name: v.new_name for v in self.renames if v.is_available and (v.keep or v.join_key)}

    def get_select_cols(self, include_join_key: bool = True):
        """Gets a list of original column names to select from the source DataFrame."""
        return [v.old_name for v in self.renames if v.keep or (v.join_key and include_join_key)]

    def has_drop_cols(self) -> bool:
        """Checks if any column is marked to be dropped from the selection."""
        return any(not v.keep for v in self.renames)

    @property
    def drop_columns(self) -> List[SelectInput]:
        """Returns a list of column names that are marked to be dropped from the selection."""
        return [v for v in self.renames if not v.keep and v.is_available]

    @property
    def non_jk_drop_columns(self) -> List[SelectInput]:
        return [v for v in self.renames if not v.keep and v.is_available and not v.join_key]

    def __add__(self, other: "SelectInput"):
        """Allows adding a SelectInput using the '+' operator."""
        self.renames.append(other)

    def append(self, other: "SelectInput"):
        """Appends a new SelectInput to the list of renames."""
        self.renames.append(other)

    def remove_select_input(self, old_key: str):
        """Removes a SelectInput from the list based on its original name."""
        self.renames = [rename for rename in self.renames if rename.old_name != old_key]

    def unselect_field(self, old_key: str):
        """Marks a field to be dropped from the final selection by setting `keep` to False."""
        for rename in self.renames:
            if old_key == rename.old_name:
                rename.keep = False

    @classmethod
    def create_from_list(cls, col_list: List[str]):
        """Creates a SelectInputs object from a simple list of column names."""
        return cls([SelectInput(c) for c in col_list])

    @classmethod
    def create_from_pl_df(cls, df: pl.DataFrame | pl.LazyFrame):
        """Creates a SelectInputs object from a Polars DataFrame's columns."""
        return cls([SelectInput(c) for c in df.columns])

    def get_select_input_on_old_name(self, old_name: str) -> SelectInput | None:
        return next((v for v in self.renames if v.old_name == old_name), None)

    def get_select_input_on_new_name(self, old_name: str) -> SelectInput | None:
        return next((v for v in self.renames if v.new_name == old_name), None)


class JoinInputs(SelectInputs):
    """Extends `SelectInputs` with functionality specific to join operations, like handling join keys."""

    def __init__(self, renames: List[SelectInput]):
        self.renames = renames

    @property
    def join_key_selects(self) -> List[SelectInput]:
        """Returns only the `SelectInput` objects that are marked as join keys."""
        return [v for v in self.renames if v.join_key]

    def get_join_key_renames(self, side: SideLit, filter_drop: bool = False) -> JoinKeyRenameResponse:
        """Gets the temporary rename mapping for all join keys on one side of a join."""
        return JoinKeyRenameResponse(
            side,
            [JoinKeyRename(jk.new_name,
                           construct_join_key_name(side, jk.new_name))
             for jk in self.join_key_selects if jk.keep or not filter_drop]
        )

    def get_join_key_rename_mapping(self, side: SideLit) -> Dict[str, str]:
        """Returns a dictionary mapping original join key names to their temporary names."""
        return {jkr[0]: jkr[1] for jkr in self.get_join_key_renames(side)[1]}


@dataclass
class JoinMap:
    """Defines a single mapping between a left and right column for a join key."""
    left_col: str
    right_col: str


class JoinSelectMixin:
    """A mixin providing common methods for join-like operations that involve left and right inputs."""
    left_select: JoinInputs = None
    right_select: JoinInputs = None

    @staticmethod
    def parse_select(select: List[SelectInput] | List[str] | List[Dict]) -> JoinInputs | None:
        """Parses various input formats into a standardized `JoinInputs` object."""
        if all(isinstance(c, SelectInput) for c in select):
            return JoinInputs(select)
        elif all(isinstance(c, dict) for c in select):
            return JoinInputs([SelectInput(**c.__dict__) for c in select])
        elif isinstance(select, dict):
            renames = select.get('renames')
            if renames:
                return JoinInputs([SelectInput(**c) for c in renames])
        elif all(isinstance(c, str) for c in select):
            return JoinInputs([SelectInput(s, s) for s in select])

    def auto_generate_new_col_name(self, old_col_name: str, side: str) -> str:
        """Generates a new, non-conflicting column name by adding a suffix if necessary."""
        current_names = self.left_select.new_cols & self.right_select.new_cols
        if old_col_name not in current_names:
            return old_col_name
        while True:
            if old_col_name not in current_names:
                return old_col_name
            old_col_name = f'{side}_{old_col_name}'

    def add_new_select_column(self, select_input: SelectInput, side: str):
        """Adds a new column to the selection for either the left or right side."""
        selects = self.right_select if side == 'right' else self.left_select
        select_input.new_name = self.auto_generate_new_col_name(select_input.old_name, side=side)
        selects.__add__(select_input)


@dataclass
class CrossJoinInput(JoinSelectMixin):
    """Defines the settings for a cross join operation, including column selections for both inputs."""
    left_select: SelectInputs = None
    right_select: SelectInputs = None

    def __init__(self, left_select: List[SelectInput] | List[str],
                 right_select: List[SelectInput] | List[str]):
        """Initializes the CrossJoinInput with selections for left and right tables."""
        self.left_select = self.parse_select(left_select)
        self.right_select = self.parse_select(right_select)

    @property
    def overlapping_records(self):
        """Finds column names that would conflict after the join."""
        return self.left_select.new_cols & self.right_select.new_cols

    def auto_rename(self):
        """Automatically renames columns on the right side to prevent naming conflicts."""
        overlapping_records = self.overlapping_records
        while len(overlapping_records) > 0:
            for right_col in self.right_select.renames:
                if right_col.new_name in overlapping_records:
                    right_col.new_name = 'right_' + right_col.new_name
            overlapping_records = self.overlapping_records


@dataclass
class JoinInput(JoinSelectMixin):
    """Defines the settings for a standard SQL-style join, including keys, strategy, and selections."""
    join_mapping: List[JoinMap]
    left_select: JoinInputs = None
    right_select: JoinInputs = None
    how: JoinStrategy = 'inner'

    @staticmethod
    def parse_join_mapping(join_mapping: any) -> List[JoinMap]:
        """Parses various input formats for join keys into a standardized list of `JoinMap` objects."""
        if isinstance(join_mapping, (tuple, list)):
            assert len(join_mapping) > 0
            if all(isinstance(jm, dict) for jm in join_mapping):
                join_mapping = [JoinMap(**jm) for jm in join_mapping]

            if not isinstance(join_mapping[0], JoinMap):
                assert len(join_mapping) <= 2
                if len(join_mapping) == 2:
                    assert isinstance(join_mapping[0], str) and isinstance(join_mapping[1], str)
                    join_mapping = [JoinMap(*join_mapping)]
                elif isinstance(join_mapping[0], str):
                    join_mapping = [JoinMap(join_mapping[0], join_mapping[0])]
        elif isinstance(join_mapping, str):
            join_mapping = [JoinMap(join_mapping, join_mapping)]
        else:
            raise Exception('No valid join mapping as input')
        return join_mapping

    def __init__(self, join_mapping: List[JoinMap] | Tuple[str, str] | str,
                 left_select: List[SelectInput] | List[str],
                 right_select: List[SelectInput] | List[str],
                 how: JoinStrategy = 'inner'):
        """Initializes the JoinInput with keys, selections, and join strategy."""
        self.join_mapping = self.parse_join_mapping(join_mapping)
        self.left_select = self.parse_select(left_select)
        self.right_select = self.parse_select(right_select)
        self.set_join_keys()
        self.how = how

    def set_join_keys(self):
        """Marks the `SelectInput` objects corresponding to join keys."""
        [setattr(v, "join_key", v.old_name in self._left_join_keys) for v in self.left_select.renames]
        [setattr(v, "join_key", v.old_name in self._right_join_keys) for v in self.right_select.renames]

    def get_join_key_renames(self, filter_drop: bool = False) -> FullJoinKeyResponse:
        """Gets the temporary rename mappings for the join keys on both sides."""
        return FullJoinKeyResponse(self.left_select.get_join_key_renames(side="left", filter_drop=filter_drop),
                                   self.right_select.get_join_key_renames(side="right", filter_drop=filter_drop))

    def get_names_for_table_rename(self) -> List[JoinMap]:
        new_mappings: List[JoinMap] = []
        left_rename_table, right_rename_table = self.left_select.rename_table, self.right_select.rename_table
        for join_map in self.join_mapping:
            new_mappings.append(JoinMap(left_rename_table.get(join_map.left_col, join_map.left_col),
                                        right_rename_table.get(join_map.right_col, join_map.right_col)
                                        )
                                )
        return new_mappings

    @property
    def _left_join_keys(self) -> Set:
        """Returns a set of the left-side join key column names."""
        return set(jm.left_col for jm in self.join_mapping)

    @property
    def _right_join_keys(self) -> Set:
        """Returns a set of the right-side join key column names."""
        return set(jm.right_col for jm in self.join_mapping)

    @property
    def left_join_keys(self) -> List[str]:
        """Returns an ordered list of the left-side join key column names to be used in the join."""
        return [jm.left_col for jm in self.used_join_mapping]

    @property
    def right_join_keys(self) -> List[str]:
        """Returns an ordered list of the right-side join key column names to be used in the join."""
        return [jm.right_col for jm in self.used_join_mapping]

    @property
    def overlapping_records(self):
        if self.how in ('left', 'right', 'inner'):
            return self.left_select.new_cols & self.right_select.new_cols
        else:
            return self.left_select.new_cols & self.right_select.new_cols

    def auto_rename(self):
        """Automatically renames columns on the right side to prevent naming conflicts."""
        self.set_join_keys()
        overlapping_records = self.overlapping_records
        while len(overlapping_records) > 0:
            for right_col in self.right_select.renames:
                if right_col.new_name in overlapping_records:
                    right_col.new_name = right_col.new_name + '_right'
            overlapping_records = self.overlapping_records

    @property
    def used_join_mapping(self) -> List[JoinMap]:
        """Returns the final join mapping after applying all renames and transformations."""
        new_mappings: List[JoinMap] = []
        left_rename_table, right_rename_table = self.left_select.rename_table, self.right_select.rename_table
        left_join_rename_mapping: Dict[str, str] = self.left_select.get_join_key_rename_mapping("left")
        right_join_rename_mapping: Dict[str, str] = self.right_select.get_join_key_rename_mapping("right")
        for join_map in self.join_mapping:
            # del self.right_select.rename_table, self.left_select.rename_table
            new_mappings.append(JoinMap(left_join_rename_mapping.get(left_rename_table.get(join_map.left_col, join_map.left_col)),
                                        right_join_rename_mapping.get(right_rename_table.get(join_map.right_col, join_map.right_col))
                                        )
                                )
        return new_mappings


@dataclass
class FuzzyMatchInput(JoinInput):
    """Extends `JoinInput` with settings specific to fuzzy matching, such as the matching algorithm and threshold."""
    join_mapping: List[FuzzyMapping]
    aggregate_output: bool = False

    @staticmethod
    def parse_fuzz_mapping(fuzz_mapping: List[FuzzyMapping] | Tuple[str, str] | str) -> List[FuzzyMapping]:
        if isinstance(fuzz_mapping, (tuple, list)):
            assert len(fuzz_mapping) > 0
            if all(isinstance(fm, dict) for fm in fuzz_mapping):
                fuzz_mapping = [FuzzyMapping(**fm) for fm in fuzz_mapping]

            if not isinstance(fuzz_mapping[0], FuzzyMapping):
                assert len(fuzz_mapping) <= 2
                if len(fuzz_mapping) == 2:
                    assert isinstance(fuzz_mapping[0], str) and isinstance(fuzz_mapping[1], str)
                    fuzz_mapping = [FuzzyMapping(*fuzz_mapping)]
                elif isinstance(fuzz_mapping[0], str):
                    fuzz_mapping = [FuzzyMapping(fuzz_mapping[0], fuzz_mapping[0])]
        elif isinstance(fuzz_mapping, str):
            fuzz_mapping = [FuzzyMapping(fuzz_mapping, fuzz_mapping)]
        elif isinstance(fuzz_mapping, FuzzyMapping):
            fuzz_mapping = [fuzz_mapping]
        else:
            raise Exception('No valid join mapping as input')
        return fuzz_mapping

    def __init__(self, join_mapping: List[FuzzyMapping] | Tuple[str, str] | str, left_select: List[SelectInput] | List[str],
                 right_select: List[SelectInput] | List[str], aggregate_output: bool = False, how: JoinStrategy = 'inner'):
        self.join_mapping = self.parse_fuzz_mapping(join_mapping)
        self.left_select = self.parse_select(left_select)
        self.right_select = self.parse_select(right_select)
        self.how = how
        for jm in self.join_mapping:

            if jm.right_col not in {v.old_name for v in self.right_select.renames}:
                self.right_select.append(SelectInput(jm.right_col, keep=False, join_key=True))
            if jm.left_col not in {v.old_name for v in self.left_select.renames}:
                self.left_select.append(SelectInput(jm.left_col, keep=False, join_key=True))
        [setattr(v, "join_key", v.old_name in self._left_join_keys) for v in self.left_select.renames]
        [setattr(v, "join_key", v.old_name in self._right_join_keys) for v in self.right_select.renames]
        self.aggregate_output = aggregate_output

    @property
    def overlapping_records(self):
        return self.left_select.new_cols & self.right_select.new_cols

    @property
    def fuzzy_maps(self) -> List[FuzzyMapping]:
        """Returns the final fuzzy mappings after applying all column renames."""
        new_mappings = []
        left_rename_table, right_rename_table = self.left_select.rename_table, self.right_select.rename_table
        for org_fuzzy_map in self.join_mapping:
            right_col = right_rename_table.get(org_fuzzy_map.right_col)
            left_col = left_rename_table.get(org_fuzzy_map.left_col)
            if right_col != org_fuzzy_map.right_col or left_col != org_fuzzy_map.left_col:
                new_mapping = deepcopy(org_fuzzy_map)
                new_mapping.left_col = left_col
                new_mapping.right_col = right_col
                new_mappings.append(new_mapping)
            else:
                new_mappings.append(org_fuzzy_map)
        return new_mappings


@dataclass
class AggColl:
    """
    A data class that represents a single aggregation operation for a group by operation.

    Attributes
    ----------
    old_name : str
        The name of the column in the original DataFrame to be aggregated.

    agg : Any
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
    new_name: Optional[str]
    output_type: Optional[str] = None

    def __init__(self, old_name: str, agg: str, new_name: str = None, output_type: str = None):
        """Initializes an aggregation column with its source, function, and new name."""
        self.old_name = str(old_name)
        if agg != 'groupby':
            self.new_name = new_name if new_name is not None else self.old_name + "_" + agg
        else:
            self.new_name = new_name if new_name is not None else self.old_name
        self.output_type = output_type if output_type is not None else get_func_type_mapping(agg)
        self.agg = agg

    @property
    def agg_func(self):
        """Returns the corresponding Polars aggregation function from the `agg` string."""
        if self.agg == 'groupby':
            return self.agg
        elif self.agg == 'concat':
            return string_concat
        else:
            return getattr(pl, self.agg) if isinstance(self.agg, str) else self.agg


@dataclass
class GroupByInput:
    """
    A data class that represents the input for a group by operation.

    Attributes
    ----------
    group_columns : List[str]
        A list of column names to group the DataFrame by. These column(s) will be set as the DataFrame index.

    agg_cols : List[AggColl]
        A list of `AggColl` objects that specify the aggregation operations to perform on the DataFrame columns
        after grouping. Each `AggColl` object should specify the column to be aggregated and the aggregation
        function to use.

    Example
    --------
    group_by_input = GroupByInput(
        agg_cols=[AggColl(old_name='ix', agg='groupby'), AggColl(old_name='groups', agg='groupby'), AggColl(old_name='col1', agg='sum'), AggColl(old_name='col2', agg='mean')]
    )
    """
    agg_cols: List[AggColl]


@dataclass
class PivotInput:
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
        group_by_cols = [AggColl(c, 'groupby') for c in self.grouped_columns]
        agg_cols = [AggColl(self.value_col, agg=aggregation, new_name=aggregation) for aggregation in self.aggregations]
        return GroupByInput(group_by_cols+agg_cols)

    def get_index_columns(self) -> List[pl.col]:
        return [pl.col(c) for c in self.index_columns]

    def get_pivot_column(self) -> pl.Expr:
        """Returns the pivot column as a Polars column expression."""
        return pl.col(self.pivot_column)

    def get_values_expr(self) -> pl.Expr:
        """Creates the struct expression used to gather the values for pivoting."""
        return pl.struct([pl.col(c) for c in self.aggregations]).alias('vals')


@dataclass
class SortByInput:
    """Defines a single sort condition on a column, including the direction."""
    column: str
    how: str = 'asc'


@dataclass
class RecordIdInput:
    """Defines settings for adding a record ID (row number) column to the data."""
    output_column_name: str = 'record_id'
    offset: int = 1
    group_by: Optional[bool] = False
    group_by_columns: Optional[List[str]] = field(default_factory=list)


@dataclass
class TextToRowsInput:
    """Defines settings for splitting a text column into multiple rows based on a delimiter."""
    column_to_split: str
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
        """Ensures that list attributes are initialized correctly if they are None."""
        if self.index_columns is None:
            self.index_columns = []
        if self.value_columns is None:
            self.value_columns = []
        if self.data_type_selector_mode is None:
            self.data_type_selector_mode = 'column'

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


@dataclass
class UnionInput:
    """Defines settings for a union (concatenation) operation."""
    mode: Literal['selective', 'relaxed'] = 'relaxed'


@dataclass
class UniqueInput:
    """Defines settings for a uniqueness operation, specifying columns and which row to keep."""
    columns: Optional[List[str]] = None
    strategy: Literal["first", "last", "any", "none"] = "any"


@dataclass
class GraphSolverInput:
    """Defines settings for a graph-solving operation (e.g., finding connected components)."""
    col_from: str
    col_to: str
    output_column_name: Optional[str] = 'graph_group'


@dataclass
class PolarsCodeInput:
    """A simple container for a string of user-provided Polars code to be executed."""
    polars_code: str
