from typing import List, Dict, Tuple, Set, Optional, Literal, Callable
from dataclasses import dataclass, field
import polars as pl
from polars import selectors
from copy import deepcopy


def get_func_type_mapping(func: str):
    if func in ["mean", "avg", "median", "std", "var"]:
        return "Float64"
    elif func in ['min', 'max', 'first', 'last', "cumsum", "sum"]:
        return None
    elif func in ['count', 'n_unique']:
        return "Int64"
    elif func in ['concat']:
        return "Utf8"


def string_concat(*column: str):
    return pl.col(column).cast(pl.Utf8).str.concat(delimiter=',')


JoinStrategy = Literal['inner', 'left', 'right', 'full', 'semi', 'anti', 'cross']
FuzzyTypeLiteral = Literal['levenshtein', 'jaro', 'jaro_winkler', 'hamming', 'damerau_levenshtein', 'indel']


@dataclass
class SelectInput:
    # __slots__ = ['old_name', 'new_name', 'keep', 'data_type', 'data_type_change', 'join_key']
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
        if self.data_type.lower() == 'string':
            return 'Utf8'
        elif self.data_type.lower() == 'integer':
            return 'Int64'
        elif self.data_type.lower() == 'double':
            return 'Float64'
        return self.data_type


@dataclass
class FieldInput:
    # __slots__ = ['old_name', 'new_name', 'keep', 'data_type', 'data_type_change', 'join_key']
    name: str
    data_type: Optional[str] = None

    def __init__(self, name: str, data_type: str = None):
        self.name = name
        self.data_type = data_type


@dataclass
class FunctionInput:
    field: FieldInput
    function: str


@dataclass
class BasicFilter:
    field: str = ''
    filter_type: str = ''  # equals, in, not in, smaller, larger
    filter_value: str = ''


@dataclass
class FilterInput:
    advanced_filter: str = ''
    basic_filter: BasicFilter = None
    filter_type: str = 'basic'


@dataclass
class SelectInputs:
    renames: List[SelectInput]

    @property
    def old_cols(self) -> Set:
        return set(v.old_name for v in self.renames if v.keep)

    @property
    def new_cols(self) -> Set:
        return set(v.new_name for v in self.renames if v.keep or v.join_key)

    @property
    def rename_table(self):
        return {v.old_name: v.new_name for v in self.renames if (v.keep or v.join_key) and v.is_available}

    def get_select_cols(self, include_join_key: bool = True):
        return [v.old_name for v in self.renames if v.keep or (v.join_key and include_join_key)]

    def __add__(self, other: SelectInput):
        self.renames.append(other)

    def append(self, other: SelectInput):
        self.renames.append(other)

    def remove_select_input(self, old_key: str):
        self.renames = [rename for rename in self.renames if rename.old_name != old_key]

    @classmethod
    def create_from_list(cls, col_list: str):
        return cls([SelectInput(c) for c in col_list])

    @classmethod
    def create_from_pl_df(cls, df: pl.DataFrame | pl.LazyFrame):
        return cls([SelectInput(c) for c in df.columns])


@dataclass
class JoinMap:
    left_col: str
    right_col: str


@dataclass
class FuzzyMap(JoinMap):
    threshold_score: Optional[float] = 80.0
    fuzzy_type: Optional[FuzzyTypeLiteral] = 'levenshtein'
    perc_unique: Optional[float] = 0.0
    output_column_name: Optional[str] = None
    valid: Optional[bool] = True

    def __init__(self, left_col: str, right_col: str = None, threshold_score: float = 80.0,
                 fuzzy_type: FuzzyTypeLiteral = 'levenshtein', perc_unique: float = 0, output_column_name: str = None,
                 _output_col_name: str = None, valid: bool = True, output_col_name: str = None):
        if right_col is None:
            right_col = left_col
        self.valid = valid
        self.left_col = left_col
        self.right_col = right_col
        self.threshold_score = threshold_score
        self.fuzzy_type = fuzzy_type
        self.perc_unique = perc_unique
        self.output_column_name = output_column_name if output_column_name is not None else _output_col_name
        self.output_column_name = self.output_column_name if self.output_column_name is not None else output_col_name
        if self.output_column_name is None:
            self.output_column_name = f'fuzzy_score_{self.left_col}_{self.right_col}'


@dataclass
class CrossJoinInput:
    left_select: SelectInputs = None
    right_select: SelectInputs = None

    @staticmethod
    def parse_select(select: List[SelectInput] | List[str] | List[Dict]) -> SelectInputs:
        if all(isinstance(c, SelectInput) for c in select):
            return SelectInputs(select)
        elif all(isinstance(c, dict) for c in select):
            return SelectInputs([SelectInput(**c) for c in select])
        elif isinstance(select, dict):
            renames = select.get('renames')
            if renames:
                return SelectInputs([SelectInput(**c) for c in renames])
        elif all(isinstance(c, str) for c in select):
            return SelectInputs([SelectInput(s, s) for s in select])

    def __init__(self, left_select: List[SelectInput] | List[str],
                 right_select: List[SelectInput] | List[str]):
        self.left_select = self.parse_select(left_select)
        self.right_select = self.parse_select(right_select)

    @property
    def overlapping_records(self):
        return self.left_select.new_cols & self.right_select.new_cols

    def auto_rename(self):
        overlapping_records = self.overlapping_records
        while len(overlapping_records) > 0:
            for right_col in self.right_select.renames:
                if right_col.new_name in overlapping_records:
                    right_col.new_name = 'right_' + right_col.new_name
            overlapping_records = self.overlapping_records

    def auto_generate_new_col_name(self, old_col_name: str, side: str) -> str:
        current_names = self.left_select.new_cols & self.right_select.new_cols
        if old_col_name not in current_names:
            return old_col_name
        while True:
            if old_col_name not in current_names:
                return old_col_name
            old_col_name = f'{side}_{old_col_name}'

    def add_new_select_column(self, select_input: SelectInput, side: str):
        selects = self.right_select if side == 'right' else self.left_select
        select_input.new_name = self.auto_generate_new_col_name(select_input.old_name, side=side)
        selects.__add__(select_input)


@dataclass
class JoinInput:
    join_mapping: List[JoinMap]
    left_select: SelectInputs = None
    right_select: SelectInputs = None
    how: JoinStrategy = 'inner'

    @staticmethod
    def parse_join_mapping(join_mapping: List[JoinMap] | Tuple[str, str] | str) -> List[JoinMap]:
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

    @staticmethod
    def parse_select(select: List[SelectInput] | List[str] | List[Dict]) -> SelectInputs:
        if all(isinstance(c, SelectInput) for c in select):
            return SelectInputs(select)
        elif all(isinstance(c, dict) for c in select):
            return SelectInputs([SelectInput(**c) for c in select])
        elif isinstance(select, dict):
            renames = select.get('renames')
            if renames:
                return SelectInputs([SelectInput(**c) for c in renames])
        elif all(isinstance(c, str) for c in select):
            return SelectInputs([SelectInput(s, s) for s in select])

    def __init__(self, join_mapping: List[JoinMap] | Tuple[str, str] | str, left_select: List[SelectInput] | List[str],
                 right_select: List[SelectInput] | List[str],
                 how: JoinStrategy = 'inner'):
        self.join_mapping = self.parse_join_mapping(join_mapping)
        self.left_select = self.parse_select(left_select)
        self.right_select = self.parse_select(right_select)
        [setattr(v, "join_key", v.old_name in self._left_join_keys) for v in self.left_select.renames]
        [setattr(v, "join_key", v.old_name in self._right_join_keys) for v in self.right_select.renames]
        self.how = how

    @property
    def _left_join_keys(self) -> Set:
        return set(jm.left_col for jm in self.join_mapping)

    @property
    def _right_join_keys(self) -> Set:
        return set(jm.right_col for jm in self.join_mapping)

    @property
    def left_join_keys(self) -> List:
        return [self.left_select.rename_table.get(jm.left_col) for jm in self.join_mapping]

    @property
    def right_join_keys(self) -> List:
        return [self.right_select.rename_table.get(jm.right_col, jm.right_col) for jm in self.join_mapping]

    @property
    def overlapping_records(self):
        if self.how in ('left', 'right', 'inner'):
            #  Never consider join keys as overlapping records since they will be dropped after the join
            return ((self.left_select.new_cols & self.right_select.new_cols) -
                    (set(self.left_join_keys) & set(self.right_join_keys)))
        else:
            return self.left_select.new_cols & self.right_select.new_cols

    def auto_rename(self):
        overlapping_records = self.overlapping_records
        while len(overlapping_records) > 0:
            for right_col in self.right_select.renames:
                if right_col.new_name in overlapping_records:
                    right_col.new_name = right_col.new_name + '_right'
            overlapping_records = self.overlapping_records

    @property
    def join_mappings(self):
        new_mappings = []
        left_rename_table, right_rename_table = self.left_select.rename_table, self.right_select.rename_table
        for join_map in self.join_mapping:
            # del self.right_select.rename_table, self.left_select.rename_table
            new_mappings.append(JoinMap(left_rename_table.get(join_map.left_col),
                                        right_rename_table.get(join_map.right_col)
                                        )
                                )
        return new_mappings

    def auto_generate_new_col_name(self, old_col_name: str, side: str) -> str:
        current_names = self.left_select.new_cols & self.right_select.new_cols
        if old_col_name not in current_names:
            return old_col_name
        while True:
            if old_col_name not in current_names:
                return old_col_name
            old_col_name = f'{side}_{old_col_name}'

    def add_new_select_column(self, select_input: SelectInput, side: str):
        selects = self.right_select if side == 'right' else self.left_select
        select_input.new_name = self.auto_generate_new_col_name(select_input.old_name, side=side)
        selects.__add__(select_input)


@dataclass
class FuzzyMatchInput(JoinInput):
    join_mapping: List[FuzzyMap]
    aggregate_output: bool = False

    @staticmethod
    def parse_fuzz_mapping(fuzz_mapping: List[FuzzyMap] | Tuple[str, str] | str) -> List[FuzzyMap]:
        if isinstance(fuzz_mapping, (tuple, list)):
            assert len(fuzz_mapping) > 0
            if all(isinstance(fm, dict) for fm in fuzz_mapping):
                fuzz_mapping = [FuzzyMap(**fm) for fm in fuzz_mapping]

            if not isinstance(fuzz_mapping[0], FuzzyMap):
                assert len(fuzz_mapping) <= 2
                if len(fuzz_mapping) == 2:
                    assert isinstance(fuzz_mapping[0], str) and isinstance(fuzz_mapping[1], str)
                    fuzz_mapping = [FuzzyMap(*fuzz_mapping)]
                elif isinstance(fuzz_mapping[0], str):
                    fuzz_mapping = [FuzzyMap(fuzz_mapping[0], fuzz_mapping[0])]
        elif isinstance(fuzz_mapping, str):
            fuzz_mapping = [FuzzyMap(fuzz_mapping, fuzz_mapping)]
        elif isinstance(fuzz_mapping, FuzzyMap):
            fuzz_mapping = [fuzz_mapping]
        else:
            raise Exception('No valid join mapping as input')
        return fuzz_mapping

    def __init__(self, join_mapping: List[FuzzyMap] | Tuple[str, str] | str, left_select: List[SelectInput] | List[str],
                 right_select: List[SelectInput] | List[str], aggregate_output: bool = False, how: str = 'inner'):
        self.join_mapping = self.parse_fuzz_mapping(join_mapping)
        self.left_select = self.parse_select(left_select)
        self.right_select = self.parse_select(right_select)
        self.how = how
        for jm in self.join_mapping:

            if jm.right_col not in self.right_select.old_cols:
                self.right_select.append(SelectInput(jm.right_col, keep=False, join_key=True))
            if jm.left_col not in self.left_select.old_cols:
                self.left_select.append(SelectInput(jm.left_col, keep=False, join_key=True))
        [setattr(v, "join_key", v.old_name in self._left_join_keys) for v in self.left_select.renames]
        [setattr(v, "join_key", v.old_name in self._right_join_keys) for v in self.right_select.renames]
        self.aggregate_output = aggregate_output

    @property
    def overlapping_records(self):
        return self.left_select.new_cols & self.right_select.new_cols

    @property
    def fuzzy_maps(self) -> List[FuzzyMap]:
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
        self.old_name = str(old_name)
        if agg != 'groupby':
            self.new_name = new_name if new_name is not None else self.old_name + "_" + agg
        else:
            self.new_name = new_name if new_name is not None else self.old_name
        self.output_type = output_type if output_type is not None else get_func_type_mapping(agg)
        self.agg = agg

    @property
    def agg_func(self):
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
    index_columns: List[str]
    pivot_column: str
    value_col: str
    aggregations: List[str]

    @property
    def grouped_columns(self) -> List[str]:
        return self.index_columns + [self.pivot_column]

    def get_group_by_input(self) -> GroupByInput:
        group_by_cols = [AggColl(c, 'groupby') for c in self.grouped_columns]
        agg_cols = [AggColl(self.value_col, agg=aggregation, new_name=aggregation) for aggregation in self.aggregations]
        return GroupByInput(group_by_cols+agg_cols)

    def get_index_columns(self) -> List[pl.col]:
        return [pl.col(c) for c in self.index_columns]

    def get_pivot_column(self) -> pl.col:
        return pl.col(self.pivot_column)

    def get_values_expr(self) -> pl.Expr:
        return pl.struct([pl.col(c) for c in self.aggregations]).alias('vals')



@dataclass
class SortByInput:
    column: str
    how: str = 'asc'


@dataclass
class RecordIdInput:
    output_column_name: str = 'record_id'
    offset: int = 1
    group_by: Optional[bool] = False
    group_by_columns: Optional[List[str]] = field(default_factory=list)


@dataclass
class TextToRowsInput:
    column_to_split: str
    output_column_name: Optional[str] = None
    split_by_fixed_value: Optional[bool] = True
    split_fixed_value: Optional[str] = ','
    split_by_column: Optional[str] = None


@dataclass
class UnpivotInput:
    index_columns: Optional[List[str]] = field(default_factory=list)
    value_columns: Optional[List[str]] = field(default_factory=list)
    data_type_selector: Optional[Literal['float', 'all', 'date', 'numeric', 'string']] = None
    data_type_selector_mode: Optional[Literal['data_type', 'column']] = 'column'

    def __post_init__(self):
        if self.index_columns is None:
            self.index_columns = []
        if self.value_columns is None:
            self.value_columns = []
        if self.data_type_selector_mode is None:
            self.data_type_selector_mode = 'column'

    @property
    def data_type_selector_expr(self) -> Callable:
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
    mode: Literal['selective', 'relaxed'] = 'relaxed'


@dataclass
class UniqueInput:
    columns: Optional[List[str]] = None
    strategy: Literal["first", "last", "any", "none"] = "any"


@dataclass
class GraphSolverInput:
    col_from: str
    col_to: str
    output_column_name: Optional[str] = 'graph_group'


@dataclass
class PolarsCodeInput:
    polars_code: str

