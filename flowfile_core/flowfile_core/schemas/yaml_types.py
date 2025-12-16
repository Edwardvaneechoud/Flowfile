from typing import TypedDict, List


# === Transform Schema YAML Types ===

class SelectInputYaml(TypedDict, total=False):
    old_name: str
    new_name: str
    keep: bool
    data_type: str


class JoinInputsYaml(TypedDict):
    select: List[SelectInputYaml]


class JoinMapYaml(TypedDict):
    left_col: str
    right_col: str


class JoinInputYaml(TypedDict):
    join_mapping: List[JoinMapYaml]
    left_select: JoinInputsYaml
    right_select: JoinInputsYaml
    how: str


class CrossJoinInputYaml(TypedDict):
    left_select: JoinInputsYaml
    right_select: JoinInputsYaml


class FuzzyMappingYaml(TypedDict, total=False):
    left_col: str
    right_col: str
    threshold_score: float
    fuzzy_type: str
    perc_unique: float
    output_column_name: str
    valid: bool


class FuzzyMatchInputYaml(TypedDict):
    join_mapping: List[FuzzyMappingYaml]
    left_select: JoinInputsYaml
    right_select: JoinInputsYaml
    how: str
    aggregate_output: bool


# === Input Schema YAML Types ===

class OutputSettingsYaml(TypedDict, total=False):
    name: str
    directory: str
    file_type: str
    write_mode: str
    abs_file_path: str
    fields: List[str]
    table_settings: dict


class NodeSelectYaml(TypedDict):
    cache_results: bool
    keep_missing: bool
    select_input: List[SelectInputYaml]
    sorted_by: str


class NodeJoinYaml(TypedDict):
    cache_results: bool
    auto_generate_selection: bool
    verify_integrity: bool
    join_input: JoinInputYaml
    auto_keep_all: bool
    auto_keep_right: bool
    auto_keep_left: bool


class NodeCrossJoinYaml(TypedDict):
    cache_results: bool
    auto_generate_selection: bool
    verify_integrity: bool
    cross_join_input: CrossJoinInputYaml
    auto_keep_all: bool
    auto_keep_right: bool
    auto_keep_left: bool


class NodeFuzzyMatchYaml(TypedDict):
    cache_results: bool
    auto_generate_selection: bool
    verify_integrity: bool
    join_input: FuzzyMatchInputYaml
    auto_keep_all: bool
    auto_keep_right: bool
    auto_keep_left: bool


class NodeOutputYaml(TypedDict):
    cache_results: bool
    output_settings: OutputSettingsYaml
