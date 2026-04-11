from pathlib import Path
from uuid import uuid4

import polars as pl
import pytest
from pl_fuzzy_frame_match.models import FuzzyMapping
from polars.testing import assert_frame_equal

from flowfile_core.flowfile.code_generator.code_generator import export_flow_to_flowframe, export_flow_to_polars, UnsupportedNodeError
from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
from flowfile_frame.flow_frame import FlowFrame
from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection
from flowfile_core.schemas import cloud_storage_schemas as cloud_ss
from flowfile_core.schemas import input_schema, schemas, transform_schema

try:
    import os

    from tests.flowfile_core_test_utils import ensure_password_is_available, is_docker_available
    from tests.utils import ensure_cloud_storage_connection_is_available_and_get_connection, get_cloud_connection
except ModuleNotFoundError:
    import os
    import sys
    sys.path.append(os.path.dirname(os.path.abspath("flowfile_core/tests/flowfile_core_test_utils.py")))
    sys.path.append(os.path.dirname(os.path.abspath("flowfile_core/tests/utils.py")))
    # noinspection PyUnresolvedReferences
    from flowfile_core_test_utils import is_docker_available

    from tests.utils import ensure_cloud_storage_connection_is_available_and_get_connection, get_cloud_connection


def create_flow_settings(flow_id: int = 1) -> schemas.FlowSettings:
    """Create basic flow settings for tests"""
    return schemas.FlowSettings(
        flow_id=flow_id,
        execution_mode="Performance",
        execution_location="local",
        path="/tmp/test_flow"
    )


def find_parent_directory(target_dir_name, start_path=None):
    """Navigate up directories until finding the target directory"""
    current_path = Path(start_path) if start_path else Path.cwd()

    while current_path != current_path.parent:
        if current_path.name == target_dir_name:
            return current_path
        if current_path.name == target_dir_name:
            return current_path
        current_path = current_path.parent

    raise FileNotFoundError(f"Directory '{target_dir_name}' not found")


def verify_if_execute(code: str):
    exec_globals = {}
    try:
        exec(code, exec_globals)
        _ = exec_globals['run_etl_pipeline']()
    except Exception as e:
        raise Exception(f"Code execution should not raise an exception:\n {e}\n\n could not execute {code}")


def get_result_from_generated_code(code: str) -> pl.DataFrame | pl.LazyFrame | list[pl.DataFrame | pl.LazyFrame] | None:
    exec_globals = {}
    exec(code, exec_globals)
    return exec_globals['run_etl_pipeline']()


def normalize_result(result):
    """Convert FlowFrame/LazyFrame results to pl.DataFrame for comparison."""
    if hasattr(result, "data") and hasattr(result, "collect"):
        # FlowFrame — use .collect()
        return result.collect()
    if hasattr(result, "collect"):
        return result.collect()
    return result


def create_basic_flow(flow_id: int = 1, name: str = "test_flow") -> FlowGraph:
    """Create a basic flow graph for testing"""
    return FlowGraph(flow_settings=create_flow_settings(flow_id), name=name)


def generate_parameterized_join_tests():
    """Generate parameterized test cases with descriptive names"""

    test_cases = []

    join_types = ["inner", "left", "right", "outer"]
    rename_configs = [
        ("no_rename", False, False),
        ("left_rename", True, False),
        ("right_rename", False, True),
        ("both_rename", True, True)
    ]
    keep_configs = [
        ("all_keep", True, True),
        ("left_no_keep", False, True),
        ("right_no_keep", True, False),
        ("both_no_keep", False, False)
    ]

    for join_type in join_types:
        for rename_name, left_rename, right_rename in rename_configs:
            for keep_name, left_keep, right_keep in keep_configs:
                test_name = f"{join_type}_{rename_name}_{keep_name}"

                scenario = input_schema.NodeJoin(
                    flow_id=1,
                    node_id=3,
                    depending_on_ids=[1, 2],
                    join_input=transform_schema.JoinInput(
                        join_mapping=[transform_schema.JoinMap("id", "id")],
                        left_select=[
                            transform_schema.SelectInput(
                                "id",
                                "left_id" if left_rename else "id",
                                keep=left_keep
                            ),
                            transform_schema.SelectInput(
                                "name",
                                "left_name" if left_rename else "name"
                            )
                        ],
                        right_select=[
                            transform_schema.SelectInput(
                                "id",
                                "right_id" if right_rename else "id",
                                keep=right_keep
                            ),
                            transform_schema.SelectInput(
                                "city",
                                "right_city" if right_rename else "city"
                            )
                        ],
                        how=join_type
                    )
                )

                test_cases.append((test_name, scenario))

    return test_cases


def generate_parameterized_join_tests_same_df():
    """Generate parameterized test cases with descriptive names"""

    test_cases = []

    join_types = ["inner", "left", "right", "outer"]
    rename_configs = [
        ("no_rename", False, False),
        ("left_rename", True, False),
        ("right_rename", False, True),
        ("both_rename", True, True)
    ]
    keep_configs = [
        ("all_keep", True, True),
        ("left_no_keep", False, True),
        ("right_no_keep", True, False),
        ("both_no_keep", False, False)
    ]

    for join_type in join_types:
        for rename_name, left_rename, right_rename in rename_configs:
            for keep_name, left_keep, right_keep in keep_configs:
                test_name = f"{join_type}_{rename_name}_{keep_name}"

                scenario = input_schema.NodeJoin(
                    flow_id=1,
                    node_id=2,
                    depending_on_ids=[1, 1],
                    join_input=transform_schema.JoinInput(
                        join_mapping=[transform_schema.JoinMap("id", "id")],
                        left_select=[
                            transform_schema.SelectInput(
                                "id",
                                "left_id" if left_rename else "id",
                                keep=left_keep
                            ),
                            transform_schema.SelectInput(
                                "category",
                                "left_category" if left_rename else "category"
                            ),
                            transform_schema.SelectInput(
                                "value",
                                "left_value" if left_rename else "value",
                            )
                        ],
                        right_select=[
                            transform_schema.SelectInput(
                                "id",
                                "right_id" if right_rename else "id",
                                keep=right_keep
                            ),
                            transform_schema.SelectInput(
                                "category",
                                "right_category" if right_rename else "category"
                            ),
                            transform_schema.SelectInput(
                                "value",
                                "right_value" if right_rename else "value"
                            )
                        ],
                        how=join_type
                    )
                )

                test_cases.append((test_name, scenario))

    return test_cases


@pytest.fixture
def join_input_dataset() -> tuple[input_schema.NodeManualInput, input_schema.NodeManualInput]:
    left_data = input_schema.NodeManualInput(
        flow_id=1,
        node_id=1,
        raw_data_format=input_schema.RawData(
            columns=[
                input_schema.MinimalFieldInfo(name="id", data_type="Integer"),
                input_schema.MinimalFieldInfo(name="name", data_type="String")
            ],
            data=[[1, 2, 3], ["Alice", "Bob", "Charlie"]]
        )
    )

    # Add second dataset
    right_data = input_schema.NodeManualInput(
        flow_id=1,
        node_id=2,
        raw_data_format=input_schema.RawData(
            columns=[
                input_schema.MinimalFieldInfo(name="id", data_type="Integer"),
                input_schema.MinimalFieldInfo(name="city", data_type="String")
            ],
            data=[[1, 2, 4], ["NYC", "LA", "Chicago"]]
        )
    )
    return left_data, right_data


@pytest.fixture
def fuzzy_join_left_data() -> input_schema.NodeManualInput:
    return input_schema.NodeManualInput(
        flow_id=1,
        node_id=1,
        raw_data_format=input_schema.RawData(
            columns=[
                input_schema.MinimalFieldInfo(name="id", data_type="Integer"),
                input_schema.MinimalFieldInfo(name="name", data_type="String"),
                input_schema.MinimalFieldInfo(name="address", data_type="String"),
            ],
            data=[[1, 2, 3, 4, 5], ["Edward", "Eduward", "Edvard", "Charles", "Charlie"],
                  ["123 Main Str", "123 Main Street", "456 Elm Str", "789 Oak Str", "789 Oak Street"]]
        )
    )


@pytest.fixture
def fuzzy_join_right_data() -> input_schema.NodeManualInput:
    return input_schema.NodeManualInput(
        flow_id=1,
        node_id=2,
        raw_data_format=input_schema.RawData(
            columns=[
                input_schema.MinimalFieldInfo(name="first_name", data_type="String"),
                input_schema.MinimalFieldInfo(name="street", data_type="String"),
            ],
            data=[[1, 2, 3, 4, 5], ["Edward", "Eduward", "Edvard", "Charles", "Charlie"],
                  ["main street 123", "main street 123", "elm street 456", "oak street 789", "oak street 789"]]
        )
    )


@pytest.fixture
def join_input_large_dataset() -> tuple[input_schema.NodeManualInput, input_schema.NodeManualInput]:
    data_engine = FlowDataEngine.create_random(100)
    data_engine_2 = FlowDataEngine.create_random(10)
    left_data = input_schema.NodeManualInput(
        flow_id=1,
        node_id=1,
        raw_data_format=data_engine.select_columns(['ID', "Name", "Address", "Zipcode"]).to_raw_data()
    )
    right_data = input_schema.NodeManualInput(
        flow_id=1,
        node_id=2,
        raw_data_format=data_engine.get_sample(50, random=True).concat(data_engine_2).select_columns(["ID", "Name", "City"]).to_raw_data()
        )
    return left_data, right_data


@pytest.fixture
def join_input_same_df() -> input_schema.NodeManualInput:
    return input_schema.NodeManualInput(
        flow_id=1,
        node_id=1,
        raw_data_format=input_schema.RawData(
            columns=[input_schema.MinimalFieldInfo(name='id', data_type='Int64'),
                     input_schema.MinimalFieldInfo(name='category', data_type='String'),
                     input_schema.MinimalFieldInfo(name='value', data_type='Int64')],
            data=[[1, 2], ['A', 'B'], [100, 200]])

    )


def get_reference_polars_dataframe() -> pl.LazyFrame:
    return pl.LazyFrame([
        [1, 2, 3, 4, 5],
        ["Alice", "Bob", "Charlie", "David", "Eve"],
        [25, 30, 35, 40, 28],
        ["NYC", "LA", "Chicago", "Houston", "NYC"],
        [50000, 75000, 90000, 85000, 65000]
    ], schema=['id', 'name', 'age', 'city', 'salary'])


def create_sample_dataframe_node(flow: FlowGraph, node_id: int = 1) -> FlowGraph:
    """Add a standard sample dataframe to the flow"""
    manual_input = input_schema.NodeManualInput(
        flow_id=flow.flow_id,
        node_id=node_id,
        raw_data_format=input_schema.RawData(
            columns=[
                input_schema.MinimalFieldInfo(name="id", data_type="Integer"),
                input_schema.MinimalFieldInfo(name="name", data_type="String"),
                input_schema.MinimalFieldInfo(name="age", data_type="Integer"),
                input_schema.MinimalFieldInfo(name="city", data_type="String"),
                input_schema.MinimalFieldInfo(name="salary", data_type="Integer")
            ],
            data=[
                [1, 2, 3, 4, 5],
                ["Alice", "Bob", "Charlie", "David", "Eve"],
                [25, 30, 35, 40, 28],
                ["NYC", "LA", "Chicago", "Houston", "NYC"],
                [50000.0, 75000.0, 90000.0, 85000.0, 65000.0]
            ]
        )
    )
    flow.add_manual_input(manual_input)
    return flow


def create_sample_dataframe_for_graph_solver(flow: FlowGraph, node_id: int = 1) -> FlowGraph:
    input_data = input_schema.RawData.from_pylist([{'from': 'a', 'to': 'b'}, {'from': 'b', 'to': 'c'}, {'from': 'g', 'to': 'd'}])
    flow.add_manual_input(input_schema.NodeManualInput(flow_id=flow.flow_id, node_id=node_id, raw_data_format=input_data))
    return flow


def create_sales_dataframe_node(flow: FlowGraph, node_id: int = 1) -> FlowGraph:
    """Add a sales-focused dataframe to the flow"""
    sales_data = input_schema.NodeManualInput(
        flow_id=flow.flow_id,
        node_id=node_id,
        raw_data_format=input_schema.RawData(
            columns=[
                input_schema.MinimalFieldInfo(name="date", data_type="String"),
                input_schema.MinimalFieldInfo(name="product", data_type="String"),
                input_schema.MinimalFieldInfo(name="quantity", data_type="Integer"),
                input_schema.MinimalFieldInfo(name="price", data_type="Double"),
                input_schema.MinimalFieldInfo(name="region", data_type="String")
            ],
            data=[
                ["2024-01-01", "2024-01-01", "2024-01-02", "2024-01-02", "2024-01-03"],
                ["A", "B", "A", "B", "C"],
                [10, 20, 15, 25, 30],
                [100.0, 200.0, 100.0, 200.0, 150.0],
                ["North", "North", "South", "South", "East"]
            ]
        )
    )
    flow.add_manual_input(sales_data)
    return flow


def get_csv_df() -> pl.DataFrame:
    return pl.DataFrame({
        "name": ["Alice", "Bob", "Charlie", "David"],
        "age": [25, 30, 35, 40],
        "city": ["NYC", "LA", "Chicago", "Houston"]
    })


def create_csv_file_node(flow: FlowGraph, tmp_path: Path, node_id: int = 1,
                         df: pl.DataFrame = None, filename: str = "test_data.csv") -> FlowGraph:
    """Create a CSV file and add a read node to the flow"""
    if df is None:
        df = get_csv_df()
    csv_path = tmp_path / filename
    df.write_csv(csv_path)
    read_node = input_schema.NodeRead(
        flow_id=flow.flow_id,
        node_id=node_id,
        received_file=input_schema.ReceivedTable(
            name=filename,
            path=str(csv_path),
            file_type="csv",
            table_settings=input_schema.InputCsvTable(
                delimiter=",",
                has_headers=True,
                encoding="utf-8"
            )
        )
    )

    flow.add_read(read_node)
    return flow


def verify_code_contains(code: str, *snippets: str) -> None:
    """Verify that all snippets are present in the code"""
    for snippet in snippets:
        if snippet not in code:
            # Try with alternate quote style if it contains quotes
            if '"' in snippet and "'" in snippet:
                # Has mixed quotes, check as-is
                assert False, f"Expected '{snippet}' to be in generated code"
            elif '"' in snippet:
                # Try swapping double quotes for single quotes
                alt_snippet = snippet.replace('"', "'")
                if alt_snippet not in code:
                    assert False, f"Expected '{snippet}' (or '{alt_snippet}') to be in generated code"
            elif "'" in snippet:
                # Try swapping single quotes for double quotes
                alt_snippet = snippet.replace("'", '"')
                if alt_snippet not in code:
                    assert False, f"Expected '{snippet}' (or '{alt_snippet}') to be in generated code"
            else:
                assert False, f"Expected '{snippet}' to be in generated code"


def verify_code_ordering(code: str, *ordered_snippets: str) -> None:
    """Verify that snippets appear in the correct order in the code"""
    lines = code.split('\n')
    indices = []

    for snippet in ordered_snippets:
        found_index = -1
        for i, line in enumerate(lines):
            if snippet in line:
                found_index = i
                break
        assert found_index != -1, f"Could not find '{snippet}' in code"
        indices.append(found_index)

    for i in range(1, len(indices)):
        assert indices[i - 1] < indices[i], f"'{ordered_snippets[i - 1]}' should appear before '{ordered_snippets[i]}'"


@pytest.mark.parametrize("export_func", [
    export_flow_to_polars,
    export_flow_to_flowframe,
], ids=["polars", "flowframe"])
@pytest.mark.parametrize("test_name,join_scenario",  generate_parameterized_join_tests())
def test_join_operation(test_name, join_scenario, join_input_dataset, export_func):
    """Parameterized test for all join operation combinations"""
    flow = create_basic_flow()
    left_data, right_data = join_input_dataset
    flow.add_manual_input(left_data)
    flow.add_manual_input(right_data)
    # Add join node
    flow.add_join(join_scenario)
    # Add connections
    left_connection = input_schema.NodeConnection.create_from_simple_input(1, 3, 'main')
    right_connection = input_schema.NodeConnection.create_from_simple_input(2, 3, 'right')
    add_connection(flow, left_connection)
    add_connection(flow, right_connection)

    # Convert to code and verify
    code = export_func(flow)
    verify_if_execute(code)
    result = normalize_result(get_result_from_generated_code(code))
    expected_df = normalize_result(flow.get_node(3).get_resulting_data().data_frame)
    assert_frame_equal(result, expected_df, check_column_order=False, check_row_order=False)


@pytest.mark.parametrize("export_func", [
    export_flow_to_polars,
    export_flow_to_flowframe,
], ids=["polars", "flowframe"])
@pytest.mark.parametrize("test_name,join_scenario",  generate_parameterized_join_tests_same_df())
def test_join_operation_same_df(test_name, join_scenario, join_input_same_df, export_func):
    """Parameterized test for all join operation combinations"""
    flow = create_basic_flow()
    flow.add_manual_input(join_input_same_df)
    # # Add join node
    flow.add_join(join_scenario)

    # Add connections
    left_connection = input_schema.NodeConnection.create_from_simple_input(1, 2, 'main')
    right_connection = input_schema.NodeConnection.create_from_simple_input(1, 2, 'right')
    add_connection(flow, left_connection)
    add_connection(flow, right_connection)
    # Convert to code and verify
    code = export_func(flow)
    from flowfile_core.schemas.input_schema import RawData
    import flowfile as ff

    verify_if_execute(code)
    result = normalize_result(get_result_from_generated_code(code))
    expected_df = normalize_result(flow.get_node(2).get_resulting_data().data_frame)
    assert_frame_equal(result, expected_df, check_column_order=False, check_row_order=False)


@pytest.mark.parametrize("export_func", [export_flow_to_polars, export_flow_to_flowframe], ids=["polars", "flowframe"])
def test_simple_manual_input(export_func):
    flow = create_basic_flow()
    flow = create_sample_dataframe_node(flow)
    code = export_func(flow)
    verify_if_execute(code)
    result = normalize_result(get_result_from_generated_code(code))
    expected = normalize_result(get_reference_polars_dataframe())
    assert_frame_equal(result, expected)


# Test functions
@pytest.mark.parametrize("export_func", [export_flow_to_polars, export_flow_to_flowframe], ids=["polars", "flowframe"])
def test_simple_csv_read_and_filter(tmp_path, export_func):
    """Test converting a simple CSV read and filter flow"""
    flow = create_basic_flow()
    flow = create_csv_file_node(flow, tmp_path)
    # Add filter node
    filter_node = input_schema.NodeFilter(
        flow_id=1,
        node_id=2,
        depending_on_id=1,
        filter_input=transform_schema.FilterInput(
            filter_type="advanced",
            advanced_filter="[age]>30"
        )
    )
    flow.add_filter(filter_node)
    add_connection(flow, node_connection=input_schema.NodeConnection.create_from_simple_input(1, 2))
    # Convert to code
    code = export_func(flow)

    verify_if_execute(code)
    result_df = normalize_result(get_result_from_generated_code(code))
    expected_df = normalize_result(flow.get_node(2).get_resulting_data().data_frame)
    assert_frame_equal(result_df, expected_df)


@pytest.mark.parametrize("export_func", [export_flow_to_polars, export_flow_to_flowframe], ids=["polars", "flowframe"])
def test_manual_input_with_select(export_func):
    """Test manual data input with column selection and renaming"""
    flow = create_basic_flow()
    flow = create_sample_dataframe_node(flow)
    select_node = input_schema.NodeSelect(
        flow_id=1,
        node_id=2,
        depending_on_id=1,
        select_input=[
            transform_schema.SelectInput("name", "full_name", keep=True),
            transform_schema.SelectInput("age", "age", keep=True),
            transform_schema.SelectInput("city", "city", keep=False),
            transform_schema.SelectInput("salary", "salary", keep=True, data_type="Float64", is_altered=True)
        ]
    )
    flow.add_select(select_node)
    add_connection(flow, node_connection=input_schema.NodeConnection.create_from_simple_input(1, 2))
    code = export_func(flow)
    if export_func is export_flow_to_polars:
        verify_code_contains(code,
                             'pl.col("name").alias("full_name")',
                             'pl.col("age")',
                             'pl.col("salary").cast(pl.Float64)'
                             )
        assert 'pl.col("city")' not in code.split("df_2 = ")[1].split("\n")[0]
    verify_if_execute(code)
    result = normalize_result(get_result_from_generated_code(code))
    expected_result = normalize_result(get_reference_polars_dataframe()
                       .select(pl.col('name').alias("full_name"), "age", pl.col("salary").cast(pl.Float64))
                       )
    assert_frame_equal(result, expected_result)


@pytest.mark.parametrize("export_func", [export_flow_to_polars, export_flow_to_flowframe], ids=["polars", "flowframe"])
def test_number_of_records(export_func):
    """Test manual data input with column selection and renaming"""
    flow = create_basic_flow()
    flow = create_sample_dataframe_node(flow)
    record_count_node = input_schema.NodeRecordCount(flow_id=1, node_id=2, depending_on_id=1)

    flow.add_record_count(record_count_node)
    add_connection(flow, node_connection=input_schema.NodeConnection.create_from_simple_input(1, 2))
    # Convert to code
    code = export_func(flow)

    verify_if_execute(code)
    result = normalize_result(get_result_from_generated_code(code))
    expected_result = normalize_result(flow.get_node(2).get_resulting_data().data_frame)
    assert_frame_equal(result, expected_result)


@pytest.mark.parametrize("export_func", [
    export_flow_to_polars,
    export_flow_to_flowframe,
], ids=["polars", "flowframe"])
def test_graph_solver(export_func):
    flow = create_basic_flow()
    create_sample_dataframe_for_graph_solver(flow)
    graph_solver_input = transform_schema.GraphSolverInput(col_from='from', col_to='to', output_column_name='g')
    flow.add_graph_solver(input_schema.NodeGraphSolver(flow_id=1, node_id=2, graph_solver_input=graph_solver_input))
    add_connection(flow, node_connection=input_schema.NodeConnection.create_from_simple_input(1, 2))
    code = export_func(flow)

    verify_if_execute(code)
    result = normalize_result(get_result_from_generated_code(code))
    expected_result = normalize_result(flow.get_node(2).get_resulting_data().data_frame)
    assert_frame_equal(result, expected_result)


@pytest.mark.parametrize("export_func", [
    export_flow_to_polars,
    export_flow_to_flowframe,
], ids=["polars", "flowframe"])
def test_join_operation_left(join_input_dataset, export_func):
    """Test join operation between two datasets"""
    flow = create_basic_flow()
    left_data, right_data = join_input_dataset
    flow.add_manual_input(left_data)
    flow.add_manual_input(right_data)

    # Add join node
    join_node = input_schema.NodeJoin(
        flow_id=1,
        node_id=3,
        depending_on_ids=[1, 2],
        join_input=transform_schema.JoinInput(
            join_mapping=[transform_schema.JoinMap("id", "id")],
            left_select=[transform_schema.SelectInput("id"), transform_schema.SelectInput("name")],
            right_select=[transform_schema.SelectInput("id"), transform_schema.SelectInput("city")],
            how="left"
        )
    )
    flow.add_join(join_node)
    left_connection = input_schema.NodeConnection.create_from_simple_input(1, 3, 'main')
    right_connection = input_schema.NodeConnection.create_from_simple_input(2, 3, 'right')
    add_connection(flow, left_connection)
    add_connection(flow, right_connection)

    # Convert to code
    code = export_func(flow)
    # Verify join code (polars-specific)
    if export_func is export_flow_to_polars:
        verify_code_contains(code,
                             "df_1.join(",
                             "df_2,",
                             'left_on=["id"]',
                             'right_on=["id"]',
                             'how="left"'
                             )
    verify_if_execute(code)
    result = normalize_result(get_result_from_generated_code(code))
    expected_df = normalize_result(flow.get_node(3).get_resulting_data().data_frame)
    assert_frame_equal(result, expected_df)


@pytest.mark.parametrize("export_func", [
    export_flow_to_polars,
    export_flow_to_flowframe,
], ids=["polars", "flowframe"])
def test_join_operation_right(join_input_dataset, export_func):
    """Test join operation between two datasets"""
    flow = create_basic_flow()
    left_data, right_data = join_input_dataset

    flow.add_manual_input(left_data)
    flow.add_manual_input(right_data)
    # Add join node
    join_node = input_schema.NodeJoin(
        flow_id=1,
        node_id=3,
        depending_on_ids=[1, 2],
        join_input=transform_schema.JoinInput(
            join_mapping=[transform_schema.JoinMap("id", "id")],
            left_select=[transform_schema.SelectInput("id"), transform_schema.SelectInput("name")],
            right_select=[transform_schema.SelectInput("id"), transform_schema.SelectInput("city")],
            how="right"
        )
    )
    flow.add_join(join_node)
    left_connection = input_schema.NodeConnection.create_from_simple_input(1, 3, 'main')
    right_connection = input_schema.NodeConnection.create_from_simple_input(2, 3, 'right')
    add_connection(flow, left_connection)
    add_connection(flow, right_connection)

    # Convert to code
    code = export_func(flow)
    # Verify join code (polars-specific)
    if export_func is export_flow_to_polars:
        verify_code_contains(code,
                             "df_1.join(",
                             "df_2,",
                             'left_on=["__jk_id"]',
                             'right_on=["id_right"]',
                             'how="right"'
                             )
    verify_if_execute(code)
    result = normalize_result(get_result_from_generated_code(code))
    expected_df = normalize_result(flow.get_node(3).get_resulting_data().data_frame)
    assert_frame_equal(result, expected_df, check_column_order=False, check_row_order=False)


def create_comprehensive_join_scenarios() -> list[tuple[str, input_schema.NodeJoin]]:
    """Generate comprehensive join test scenarios including complex cases"""

    join_scenarios: list[tuple[str, input_schema.NodeJoin]] = []
    how_options: list[transform_schema.JoinStrategy] = ["left", "right", "inner", "outer"]

    def add_scenario(name: str, join_input: transform_schema.JoinInput) -> None:
        join_scenarios.append((
            name,
            input_schema.NodeJoin(
                flow_id=1,
                node_id=3,
                depending_on_ids=[1, 2],
                join_input=join_input
            )
        ))

    # Test data columns for reference:
    # Left: ID, Name, Address, Zipcode
    # Right: ID, Name, City

    for how in how_options:
        # 1. Basic single column join
        add_scenario(
            f"{how}_basic_single_join",
            transform_schema.JoinInput(
                join_mapping=[transform_schema.JoinMap("ID", "ID")],
                left_select=[
                    transform_schema.SelectInput("ID"),
                    transform_schema.SelectInput("Address"),
                    transform_schema.SelectInput("Zipcode"),
                    transform_schema.SelectInput("Name")
                ],
                right_select=[
                    transform_schema.SelectInput("ID"),
                    transform_schema.SelectInput("City"),
                    transform_schema.SelectInput("Name")
                ],
                how=how
            )
        )
        # 2. Multi-column join with unselected right overlapping columns
        add_scenario(
            f"{how}_unselect_all_right_overlapping_col_left",
            transform_schema.JoinInput(
                join_mapping=[transform_schema.JoinMap("ID", "ID")],
                left_select=[
                    transform_schema.SelectInput("ID"),
                    transform_schema.SelectInput("Address"),
                    transform_schema.SelectInput("Zipcode"),
                    transform_schema.SelectInput("Name", keep=False)
                ],
                right_select=[
                    transform_schema.SelectInput("ID", keep=False),
                    transform_schema.SelectInput("City", keep=False),
                    transform_schema.SelectInput("Name", keep=False)
                ],
                how=how
            )
        )

        # 3. Multi-column join with all right columns dropped
        add_scenario(
            f"{how}_unselect_all_right_overlapping_col_left_multi_join",
            transform_schema.JoinInput(
                join_mapping=[
                    transform_schema.JoinMap("ID", "ID"),
                    transform_schema.JoinMap("Name", "Name")
                ],
                left_select=[
                    transform_schema.SelectInput("ID"),
                    transform_schema.SelectInput("Address"),
                    transform_schema.SelectInput("Zipcode"),
                    transform_schema.SelectInput("Name", keep=False)
                ],
                right_select=[
                    transform_schema.SelectInput("ID", keep=False),
                    transform_schema.SelectInput("City", keep=False),
                    transform_schema.SelectInput("Name", keep=False)
                ],
                how=how
            )
        )

        # 4. Rename left overlapping column
        add_scenario(
            f"{how}_unselect_all_right_overlapping_col_left_rename",
            transform_schema.JoinInput(
                join_mapping=[
                    transform_schema.JoinMap("ID", "ID"),
                    transform_schema.JoinMap("Name", "Name")
                ],
                left_select=[
                    transform_schema.SelectInput("ID"),
                    transform_schema.SelectInput("Address"),
                    transform_schema.SelectInput("Zipcode"),
                    transform_schema.SelectInput("Name", new_name="LeftName", keep=True)
                ],
                right_select=[
                    transform_schema.SelectInput("ID", keep=False),
                    transform_schema.SelectInput("City", keep=False),
                    transform_schema.SelectInput("Name", keep=False)
                ],
                how=how
            )
        )

        # 5. Rename right overlapping column
        add_scenario(
            f"{how}_rename_right_overlapping_col",
            transform_schema.JoinInput(
                join_mapping=[transform_schema.JoinMap("ID", "ID")],
                left_select=[
                    transform_schema.SelectInput("ID"),
                    transform_schema.SelectInput("Address"),
                    transform_schema.SelectInput("Zipcode"),
                    transform_schema.SelectInput("Name")
                ],
                right_select=[
                    transform_schema.SelectInput("ID", new_name="RightID"),
                    transform_schema.SelectInput("City"),
                    transform_schema.SelectInput("Name", new_name="RightName")
                ],
                how=how
            )
        )

        # 6. Rename both overlapping columns
        add_scenario(
            f"{how}_rename_both_overlapping_cols",
            transform_schema.JoinInput(
                join_mapping=[transform_schema.JoinMap("ID", "ID")],
                left_select=[
                    transform_schema.SelectInput("ID", new_name="LeftID"),
                    transform_schema.SelectInput("Address"),
                    transform_schema.SelectInput("Zipcode"),
                    transform_schema.SelectInput("Name", new_name="LeftName")
                ],
                right_select=[
                    transform_schema.SelectInput("ID", new_name="RightID"),
                    transform_schema.SelectInput("City"),
                    transform_schema.SelectInput("Name", new_name="RightName")
                ],
                how=how
            )
        )

        # 7. Keep only join columns
        add_scenario(
            f"{how}_keep_only_join_cols",
            transform_schema.JoinInput(
                join_mapping=[transform_schema.JoinMap("ID", "ID")],
                left_select=[
                    transform_schema.SelectInput("ID"),
                    transform_schema.SelectInput("Address", keep=False),
                    transform_schema.SelectInput("Zipcode", keep=False),
                    transform_schema.SelectInput("Name", keep=False)
                ],
                right_select=[
                    transform_schema.SelectInput("ID", keep=False),
                    transform_schema.SelectInput("City", keep=False),
                    transform_schema.SelectInput("Name", keep=False)
                ],
                how=how
            )
        )

        # 8. Drop all join columns
        add_scenario(
            f"{how}_drop_all_join_cols",
            transform_schema.JoinInput(
                join_mapping=[transform_schema.JoinMap("ID", "ID")],
                left_select=[
                    transform_schema.SelectInput("ID", keep=False),
                    transform_schema.SelectInput("Address"),
                    transform_schema.SelectInput("Zipcode"),
                    transform_schema.SelectInput("Name")
                ],
                right_select=[
                    transform_schema.SelectInput("ID", keep=False),
                    transform_schema.SelectInput("City"),
                    transform_schema.SelectInput("Name", new_name="RightName")
                ],
                how=how
            )
        )

        # 9. Complex multi-join with mixed strategies
        add_scenario(
            f"{how}_complex_multi_join_mixed_strategy",
            transform_schema.JoinInput(
                join_mapping=[
                    transform_schema.JoinMap("ID", "ID"),
                    transform_schema.JoinMap("Name", "Name")
                ],
                left_select=[
                    transform_schema.SelectInput("ID", new_name="JoinID"),
                    transform_schema.SelectInput("Address"),
                    transform_schema.SelectInput("Zipcode", keep=False),
                    transform_schema.SelectInput("Name", new_name="JoinName")
                ],
                right_select=[
                    transform_schema.SelectInput("ID", keep=False),
                    transform_schema.SelectInput("City", new_name="RightCity"),
                    transform_schema.SelectInput("Name", keep=False)
                ],
                how=how
            )
        )

        # 10. Minimal output (only non-join columns)
        add_scenario(
            f"{how}_minimal_output_non_join_cols",
            transform_schema.JoinInput(
                join_mapping=[transform_schema.JoinMap("ID", "ID")],
                left_select=[
                    transform_schema.SelectInput("ID", keep=False),
                    transform_schema.SelectInput("Address"),
                    transform_schema.SelectInput("Zipcode", keep=False),
                    transform_schema.SelectInput("Name", keep=False)
                ],
                right_select=[
                    transform_schema.SelectInput("ID", keep=False),
                    transform_schema.SelectInput("City"),
                    transform_schema.SelectInput("Name", keep=False)
                ],
                how=how
            )
        )

        add_scenario(
            f"{how}_complex_multi_join_mixed_rename_left_strategy",
            transform_schema.JoinInput(
                join_mapping=[
                    transform_schema.JoinMap("ID", "ID"),
                    transform_schema.JoinMap("Name", "Name")
                ],
                left_select=[
                    transform_schema.SelectInput("ID"),
                    transform_schema.SelectInput("Address"),
                    transform_schema.SelectInput("Zipcode", keep=False),
                    transform_schema.SelectInput("Name", new_name="JoinName")
                ],
                right_select=[
                    transform_schema.SelectInput("ID", keep=False),
                    transform_schema.SelectInput("City", new_name="RightCity"),
                    transform_schema.SelectInput("Name", keep=False)
                ],
                how=how
            )
        )

        add_scenario(
            f"{how}_complex_multi_join_mixed_rename_right_strategy",
            transform_schema.JoinInput(
                join_mapping=[
                    transform_schema.JoinMap("ID", "ID"),
                    transform_schema.JoinMap("Name", "Name")
                ],
                left_select=[
                    transform_schema.SelectInput("ID"),
                    transform_schema.SelectInput("Address"),
                    transform_schema.SelectInput("Zipcode", keep=False),
                    transform_schema.SelectInput("Name", new_name="JoinName")
                ],
                right_select=[
                    transform_schema.SelectInput("ID", keep=False),
                    transform_schema.SelectInput("City", new_name="RightCity"),
                    transform_schema.SelectInput("Name", new_name="new_name", keep=False)
                ],
                how=how
            )
        )

        # 12. All columns renamed
        add_scenario(
            f"{how}_all_columns_renamed",
            transform_schema.JoinInput(
                join_mapping=[transform_schema.JoinMap("ID", "ID")],
                left_select=[
                    transform_schema.SelectInput("ID", new_name="L_ID"),
                    transform_schema.SelectInput("Address", new_name="L_Address"),
                    transform_schema.SelectInput("Zipcode", new_name="L_Zipcode"),
                    transform_schema.SelectInput("Name", new_name="L_Name")
                ],
                right_select=[
                    transform_schema.SelectInput("ID", new_name="R_ID"),
                    transform_schema.SelectInput("City", new_name="R_City"),
                    transform_schema.SelectInput("Name", new_name="R_Name")
                ],
                how=how
            )
        )

    return join_scenarios


def create_comprehensive_anti_semi_join_scenarios() -> list[tuple[str, input_schema.NodeJoin]]:
    """Generate comprehensive join test scenarios including complex cases"""

    join_scenarios: list[tuple[str, input_schema.NodeJoin]] = []
    how_options: list[transform_schema.JoinStrategy] = ["semi", "anti"]

    def add_scenario(name: str, join_input: transform_schema.JoinInput) -> None:
        join_scenarios.append((
            name,
            input_schema.NodeJoin(
                flow_id=1,
                node_id=3,
                depending_on_ids=[1, 2],
                join_input=join_input
            )
        ))


    for how in how_options:
        # 1. Basic single column join
        add_scenario(
            f"{how}_basic_single_join",
            transform_schema.JoinInput(
                join_mapping=[transform_schema.JoinMap("ID", "ID")],
                left_select=[
                    transform_schema.SelectInput("ID"),
                    transform_schema.SelectInput("Address"),
                    transform_schema.SelectInput("Zipcode"),
                    transform_schema.SelectInput("Name")
                ],
                right_select=[
                    transform_schema.SelectInput("ID"),
                    transform_schema.SelectInput("City"),
                    transform_schema.SelectInput("Name")
                ],
                how=how
            )
        )

        # 3. Multi-column join with all right columns dropped
        add_scenario(
            f"{how}_unselect_all_right_overlapping_col_left_multi_join",
            transform_schema.JoinInput(
                join_mapping=[
                    transform_schema.JoinMap("ID", "ID"),
                    transform_schema.JoinMap("Name", "Name")
                ],
                left_select=[
                    transform_schema.SelectInput("ID"),
                    transform_schema.SelectInput("Address"),
                    transform_schema.SelectInput("Zipcode"),
                    transform_schema.SelectInput("Name")
                ],
                right_select=[
                    transform_schema.SelectInput("ID"),
                    transform_schema.SelectInput("City"),
                    transform_schema.SelectInput("Name")
                ],
                how=how
            )
        )

    return join_scenarios



@pytest.mark.parametrize("export_func", [
    export_flow_to_polars,
    export_flow_to_flowframe,
], ids=["polars", "flowframe"])
@pytest.mark.parametrize("test_name, join_scenario", create_comprehensive_join_scenarios())
def test_join_operation_complex(test_name, join_scenario, join_input_large_dataset, export_func):
    flow = create_basic_flow()
    left_data, right_data = join_input_large_dataset
    flow.add_manual_input(left_data)
    flow.add_manual_input(right_data)
    flow.add_join(join_scenario)
    left_connection = input_schema.NodeConnection.create_from_simple_input(1, 3, 'main')
    right_connection = input_schema.NodeConnection.create_from_simple_input(2, 3, 'right')
    add_connection(flow, left_connection)
    add_connection(flow, right_connection)

    # Convert to code
    code = export_func(flow)
    result = normalize_result(get_result_from_generated_code(code))
    expected_df = normalize_result(flow.get_node(3).get_resulting_data().data_frame)
    assert_frame_equal(result, expected_df, check_column_order=False, check_row_order=False)


@pytest.mark.parametrize("export_func", [export_flow_to_polars, export_flow_to_flowframe], ids=["polars", "flowframe"])
@pytest.mark.parametrize("test_name, join_scenario", create_comprehensive_anti_semi_join_scenarios())
def test_semi_and_anti_join(test_name, join_scenario, join_input_large_dataset, export_func):
    flow = create_basic_flow()
    left_data, right_data = join_input_large_dataset
    flow.add_manual_input(left_data)
    flow.add_manual_input(right_data)
    flow.add_join(join_scenario)
    left_connection = input_schema.NodeConnection.create_from_simple_input(1, 3, 'main')
    right_connection = input_schema.NodeConnection.create_from_simple_input(2, 3, 'right')
    add_connection(flow, left_connection)
    add_connection(flow, right_connection)

    # Convert to code
    code = export_func(flow)
    result = normalize_result(get_result_from_generated_code(code))
    expected_df = normalize_result(flow.get_node(3).get_resulting_data().data_frame)
    assert_frame_equal(result, expected_df, check_column_order=False, check_row_order=False)


@pytest.mark.parametrize("export_func", [export_flow_to_polars, export_flow_to_flowframe], ids=["polars", "flowframe"])
def test_join_operation_left_rename(join_input_dataset, export_func):
    """Test join operation between two datasets"""
    flow = create_basic_flow()
    left_data, right_data = join_input_dataset
    flow.add_manual_input(left_data)
    flow.add_manual_input(right_data)
    # Add join node
    join_node = input_schema.NodeJoin(
        flow_id=1,
        node_id=3,
        depending_on_ids=[1, 2],
        join_input=transform_schema.JoinInput(
            join_mapping=[transform_schema.JoinMap("id", "id")],
            left_select=[transform_schema.SelectInput("id", "left_id"),
                         transform_schema.SelectInput("name", "left_name")],
            right_select=[transform_schema.SelectInput("id", "right_id"),
                          transform_schema.SelectInput("city", "right_city")],
            how="left"
        )
    )
    flow.add_join(join_node)
    left_connection = input_schema.NodeConnection.create_from_simple_input(1, 3, 'main')
    right_connection = input_schema.NodeConnection.create_from_simple_input(2, 3, 'right')
    add_connection(flow, left_connection)
    add_connection(flow, right_connection)

    # Convert to code
    code = export_func(flow)

    verify_if_execute(code)
    result = normalize_result(get_result_from_generated_code(code))
    expected_df = normalize_result(flow.get_node(3).get_resulting_data().data_frame)
    assert_frame_equal(result, expected_df, check_column_order=False, check_row_order=False)


@pytest.mark.parametrize("export_func", [export_flow_to_polars, export_flow_to_flowframe], ids=["polars", "flowframe"])
def test_group_by_aggregation(export_func):
    """Test group by with multiple aggregations"""
    flow = create_basic_flow()
    flow = create_sales_dataframe_node(flow)

    # Add group by node
    groupby_node = input_schema.NodeGroupBy(
        flow_id=1,
        node_id=2,
        depending_on_id=1,
        groupby_input=transform_schema.GroupByInput(
            agg_cols=[
                transform_schema.AggColl("product", "groupby"),
                transform_schema.AggColl("region", "groupby"),
                transform_schema.AggColl("quantity", "sum", "total_quantity"),
                transform_schema.AggColl("price", "mean", "avg_price"),
                transform_schema.AggColl("quantity", "count", "num_transactions")
            ]
        )
    )
    flow.add_group_by(groupby_node)
    add_connection(flow, node_connection=input_schema.NodeConnection.create_from_simple_input(1, 2))

    code = export_func(flow)
    if export_func is export_flow_to_polars:
        verify_code_contains(code,
                             'group_by(["product", "region"])',
                             'pl.col("quantity").sum().alias("total_quantity")',
                             'pl.col("price").mean().alias("avg_price")',
                             'pl.col("quantity").count().alias("num_transactions")'
                             )
    verify_if_execute(code)
    result_df = normalize_result(get_result_from_generated_code(code))
    expected_df = normalize_result(flow.get_node(2).get_resulting_data().data_frame)
    assert_frame_equal(result_df, expected_df, check_row_order=False)


@pytest.mark.parametrize("export_func", [
    export_flow_to_polars,
    export_flow_to_flowframe,
], ids=["polars", "flowframe"])
def test_formula_node_cast(export_func):
    """Test formula/expression node"""
    flow = create_basic_flow()
    flow = create_sales_dataframe_node(flow)
    # Add formula node

    formula_node = input_schema.NodeFormula(
        flow_id=1,
        node_id=2,
        depending_on_id=1,
        function=transform_schema.FunctionInput(
            field=transform_schema.FieldInput(name="total", data_type="Integer"),
            function="[price] * [quantity]"
        )
    )
    flow.add_formula(formula_node)
    add_connection(flow, node_connection=input_schema.NodeConnection.create_from_simple_input(1, 2))

    # Convert to code
    code = export_func(flow)
    # Verify formula code
    if export_func is export_flow_to_polars:
        verify_code_contains(code,
                             "with_columns",
                             "df_2 = df_1.with_columns(",
                             'alias("total")',
                             'cast(pl.Int64)'
                             )
    verify_if_execute(code)
    result_df = normalize_result(get_result_from_generated_code(code))
    expected_df = normalize_result(flow.get_node(2).get_resulting_data().data_frame)
    assert_frame_equal(result_df, expected_df)


@pytest.mark.parametrize("export_func", [
    export_flow_to_polars,
    export_flow_to_flowframe,
], ids=["polars", "flowframe"])
def test_non_convertable_formula_node_cast(export_func):
    # test to validate that if code can not be converted to polars code it still works by falling back on
    # to the original code
    flow = create_basic_flow()
    flow = create_sales_dataframe_node(flow)
    flow.get_node(1).get_resulting_data().collect()
    formula_node = input_schema.NodeFormula(
        flow_id=1,
        node_id=2,
        depending_on_id=1,
        function=transform_schema.FunctionInput(
            field=transform_schema.FieldInput(name="total", data_type="Double"),
            function="string_similarity([region], 'Noorden')"
        )
    )
    flow.add_formula(formula_node)
    add_connection(flow, node_connection=input_schema.NodeConnection.create_from_simple_input(1, 2))
    code = export_func(flow)
    if export_func is export_flow_to_polars:
        verify_code_contains(code,
                             "with_columns",
                             "df_2 = df_1.with_columns(",
                             'alias("total")',
                             "simple_function_to_expr"
                             )
    verify_if_execute(code)
    result_df = normalize_result(get_result_from_generated_code(code))
    expected_df = normalize_result(flow.get_node(2).get_resulting_data().data_frame)
    assert_frame_equal(result_df, expected_df)

@pytest.mark.parametrize("export_func", [
    export_flow_to_polars,
    export_flow_to_flowframe,
], ids=["polars", "flowframe"])
def test_formula_node(export_func):
    """Test formula/expression node"""
    flow = create_basic_flow()
    flow = create_sales_dataframe_node(flow)
    # Add formula node
    formula_node = input_schema.NodeFormula(
        flow_id=1,
        node_id=2,
        depending_on_id=1,
        function=transform_schema.FunctionInput(
            field=transform_schema.FieldInput(name="total", data_type="Auto"),
            function="[price] * [quantity]"
        )
    )
    flow.add_formula(formula_node)
    add_connection(flow, node_connection=input_schema.NodeConnection.create_from_simple_input(1, 2))

    # Convert to code
    code = export_func(flow)
    # Verify formula code (polars-specific)
    if export_func is export_flow_to_polars:
        verify_code_contains(code,
                             "with_columns",
                             "df_2 = df_1.with_columns(",
                             'alias("total")',
                             )
    verify_if_execute(code)
    result_df = normalize_result(get_result_from_generated_code(code))
    expected_df = normalize_result(flow.get_node(2).get_resulting_data().data_frame)
    assert_frame_equal(result_df, expected_df)


@pytest.mark.parametrize("export_func", [export_flow_to_polars, export_flow_to_flowframe], ids=["polars", "flowframe"])
def test_pivot_operation(export_func):
    """Test pivot operation"""
    flow = create_basic_flow()
    flow = create_sales_dataframe_node(flow)

    pivot_node = input_schema.NodePivot(
        flow_id=1,
        node_id=2,
        depending_on_id=1,
        pivot_input=transform_schema.PivotInput(
            index_columns=["date"],
            pivot_column="product",
            value_col="quantity",
            aggregations=["sum"]
        )
    )
    flow.add_pivot(pivot_node)
    add_connection(flow, node_connection=input_schema.NodeConnection.create_from_simple_input(1, 2))

    # Convert to code
    code = export_func(flow)

    # Verify pivot code (polars-specific)
    if export_func is export_flow_to_polars:
        verify_code_contains(code,
                             "pivot(",
                             "values='quantity'",
                             'index=["date"]',
                             "on='product'",
                             "aggregate_function='sum'"
                             )
    verify_if_execute(code)
    result_df = normalize_result(get_result_from_generated_code(code))
    expected_df = normalize_result(flow.get_node(2).get_resulting_data().data_frame)
    assert_frame_equal(result_df, expected_df, check_row_order=False)


@pytest.mark.parametrize("export_func", [export_flow_to_polars, export_flow_to_flowframe], ids=["polars", "flowframe"])
def test_pivot_no_index_operation(export_func):
    """Test pivot operation"""
    flow = create_basic_flow()
    flow = create_sales_dataframe_node(flow)
    # Add pivot node
    pivot_node = input_schema.NodePivot(
        flow_id=1,
        node_id=2,
        depending_on_id=1,
        pivot_input=transform_schema.PivotInput(
            index_columns=[],
            pivot_column="product",
            value_col="quantity",
            aggregations=["sum"]
        )
    )
    flow.add_pivot(pivot_node)
    add_connection(flow, node_connection=input_schema.NodeConnection.create_from_simple_input(1, 2))
    # Convert to code
    code = export_func(flow)
    # Verify pivot code (polars-specific)
    if export_func is export_flow_to_polars:
        verify_code_contains(code,
                             "pivot(",
                             "values='quantity'",
                             'index=["_temp_index_"]',
                             "on='product'",
                             "aggregate_function='sum'"
                             )
    verify_if_execute(code)
    result_df = normalize_result(get_result_from_generated_code(code))
    expected_df = normalize_result(flow.get_node(2).get_resulting_data().data_frame)
    assert_frame_equal(result_df, expected_df, check_row_order=False)


@pytest.mark.parametrize("export_func", [export_flow_to_polars, export_flow_to_flowframe], ids=["polars", "flowframe"])
def test_union_multiple_dataframes(export_func):
    """Test union of multiple dataframes"""
    flow = create_basic_flow()
    # Add three manual inputs with same structure
    for i in range(1, 4):
        data = input_schema.NodeManualInput(
            flow_id=1,
            node_id=i,
            raw_data_format=input_schema.RawData(
                columns=[
                    input_schema.MinimalFieldInfo(name="id", data_type="Integer"),
                    input_schema.MinimalFieldInfo(name="value", data_type="String")
                ],
                data=[[i, i + 10, i + 20], [f"A{i}", f"B{i}", f"C{i}"]]
            )
        )
        flow.add_manual_input(data)

    # Add union node
    union_node = input_schema.NodeUnion(
        flow_id=1,
        node_id=4,
        depending_on_ids=[1, 2, 3],
        union_input=transform_schema.UnionInput(mode="relaxed")
    )
    flow.add_union(union_node)
    for i in range(1, 4):
        connection = input_schema.NodeConnection.create_from_simple_input(i, 4, 'main')
        add_connection(flow, connection)

    code = export_func(flow)
    if export_func is export_flow_to_polars:
        verify_code_contains(code,
                             "pl.concat([",
                             "df_1,",
                             "df_2,",
                             "df_3,",
                             "how='diagonal_relaxed'"
                             )
    verify_if_execute(code)
    result_df = normalize_result(get_result_from_generated_code(code))
    expected_df = normalize_result(flow.get_node(4).get_resulting_data().data_frame)
    assert_frame_equal(expected_df, result_df, check_row_order=False)


@pytest.mark.parametrize("export_func", [
    export_flow_to_polars,
    pytest.param(export_flow_to_flowframe, marks=pytest.mark.xfail(reason="FlowFrame code gen: custom polars_code nodes use pl.LazyFrame which is not available via ff")),
], ids=["polars", "flowframe"])
def test_custom_polars_code(export_func):
    """Test custom Polars code node with single input"""
    flow = create_basic_flow()
    flow = create_sample_dataframe_node(flow)
    # Add custom Polars code node
    polars_code_node = input_schema.NodePolarsCode(
        flow_id=1,
        node_id=2,
        depending_on_ids=[1],
        polars_code_input=transform_schema.PolarsCodeInput(
            polars_code="input_df.with_columns((pl.col('age') * 2).alias('double_age'))"
        )
    )
    flow.add_polars_code(polars_code_node)
    add_connection(flow, node_connection=input_schema.NodeConnection.create_from_simple_input(1, 2))

    # Convert to code
    code = export_func(flow)

    # Verify custom code handling
    if export_func is export_flow_to_polars:
        verify_code_contains(code,
                             "def _polars_code_2(input_df: pl.LazyFrame):",
                             "return input_df.with_columns((pl.col('age') * 2).alias('double_age'))",
                             "df_2 = _polars_code_2(df_1)"
                             )
    verify_if_execute(code)
    result_df = normalize_result(get_result_from_generated_code(code))
    expected_df = normalize_result(flow.get_node(2).get_resulting_data().data_frame)
    assert_frame_equal(expected_df, result_df, check_row_order=False)


polars_test_cases = [
    (
        "input_df.filter(pl.col('salary') >= 50001)",
        "filter_greater_than"
    ),
    (
        "output_df = input_df.filter(pl.col('salary') >= 50001)",
        "filter_greater_than_with_output"
    ),
    (
        "input_df.select(['city', 'name'])",
        "select_columns"
    ),
    (
        "output_df = input_df.select(['city', 'name'])",
        "select_columns"
    ),
    (
        "input_df.with_columns((pl.col('salary') * 1.1).alias('value_plus_10_percent'))",
        "with_columns"
    ),
    (
        "output_df = input_df.group_by('city').agg(pl.col('salary').sum().alias('total_value'))",
        "group_by_and_aggregate"
    ),
    (
        "# A multi-line example with comments\n"
        "temp_df = input_df.sort('salary', descending=True)\noutput_df = temp_df.limit(3)",
        "sort_and_limit_with"
    )
]


@pytest.mark.parametrize("export_func", [
    export_flow_to_polars,
    pytest.param(export_flow_to_flowframe, marks=pytest.mark.xfail(reason="FlowFrame code gen: custom polars_code nodes use pl.LazyFrame which is not available via ff")),
], ids=["polars", "flowframe"])
@pytest.mark.parametrize("polars_code, test_id", polars_test_cases)
def test_code(polars_code, test_id, export_func):
    """Test custom Polars code node with single input when the polars code only contains input_df"""
    flow = create_basic_flow()
    flow = create_sample_dataframe_node(flow)
    # Add custom Polars code node
    polars_code_node = input_schema.NodePolarsCode(
        flow_id=1,
        node_id=2,
        depending_on_ids=[1],
        polars_code_input=transform_schema.PolarsCodeInput(
            polars_code=polars_code
        )
    )
    flow.add_polars_code(polars_code_node)
    add_connection(flow, node_connection=input_schema.NodeConnection.create_from_simple_input(1, 2))

    # Convert to code
    code = export_func(flow)

    verify_if_execute(code)
    result_df = normalize_result(get_result_from_generated_code(code))
    expected_df = normalize_result(flow.get_node(2).get_resulting_data().data_frame)
    assert_frame_equal(expected_df, result_df, check_row_order=False)


@pytest.mark.parametrize("export_func", [
    export_flow_to_polars,
    export_flow_to_flowframe,
], ids=["polars", "flowframe"])
def test_formula_with_string(export_func):
    """Test custom Polars code node with single input"""
    flow = create_basic_flow()
    flow = create_sample_dataframe_node(flow)
    # Add custom Polars code node
    formula_node = input_schema.NodeFormula(
        flow_id=1,
        node_id=2,
        depending_on_id=1,
        function=transform_schema.FunctionInput(
            field=transform_schema.FieldInput(name="total", data_type="String"),
            function='"This is a string"'
        )
    )
    flow.add_formula(formula_node)
    add_connection(flow, node_connection=input_schema.NodeConnection.create_from_simple_input(1, 2))

    # Convert to code
    code = export_func(flow)
    verify_if_execute(code)
    result_df = normalize_result(get_result_from_generated_code(code))
    expected_df = normalize_result(flow.get_node(2).get_resulting_data().data_frame)
    assert_frame_equal(expected_df, result_df, check_row_order=False)


@pytest.mark.parametrize("export_func", [
    export_flow_to_polars,
    pytest.param(export_flow_to_flowframe, marks=pytest.mark.xfail(reason="FlowFrame code gen: custom polars_code nodes use pl.LazyFrame which is not available via ff")),
], ids=["polars", "flowframe"])
def test_custom_polars_code_multiple_inputs(export_func):
    """Test custom Polars code node with multiple inputs"""
    flow = create_basic_flow()

    for i in range(1, 3):
        data = input_schema.NodeManualInput(
            flow_id=1,
            node_id=i,
            raw_data_format=input_schema.RawData(
                columns=[input_schema.MinimalFieldInfo(name=f"col{i}", data_type="Integer")],
                data=[[1, 2, 3]]
            )
        )
        flow.add_manual_input(data)

    # Add custom code that uses both inputs
    polars_code_node = input_schema.NodePolarsCode(
        flow_id=1,
        node_id=3,
        depending_on_ids=[1, 2],
        polars_code_input=transform_schema.PolarsCodeInput(
            polars_code="output_df = input_df_1.join(input_df_2, how='cross')\nreturn output_df"
        )
    )
    flow.add_polars_code(polars_code_node)
    for i in [1, 2]:
        add_connection(flow, input_schema.NodeConnection.create_from_simple_input(i, 3))
    # Convert to code
    code = export_func(flow)

    # Verify custom code handling
    if export_func is export_flow_to_polars:
        verify_code_contains(code,
                             "def _polars_code_3(input_df_1: pl.LazyFrame, input_df_2: pl.LazyFrame):",
                             "output_df = input_df_1.join(input_df_2, how='cross')",
                             "return output_df",
                             "df_3 = _polars_code_3(df_1, df_2)"
                             )
    verify_if_execute(code)
    result_df = normalize_result(get_result_from_generated_code(code))
    expected_df = normalize_result(flow.get_node(3).get_resulting_data().data_frame)
    assert_frame_equal(expected_df, result_df, check_row_order=False)


@pytest.mark.parametrize("export_func", [
    export_flow_to_polars,
    pytest.param(export_flow_to_flowframe, marks=pytest.mark.xfail(reason="FlowFrame code gen: custom polars_code nodes use pl.LazyFrame which is not available via ff")),
], ids=["polars", "flowframe"])
def test_custom_polars_no_inputs(export_func):
    """Test custom Polars code node with multiple inputs"""
    flow = create_basic_flow()

    # Add custom code that uses both inputs
    polars_code_node = input_schema.NodePolarsCode(
        flow_id=1,
        node_id=1,
        depending_on_ids=[],
        polars_code_input=transform_schema.PolarsCodeInput(
            polars_code="output_df = pl.LazyFrame({'a': [1,2,3]})\nreturn output_df"
        )
    )
    flow.add_polars_code(polars_code_node)
    # Convert to code
    code = export_func(flow)

    verify_if_execute(code)
    result_df = normalize_result(get_result_from_generated_code(code))
    expected_df = normalize_result(flow.get_node(1).get_resulting_data().data_frame)
    assert_frame_equal(expected_df, result_df, check_row_order=False)


@pytest.mark.parametrize("export_func", [
    export_flow_to_polars,
    export_flow_to_flowframe,
], ids=["polars", "flowframe"])
def test_complex_workflow(tmp_path, export_func):
    """Test a complex workflow with multiple operations"""
    flow = create_basic_flow()
    flow = create_csv_file_node(flow, tmp_path,
                                df=pl.DataFrame({
                                    "date": ["2024-01-01", "2024-01-01", "2024-01-02", "2024-01-02"],
                                    "product": ["A", "B", "A", "B"],
                                    "quantity": [10, 20, 15, 25],
                                    "price": [100.0, 200.0, 100.0, 200.0]
                                }),
                                filename="sales_data.csv"
                                )

    # 2. Add formula for total
    formula_node = input_schema.NodeFormula(
        flow_id=1,
        node_id=2,
        depending_on_id=1,
        function=transform_schema.FunctionInput(
            field=transform_schema.FieldInput(name="total", data_type="Double"),
            function="[quantity] * [price]"
        )
    )
    flow.add_formula(formula_node)
    add_connection(flow, node_connection=input_schema.NodeConnection.create_from_simple_input(1, 2))

    # 3. Filter high value transactions
    filter_node = input_schema.NodeFilter(
        flow_id=1,
        node_id=3,
        depending_on_id=2,
        filter_input=transform_schema.FilterInput(
            filter_type="advanced",
            advanced_filter="[total]>1500"
        )
    )
    flow.add_filter(filter_node)
    add_connection(flow, node_connection=input_schema.NodeConnection.create_from_simple_input(2, 3))
    # 4. Group by product
    group_by_node = input_schema.NodeGroupBy(
        flow_id=1,
        node_id=4,
        depending_on_id=3,
        groupby_input=transform_schema.GroupByInput(
            agg_cols=[
                transform_schema.AggColl("product", "groupby"),
                transform_schema.AggColl("total", "sum", "total_revenue"),
                transform_schema.AggColl("quantity", "sum", "total_quantity")
            ]
        )
    )
    flow.add_group_by(group_by_node)
    add_connection(flow, node_connection=input_schema.NodeConnection.create_from_simple_input(3, 4))
    output_node = input_schema.NodeOutput(
        flow_id=1,
        node_id=5,
        depending_on_id=4,
        output_settings=input_schema.OutputSettings(
            name="output.parquet",
            directory=str(tmp_path),
            file_type="parquet",
            table_settings=input_schema.OutputParquetTable()
        )
    )
    flow.add_output(output_node)
    add_connection(flow, node_connection=input_schema.NodeConnection.create_from_simple_input(4, 5))
    # Convert to code
    code = export_func(flow)

    # Verify the complete workflow is represented
    if export_func is export_flow_to_polars:
        verify_code_contains(code,
                             "pl.scan_csv(",
                             "with_columns",
                             "filter(",
                             "group_by",
                             "sink_parquet"
                             )

        # Verify proper sequencing
        verify_code_ordering(code,
                             "df_1 = ",
                             "df_2 = ",
                             "df_3 = ",
                             "df_4 = ",
                             "sink_parquet"
                             )
    verify_if_execute(code)
    result_df = normalize_result(get_result_from_generated_code(code))
    expected_df = normalize_result(flow.get_node(4).get_resulting_data().data_frame)
    assert_frame_equal(result_df, expected_df, check_row_order=False)


@pytest.mark.parametrize("export_func", [
    export_flow_to_polars,
    export_flow_to_flowframe,
], ids=["polars", "flowframe"])
def test_complex_workflow_unordered(tmp_path, export_func):
    """Test a complex workflow with multiple operations"""
    flow = create_basic_flow()
    flow = create_csv_file_node(flow, tmp_path,
                                df=pl.DataFrame({
                                    "date": ["2024-01-01", "2024-01-01", "2024-01-02", "2024-01-02"],
                                    "product": ["A", "B", "A", "B"],
                                    "quantity": [10, 20, 15, 25],
                                    "price": [100.0, 200.0, 100.0, 200.0]
                                }),
                                filename="sales_data.csv"
                                )

    # 4. Group by product
    group_by_node = input_schema.NodeGroupBy(
        flow_id=1,
        node_id=4,
        groupby_input=transform_schema.GroupByInput(
            agg_cols=[
                transform_schema.AggColl("product", "groupby"),
                transform_schema.AggColl("total", "sum", "total_revenue"),
                transform_schema.AggColl("quantity", "sum", "total_quantity")
            ]
        )
    )
    flow.add_group_by(group_by_node)
    output_node = input_schema.NodeOutput(
        flow_id=1,
        node_id=5,
        depending_on_id=4,
        output_settings=input_schema.OutputSettings(
            name="output.parquet",
            directory=str(tmp_path),
            file_type="parquet",
            table_settings=input_schema.OutputParquetTable()
        )
    )
    flow.add_output(output_node)
    # Convert to code

    formula_node = input_schema.NodeFormula(
        flow_id=1,
        node_id=2,
        function=transform_schema.FunctionInput(
            field=transform_schema.FieldInput(name="total", data_type="Double"),
            function="[quantity] * [price]"
        )
    )
    flow.add_formula(formula_node)

    # 3. Filter high value transactions
    filter_node = input_schema.NodeFilter(
        flow_id=1,
        node_id=3,
        filter_input=transform_schema.FilterInput(
            filter_type="advanced",
            advanced_filter="[total]>1500"
        )
    )
    flow.add_filter(filter_node)

    add_connection(flow, node_connection=input_schema.NodeConnection.create_from_simple_input(4, 5))
    add_connection(flow, node_connection=input_schema.NodeConnection.create_from_simple_input(3, 4))
    add_connection(flow, node_connection=input_schema.NodeConnection.create_from_simple_input(2, 3))
    add_connection(flow, node_connection=input_schema.NodeConnection.create_from_simple_input(1, 2))

    code = export_func(flow)
    # Verify the complete workflow is represented
    if export_func is export_flow_to_polars:
        verify_code_contains(code,
                             "pl.scan_csv(",
                             "with_columns",
                             "filter(",
                             "group_by",
                             "sink_parquet"
                             )

        # Verify proper sequencing
        verify_code_ordering(code,
                             "df_1 = ",
                             "df_2 = ",
                             "df_3 = ",
                             "df_4 = ",
                             "sink_parquet"
                             )
    verify_if_execute(code)
    result_df = normalize_result(get_result_from_generated_code(code))
    expected_df = normalize_result(flow.get_node(4).get_resulting_data().data_frame)
    assert_frame_equal(result_df, expected_df, check_row_order=False)


@pytest.mark.parametrize("export_func", [
    export_flow_to_polars,
    export_flow_to_flowframe,
], ids=["polars", "flowframe"])
def test_text_to_rows_operation(export_func):
    """Test text to rows (explode) operation"""
    flow = create_basic_flow()
    # Add manual input with comma-separated values
    data = input_schema.NodeManualInput(
        flow_id=1,
        node_id=1,
        raw_data_format=input_schema.RawData(
            columns=[
                input_schema.MinimalFieldInfo(name="id", data_type="Integer"),
                input_schema.MinimalFieldInfo(name="tags", data_type="String")
            ],
            data=[[1, 2, 3], ["python,data,analysis", "machine,learning", "etl,pipeline,flowfile"]]
        )
    )
    flow.add_manual_input(data)

    # Add text to rows node
    text_to_rows_node = input_schema.NodeTextToRows(
        flow_id=1,
        node_id=2,
        depending_on_id=1,
        text_to_rows_input=transform_schema.TextToRowsInput(
            column_to_split="tags",
            output_column_name="tag",
            split_fixed_value=","
        )
    )
    flow.add_text_to_rows(text_to_rows_node)
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2))
    # Convert to code
    code = export_func(flow)

    # Verify text to rows code (polars-specific)
    if export_func is export_flow_to_polars:
        verify_code_contains(code,
                             'str.split(",")',
                             'alias("tag")',
                             "explode('tag')"
                             )
    verify_if_execute(code)
    result_df = normalize_result(get_result_from_generated_code(code))
    expected_df = normalize_result(flow.get_node(2).get_resulting_data().data_frame)
    assert_frame_equal(result_df, expected_df)


@pytest.mark.parametrize("export_func", [
    export_flow_to_polars,
    export_flow_to_flowframe,
], ids=["polars", "flowframe"])
def test_text_to_rows_operation_no_rename(export_func):
    """Test text to rows (explode) operation"""
    flow = create_basic_flow()

    # Add manual input with comma-separated values
    data = input_schema.NodeManualInput(
        flow_id=1,
        node_id=1,
        raw_data_format=input_schema.RawData(
            columns=[
                input_schema.MinimalFieldInfo(name="id", data_type="Integer"),
                input_schema.MinimalFieldInfo(name="tags", data_type="String")
            ],
            data=[[1, 2, 3], ["python,data,analysis", "machine,learning", "etl,pipeline,flowfile"]]
        )
    )
    flow.add_manual_input(data)

    # Add text to rows node
    text_to_rows_node = input_schema.NodeTextToRows(
        flow_id=1,
        node_id=2,
        depending_on_id=1,
        text_to_rows_input=transform_schema.TextToRowsInput(
            column_to_split="tags",
            split_fixed_value=","
        )
    )
    flow.add_text_to_rows(text_to_rows_node)
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2))
    # Convert to code
    code = export_func(flow)

    # Verify text to rows code (polars-specific)
    if export_func is export_flow_to_polars:
        verify_code_contains(code,
                             'str.split(",")',
                             "explode('tags')"
                             )
    verify_if_execute(code)
    result_df = normalize_result(get_result_from_generated_code(code))
    expected_df = normalize_result(flow.get_node(2).get_resulting_data().data_frame)
    assert_frame_equal(result_df, expected_df)


@pytest.mark.parametrize("export_func", [export_flow_to_polars, export_flow_to_flowframe], ids=["polars", "flowframe"])
def test_sort_operation(export_func):
    """Test sort and unique operations"""
    flow = create_basic_flow()
    # Add manual input with duplicate data
    data = input_schema.NodeManualInput(
        flow_id=1,
        node_id=1,
        raw_data_format=input_schema.RawData(
            columns=[
                input_schema.MinimalFieldInfo(name="name", data_type="String"),
                input_schema.MinimalFieldInfo(name="score", data_type="Integer")
            ],
            data=[["Alice", "Bob", "Alice", "Charlie", "Bob"], [85, 90, 85, 75, 95]]
        )
    )
    flow.add_manual_input(data)

    # Add sort node
    sort_node = input_schema.NodeSort(
        flow_id=1,
        node_id=2,
        depending_on_id=1,
        sort_input=[
            transform_schema.SortByInput(column="score", how="desc"),
            transform_schema.SortByInput(column="name", how="asc")
        ]
    )
    flow.add_sort(sort_node)
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2))

    # Convert to code
    code = export_func(flow)

    # Verify sort code (polars-specific)
    if export_func is export_flow_to_polars:
        verify_code_contains(code,
                             'sort(["score", "name"], descending=[True, False])',
                             )
    verify_if_execute(code)
    result_df = normalize_result(get_result_from_generated_code(code))
    expected_df = normalize_result(flow.get_node(2).get_resulting_data().data_frame)
    assert_frame_equal(result_df, expected_df)


@pytest.mark.parametrize("export_func", [export_flow_to_polars, export_flow_to_flowframe], ids=["polars", "flowframe"])
def test_sort_and_unique_operations(export_func):
    """Test sort and unique operations"""
    flow = create_basic_flow()
    # Add manual input with duplicate data
    data = input_schema.NodeManualInput(
        flow_id=1,
        node_id=1,
        raw_data_format=input_schema.RawData(
            columns=[
                input_schema.MinimalFieldInfo(name="name", data_type="String"),
                input_schema.MinimalFieldInfo(name="score", data_type="Integer")
            ],
            data=[["Alice", "Bob", "Alice", "Charlie", "Bob"], [85, 90, 85, 75, 95]]
        )
    )
    flow.add_manual_input(data)

    # Add sort node
    sort_node = input_schema.NodeSort(
        flow_id=1,
        node_id=2,
        depending_on_id=1,
        sort_input=[
            transform_schema.SortByInput(column="score", how="desc"),
            transform_schema.SortByInput(column="name", how="asc")
        ]
    )
    flow.add_sort(sort_node)
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2))

    # Add unique node
    unique_node = input_schema.NodeUnique(
        flow_id=1,
        node_id=3,
        depending_on_id=2,
        unique_input=transform_schema.UniqueInput(
            columns=["name"],
            strategy="first"
        )
    )
    flow.add_unique(unique_node)
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(2, 3))

    # Convert to code
    code = export_func(flow)

    # Verify sort and unique code (polars-specific)
    if export_func is export_flow_to_polars:
        verify_code_contains(code,
                             'sort(["score", "name"], descending=[True, False])',
                             "unique(subset=['name'], keep='first')"
                             )
    verify_if_execute(code)
    result_df = normalize_result(get_result_from_generated_code(code))
    expected_df = normalize_result(flow.get_node(3).get_resulting_data().data_frame)
    assert_frame_equal(result_df, expected_df, check_row_order=False)


@pytest.mark.parametrize("export_func", [export_flow_to_polars, export_flow_to_flowframe], ids=["polars", "flowframe"])
def test_record_id_generation_with_grouping(export_func):
    """Test record ID generation with grouping"""
    flow = create_basic_flow()

    # Add manual input
    data = input_schema.NodeManualInput(
        flow_id=1,
        node_id=1,
        raw_data_format=input_schema.RawData(
            columns=[
                input_schema.MinimalFieldInfo(name="category", data_type="String"),
                input_schema.MinimalFieldInfo(name="value", data_type="Integer")
            ],
            data=[["A", "B", "A", "B", "A"], [10, 20, 30, 40, 50]]
        )
    )
    flow.add_manual_input(data)

    # Add record ID node with grouping
    record_id_node = input_schema.NodeRecordId(
        flow_id=1,
        node_id=2,
        depending_on_id=1,
        record_id_input=transform_schema.RecordIdInput(
            output_column_name="row_num",
            offset=1,
            group_by=True,
            group_by_columns=["category"]
        )
    )
    flow.add_record_id(record_id_node)
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2))

    # Convert to code
    code = export_func(flow)

    verify_if_execute(code)
    result_df = normalize_result(get_result_from_generated_code(code))
    expected_df = normalize_result(flow.get_node(2).get_resulting_data().data_frame)
    assert_frame_equal(result_df, expected_df, check_row_order=False)


@pytest.mark.parametrize("export_func", [export_flow_to_polars, export_flow_to_flowframe], ids=["polars", "flowframe"])
def test_record_id_generation_without_grouping(export_func):
    """Test record ID generation without grouping"""
    flow = create_basic_flow()
    # Add manual input
    data = input_schema.NodeManualInput(
        flow_id=1,
        node_id=1,
        raw_data_format=input_schema.RawData(
            columns=[
                input_schema.MinimalFieldInfo(name="category", data_type="String"),
                input_schema.MinimalFieldInfo(name="value", data_type="Integer")
            ],
            data=[["A", "B", "A", "B", "A"], [10, 20, 30, 40, 50]]
        )
    )
    flow.add_manual_input(data)

    # Add record ID node with grouping
    record_id_node = input_schema.NodeRecordId(
        flow_id=1,
        node_id=2,
        depending_on_id=1,
        record_id_input=transform_schema.RecordIdInput(
            output_column_name="row_num",
            offset=1,
        )
    )
    flow.add_record_id(record_id_node)
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2))

    # Convert to code
    code = export_func(flow)

    verify_if_execute(code)
    result_df = normalize_result(get_result_from_generated_code(code))
    expected_df = normalize_result(flow.get_node(2).get_resulting_data().data_frame)
    assert_frame_equal(result_df, expected_df, check_row_order=False)


@pytest.mark.parametrize("export_func", [export_flow_to_polars, export_flow_to_flowframe], ids=["polars", "flowframe"])
def test_sample_operation(export_func):
    """Test sample operation"""
    flow = create_basic_flow()
    flow = create_sample_dataframe_node(flow)

    # Add sample node
    sample_node = input_schema.NodeSample(
        flow_id=1,
        node_id=2,
        depending_on_id=1,
        sample_size=3
    )
    flow.add_sample(sample_node)
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2))

    # Convert to code
    code = export_func(flow)

    # Verify sample code (polars-specific)
    if export_func is export_flow_to_polars:
        verify_code_contains(code, "head(n=3)")
    verify_if_execute(code)
    result_df = normalize_result(get_result_from_generated_code(code))
    expected_df = normalize_result(flow.get_node(2).get_resulting_data().data_frame)
    assert_frame_equal(result_df, expected_df, check_row_order=False)


def test_empty_flow():
    """Test converting an empty flow"""
    flow = create_basic_flow()

    # Convert empty flow
    code = export_flow_to_polars(flow)

    # Should still generate valid Python structure
    verify_code_contains(code,
                         "import polars as pl",
                         "def run_etl_pipeline():",
                         'if __name__ == "__main__":'
                         )


@pytest.mark.parametrize("export_func", [export_flow_to_polars, export_flow_to_flowframe], ids=["polars", "flowframe"])
def test_cross_join_operation(export_func):
    """Test cross join with multiple inputs"""
    flow = create_basic_flow()

    # Add two manual inputs
    data1 = input_schema.NodeManualInput(
        flow_id=1,
        node_id=1,
        raw_data_format=input_schema.RawData(
            columns=[input_schema.MinimalFieldInfo(name="x", data_type="Integer")],
            data=[[1, 2, 3]]
        )
    )
    flow.add_manual_input(data1)

    data2 = input_schema.NodeManualInput(
        flow_id=1,
        node_id=2,
        raw_data_format=input_schema.RawData(
            columns=[input_schema.MinimalFieldInfo(name="y", data_type="String")],
            data=[["A", "B"]]
        )
    )
    flow.add_manual_input(data2)
    # Add cross join
    cross_join_node = input_schema.NodeCrossJoin(
        flow_id=1,
        node_id=3,
        depending_on_ids=[1, 2],
        cross_join_input=transform_schema.CrossJoinInput(
            left_select=[transform_schema.SelectInput("x")],
            right_select=[transform_schema.SelectInput("y")]
        )
    )
    flow.add_cross_join(cross_join_node)
    left_connection = input_schema.NodeConnection.create_from_simple_input(1, 3, 'main')
    right_connection = input_schema.NodeConnection.create_from_simple_input(2, 3, 'right')
    add_connection(flow, left_connection)
    add_connection(flow, right_connection)
    # Convert to code
    code = export_func(flow)

    # Verify cross join code (polars-specific)
    if export_func is export_flow_to_polars:
        verify_code_contains(code,
                             "join(",
                             "how='cross'"
                             )
    verify_if_execute(code)
    result_df = normalize_result(get_result_from_generated_code(code))
    expected_df = normalize_result(flow.get_node(3).get_resulting_data().data_frame)
    assert_frame_equal(result_df, expected_df, check_row_order=False)


@pytest.mark.parametrize("export_func", [export_flow_to_polars, export_flow_to_flowframe], ids=["polars", "flowframe"])
def test_cross_join_operation_equal_column(export_func):
    """Test cross join with multiple inputs"""
    flow = create_basic_flow()

    # Add two manual inputs
    data1 = input_schema.NodeManualInput(
        flow_id=1,
        node_id=1,
        raw_data_format=input_schema.RawData(
            columns=[input_schema.MinimalFieldInfo(name="x", data_type="Integer")],
            data=[[1, 2, 3]]
        )
    )
    flow.add_manual_input(data1)

    data2 = input_schema.NodeManualInput(
        flow_id=1,
        node_id=2,
        raw_data_format=input_schema.RawData(
            columns=[input_schema.MinimalFieldInfo(name="x", data_type="String")],
            data=[["A", "B"]]
        )
    )
    flow.add_manual_input(data2)
    # Add cross join
    cross_join_node = input_schema.NodeCrossJoin(
        flow_id=1,
        node_id=3,
        depending_on_ids=[1, 2],
        cross_join_input=transform_schema.CrossJoinInput(
            left_select=[transform_schema.SelectInput("x")],
            right_select=[transform_schema.SelectInput("x")]
        )
    )
    flow.add_cross_join(cross_join_node)
    left_connection = input_schema.NodeConnection.create_from_simple_input(1, 3, 'main')
    right_connection = input_schema.NodeConnection.create_from_simple_input(2, 3, 'right')
    add_connection(flow, left_connection)
    add_connection(flow, right_connection)
    # Convert to code
    code = export_func(flow)

    # Verify cross join code (polars-specific)
    if export_func is export_flow_to_polars:
        verify_code_contains(code,
                             "join(",
                             "how='cross'"
                             )
    verify_if_execute(code)
    result_df = normalize_result(get_result_from_generated_code(code))
    expected_df = normalize_result(flow.get_node(3).get_resulting_data().data_frame)
    assert_frame_equal(result_df, expected_df, check_row_order=False)


@pytest.mark.parametrize("export_func", [export_flow_to_polars, export_flow_to_flowframe], ids=["polars", "flowframe"])
def test_unpivot_operation(export_func):
    """Test unpivot/melt operation"""
    flow = create_basic_flow()

    # Add manual input with wide format data
    data = input_schema.NodeManualInput(
        flow_id=1,
        node_id=1,
        raw_data_format=input_schema.RawData(
            columns=[
                input_schema.MinimalFieldInfo(name="id", data_type="Integer"),
                input_schema.MinimalFieldInfo(name="jan", data_type="Double"),
                input_schema.MinimalFieldInfo(name="feb", data_type="Double"),
                input_schema.MinimalFieldInfo(name="mar", data_type="Double")
            ],
            data=[[1, 2], [100.0, 150.0], [110.0, 160.0], [120.0, 170.0]]
        )
    )
    flow.add_manual_input(data)

    # Add unpivot node
    unpivot_node = input_schema.NodeUnpivot(
        flow_id=1,
        node_id=2,
        depending_on_id=1,
        unpivot_input=transform_schema.UnpivotInput(
            index_columns=["id"],
            value_columns=["jan", "feb", "mar"]
        )
    )
    flow.add_unpivot(unpivot_node)
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2))
    # Convert to code
    code = export_func(flow)

    # Verify unpivot code (polars-specific)
    if export_func is export_flow_to_polars:
        verify_code_contains(code,
                             "unpivot(",
                             'index=["id"]',
                             'on=["jan", "feb", "mar"]',
                             "variable_name='variable'",
                             "value_name='value'"
                             )
    verify_if_execute(code)
    result_df = normalize_result(get_result_from_generated_code(code))
    expected_df = normalize_result(flow.get_node(2).get_resulting_data().data_frame)
    assert_frame_equal(result_df, expected_df, check_row_order=False)


@pytest.mark.parametrize("export_func", [
    export_flow_to_polars,
    export_flow_to_flowframe,
], ids=["polars", "flowframe"])
def test_multiple_output_formats(tmp_path, export_func):
    """Test different output formats"""
    flow = create_basic_flow()
    flow = create_sample_dataframe_node(flow)
    # Test CSV output
    csv_output = input_schema.NodeOutput(
        flow_id=1,
        node_id=2,
        depending_on_id=1,
        output_settings=input_schema.OutputSettings(
            name="output.csv",
            directory=str(tmp_path),
            file_type="csv",
            table_settings=input_schema.OutputCsvTable(delimiter="|")
        )
    )
    flow.add_output(csv_output)
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2))

    code = export_func(flow)
    if export_func is export_flow_to_polars:
        verify_code_contains(code,
                             "sink_csv(",
                             'separator="|"'
                             )

    excel_output = input_schema.NodeOutput(
        flow_id=1,
        node_id=3,
        depending_on_id=1,
        output_settings=input_schema.OutputSettings(
            name="output.xlsx",
            directory=str(tmp_path),
            file_type="excel",
            table_settings=input_schema.OutputExcelTable(sheet_name="Results")
        )
    )
    flow.add_output(excel_output)
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 3))

    parquet_output = input_schema.NodeOutput(
        flow_id=1,
        node_id=4,
        depending_on_id=1,
        output_settings=input_schema.OutputSettings(
            name="output.parquet",
            directory=str(tmp_path),
            file_type="parquet",
            table_settings=input_schema.OutputParquetTable()
        )
    )
    flow.add_output(parquet_output)
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 4))
    code = export_func(flow)
    verify_if_execute(code)
    if export_func is export_flow_to_polars:
        try:
            pl.read_csv(str(tmp_path) + os.sep + "output.csv", separator="|")
        except Exception:
            raise Exception("Could not read the CSV file that should have been written")
        try:
            pl.read_excel(str(tmp_path) + os.sep + "output.xlsx", sheet_name='Results')
        except Exception:
            raise Exception("Could not read the xlsx file that should have been written")
        try:
            pl.read_parquet(str(tmp_path) + os.sep + "output.parquet")
        except Exception:
            raise Exception("Could not read the parquet file that should have been written")


def test_node_with_no_handler():
    """Test behavior when encountering a node type with no handler"""
    flow = create_basic_flow()

    # Add a promise node (which should be skipped)
    promise_node = input_schema.NodePromise(
        flow_id=1,
        node_id=1,
        node_type="manual_input"
    )
    flow.add_node_promise(promise_node)

    # Convert to Polars code
    code = export_flow_to_polars(flow)
    verify_if_execute(code)


@pytest.mark.parametrize("export_func", [export_flow_to_polars, export_flow_to_flowframe], ids=["polars", "flowframe"])
def test_data_type_conversions(export_func):
    """Test data type conversions in select nodes"""
    flow = create_basic_flow()
    flow = create_sample_dataframe_node(flow)

    # Add select with type conversions
    select_node = input_schema.NodeSelect(
        flow_id=1,
        node_id=2,
        depending_on_id=1,
        select_input=[
            transform_schema.SelectInput("age", "age_float", keep=True, data_type="Float64", data_type_change=True),
            transform_schema.SelectInput("salary", "salary_int", keep=True, data_type="Int32", data_type_change=True),
            transform_schema.SelectInput("id", "id_str", keep=True, data_type="String", data_type_change=True),
        ],
        keep_missing=False
    )
    flow.add_select(select_node)
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2))
    code = export_func(flow)
    if export_func is export_flow_to_polars:
        verify_code_contains(code,
                             'pl.col("age").alias("age_float").cast(pl.Float64)',
                             'pl.col("salary").alias("salary_int").cast(pl.Int32)',
                             'pl.col("id").alias("id_str").cast(pl.Utf8)'
                             )
    verify_if_execute(code)
    result_df = normalize_result(get_result_from_generated_code(code))
    expected_df = normalize_result(flow.get_node(2).get_resulting_data().data_frame)
    assert_frame_equal(result_df, expected_df)


@pytest.mark.parametrize("export_func", [export_flow_to_polars, export_flow_to_flowframe], ids=["polars", "flowframe"])
def test_csv_read_utf_8(export_func):
    """Test reading parquet files"""
    flow = create_basic_flow()
    flowfile_core_path = find_parent_directory('Flowfile')

    file_path = str(Path(flowfile_core_path) / 'flowfile_core' / 'tests' / 'support_files' / 'data' / 'fake_data.csv')
    # Add parquet read node
    read_node = input_schema.NodeRead(
        flow_id=1,
        node_id=1,
        received_file=input_schema.ReceivedTable(
            name="fake_data.csv",
            path=file_path,
            file_type="csv",
            table_settings=input_schema.InputCsvTable()
        )
    )

    flow.add_read(read_node)
    flow.get_node(1).get_resulting_data()
    # Convert to code
    code = export_func(flow)
    verify_if_execute(code)
    result = normalize_result(get_result_from_generated_code(code))
    assert len(result) > 0


@pytest.mark.parametrize("export_func", [export_flow_to_polars, export_flow_to_flowframe], ids=["polars", "flowframe"])
def test_parquet_read(export_func):
    """Test reading parquet files"""
    flow = create_basic_flow()
    flowfile_core_path = find_parent_directory('Flowfile')
    file_path = str(
        (
                Path(flowfile_core_path) / 'flowfile_core' / 'tests' / 'support_files' / 'data' / 'fake_data.parquet'
         ).absolute()
    )
    # Add parquet read node
    read_node = input_schema.NodeRead(
        flow_id=1,
        node_id=1,
        received_file=input_schema.ReceivedTable(
            name="fake_data.parquet",
            path=file_path,
            file_type="parquet",
            table_settings=input_schema.InputParquetTable()
        )
    )

    flow.add_read(read_node)
    flow.get_node(1).get_resulting_data()
    # Convert to code
    code = export_func(flow)
    verify_if_execute(code)
    result = normalize_result(get_result_from_generated_code(code))
    assert len(result) > 0


@pytest.mark.parametrize("export_func", [
    export_flow_to_polars,
    pytest.param(export_flow_to_flowframe, marks=pytest.mark.xfail(reason="FlowFrame code gen: ff.read_excel does not exist yet")),
], ids=["polars", "flowframe"])
def test_excel_read(export_func):
    """Test reading Excel files"""
    flow = create_basic_flow()
    flowfile_core_path = find_parent_directory('Flowfile')

    file_path = str(
        Path(flowfile_core_path) / 'flowfile_core' / 'tests' / 'support_files' / 'data' / 'fake_data.xlsx')
    # Add Excel read node
    read_node = input_schema.NodeRead(
        flow_id=1,
        node_id=1,
        received_file=input_schema.ReceivedTable(
            name="data.xlsx",
            path=file_path,
            file_type="excel",
            table_settings=input_schema.InputExcelTable(sheet_name="Sheet1")
        )
    )
    flow.add_read(read_node)

    # Convert to code
    code = export_func(flow)

    # Verify Excel read
    if export_func is export_flow_to_polars:
        verify_code_contains(code,
                             "pl.read_excel(",
                             'sheet_name="Sheet1"'
                             )
    verify_if_execute(code)
    result = normalize_result(get_result_from_generated_code(code))
    assert len(result) > 0


@pytest.mark.parametrize("export_func", [export_flow_to_polars, export_flow_to_flowframe], ids=["polars", "flowframe"])
def test_aggregation_functions(export_func):
    """Test various aggregation functions in group by"""
    flow = create_basic_flow()
    flow = create_sales_dataframe_node(flow)

    # Add group by with various aggregations
    groupby_node = input_schema.NodeGroupBy(
        flow_id=1,
        node_id=2,
        depending_on_id=1,
        groupby_input=transform_schema.GroupByInput(
            agg_cols=[
                transform_schema.AggColl("product", "groupby"),
                transform_schema.AggColl("quantity", "min", "min_qty"),
                transform_schema.AggColl("quantity", "max", "max_qty"),
                transform_schema.AggColl("price", "std", "price_std"),
                transform_schema.AggColl("price", "var", "price_var"),
                transform_schema.AggColl("product", "n_unique", "unique_products"),
                transform_schema.AggColl("region", "first", "first_region"),
                transform_schema.AggColl("region", "last", "last_region"),
            ]
        )
    )
    flow.add_group_by(groupby_node)
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2))
    code = export_func(flow)
    if export_func is export_flow_to_polars:
        verify_code_contains(code,
                             'pl.col("quantity").min().alias("min_qty")',
                             'pl.col("quantity").max().alias("max_qty")',
                             'pl.col("price").std().alias("price_std")',
                             'pl.col("price").var().alias("price_var")',
                             'pl.col("product").n_unique().alias("unique_products")',
                             'pl.col("region").first().alias("first_region")',
                             'pl.col("region").last().alias("last_region")'
                             )
    verify_if_execute(code)
    result_df = normalize_result(get_result_from_generated_code(code))
    expected_df = normalize_result(flow.get_node(2).get_resulting_data().data_frame)
    assert_frame_equal(result_df, expected_df, check_row_order=False)


@pytest.mark.parametrize("export_func", [
    export_flow_to_polars,
    export_flow_to_flowframe,
], ids=["polars", "flowframe"])
def test_flow_with_disconnected_nodes(export_func):
    """Test a flow where some nodes might not be connected properly"""
    flow = create_basic_flow()

    # Add first node
    data1 = input_schema.NodeManualInput(
        flow_id=1,
        node_id=1,
        raw_data_format=input_schema.RawData(
            columns=[input_schema.MinimalFieldInfo(name="col1", data_type="Integer")],
            data=[[1, 2, 3]]
        )
    )
    flow.add_manual_input(data1)

    # Add disconnected node (no depending_on_id connection)
    data2 = input_schema.NodeManualInput(
        flow_id=1,
        node_id=2,
        raw_data_format=input_schema.RawData(
            columns=[input_schema.MinimalFieldInfo(name="col2", data_type="String")],
            data=[["A", "B", "C"]]
        )
    )
    flow.add_manual_input(data2)

    # Convert to code - should handle both nodes
    code = export_func(flow)

    # Both dataframes should be created
    if export_func is export_flow_to_polars:
        verify_code_contains(code,
                             "df_1 = pl.LazyFrame(",
                             "df_2 = pl.LazyFrame("
                             )


@pytest.mark.parametrize("export_func", [
    export_flow_to_polars,
    pytest.param(export_flow_to_flowframe, marks=pytest.mark.xfail(reason="FlowFrame code gen: custom polars_code nodes use pl.LazyFrame which is not available via ff")),
], ids=["polars", "flowframe"])
def test_custom_code_with_assignment(export_func):
    """Test custom Polars code that includes variable assignments"""
    flow = create_basic_flow()
    flow = create_sample_dataframe_node(flow)

    # Add custom code with assignments
    polars_code_node = input_schema.NodePolarsCode(
        flow_id=1,
        node_id=2,
        depending_on_ids=[1],
        polars_code_input=transform_schema.PolarsCodeInput(
            polars_code="""filtered = input_df.filter(pl.col('age') > 25)
sorted = filtered.sort('salary', descending=True)
output_df = sorted.select(['name', 'salary'])"""
        )
    )
    flow.add_polars_code(polars_code_node)
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2))

    # Convert to code
    code = export_func(flow)

    # Verify the code structure
    if export_func is export_flow_to_polars:
        verify_code_contains(code,
                             "def _polars_code_2(input_df: pl.LazyFrame):",
                             "filtered = input_df.filter(pl.col('age') > 25)",
                             "sorted = filtered.sort('salary', descending=True)",
                             "output_df = sorted.select(['name', 'salary'])",
                             "return output_df"
                             )
    verify_if_execute(code)
    get_result_from_generated_code(code)
    result_df = normalize_result(get_result_from_generated_code(code))
    expected_df = normalize_result(flow.get_node(2).get_resulting_data().data_frame)
    assert_frame_equal(result_df, expected_df, check_row_order=False)


@pytest.mark.parametrize("export_func", [
    export_flow_to_polars,
    export_flow_to_flowframe,
], ids=["polars", "flowframe"])
def test_text_to_rows_without_output_name(export_func):
    """Test text to rows when output column name is not specified"""
    flow = create_basic_flow()

    # Add data with text to split
    data = input_schema.NodeManualInput(
        flow_id=1,
        node_id=1,
        raw_data_format=input_schema.RawData(
            columns=[
                input_schema.MinimalFieldInfo(name="items", data_type="String")
            ],
            data=[["a,b,c", "x,y,z"]]
        )
    )
    flow.add_manual_input(data)

    # Add text to rows without output column name
    text_to_rows_node = input_schema.NodeTextToRows(
        flow_id=1,
        node_id=2,
        depending_on_id=1,
        text_to_rows_input=transform_schema.TextToRowsInput(
            column_to_split="items",
            split_fixed_value=","
        )
    )
    flow.add_text_to_rows(text_to_rows_node)
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2))

    # Convert to code
    code = export_func(flow)

    # Should explode the original column
    if export_func is export_flow_to_polars:
        verify_code_contains(code,
                             'pl.col("items").str.split(",")',
                             "explode('items')"
                             )
    result_df = normalize_result(get_result_from_generated_code(code))
    expected_df = normalize_result(flow.get_node(2).get_resulting_data().data_frame)
    assert_frame_equal(result_df, expected_df, check_row_order=False)


@pytest.mark.skipif(not is_docker_available(), reason="Docker is not available or not running")
def test_cloud_storage_reader():
    conn = ensure_cloud_storage_connection_is_available_and_get_connection()

    flow = create_basic_flow()
    read_settings = cloud_ss.CloudStorageReadSettings(
        resource_path="s3://test-bucket/single-file-parquet/data.parquet",
        file_format="parquet",
        scan_mode="single_file",
        connection_name=conn.connection_name
    )
    node_settings = input_schema.NodeCloudStorageReader(flow_id=flow.flow_id, node_id=1, user_id=1,
                                                        cloud_storage_settings=read_settings)
    flow.add_cloud_storage_reader(node_settings)
    record_count_node = input_schema.NodeRecordCount(flow_id=1, node_id=2, depending_on_id=1)

    flow.add_record_count(record_count_node)
    add_connection(flow, node_connection=input_schema.NodeConnection.create_from_simple_input(1, 2))
    code = export_flow_to_flowframe(flow)
    result = normalize_result(get_result_from_generated_code(code))
    expected_df = normalize_result(flow.get_node(2).get_resulting_data().data_frame)
    assert_frame_equal(result, expected_df, check_row_order=False)


@pytest.mark.skipif(not is_docker_available(), reason="Docker is not available or not running")
@pytest.mark.parametrize("file_format", ["csv", "parquet", "json", "delta"])
def test_cloud_storage_writer(file_format):
    if file_format != "delta":
        output_file_name = f"s3://flowfile-test/flowfile_generated_data_{uuid4()}.{file_format}"
    else:
        output_file_name = f"s3://flowfile-test/flowfile_generated_data_{uuid4()}"
    conn = ensure_cloud_storage_connection_is_available_and_get_connection()
    write_settings = cloud_ss.CloudStorageWriteSettings(
        resource_path=output_file_name,
        file_format=file_format,
        connection_name=conn.connection_name
    )
    read_settings = cloud_ss.CloudStorageReadSettings(
        resource_path=output_file_name,
        file_format=file_format,
        connection_name=conn.connection_name
    )
    flow = create_basic_flow()
    flow = create_sample_dataframe_node(flow)
    record_count_node = input_schema.NodeRecordCount(flow_id=1, node_id=2, depending_on_id=1)
    flow.add_record_count(record_count_node)
    add_connection(flow, node_connection=input_schema.NodeConnection.create_from_simple_input(1, 2))

    node_settings = input_schema.NodeCloudStorageWriter(flow_id=flow.flow_id, node_id=3, user_id=1,
                                                        cloud_storage_settings=write_settings,)
    flow.add_cloud_storage_writer(node_settings)

    add_connection(flow, node_connection=input_schema.NodeConnection.create_from_simple_input(2, 3))
    code = export_flow_to_flowframe(flow)
    verify_if_execute(code)
    fde = FlowDataEngine.from_cloud_storage_obj(
        cloud_ss.CloudStorageReadSettingsInternal(read_settings=read_settings, connection=get_cloud_connection())
    )
    assert fde.collect()[0, 0] == 5


@pytest.mark.parametrize("export_func", [
    export_flow_to_polars,
    export_flow_to_flowframe,
], ids=["polars", "flowframe"])
def test_fuzzy_match_single_file(fuzzy_join_left_data, export_func):
    flow = create_basic_flow(1)
    flow.add_manual_input(fuzzy_join_left_data)
    settings = input_schema.NodeFuzzyMatch(flow_id=1, node_id=2, description='', auto_generate_selection=True,
                                           join_input=transform_schema.FuzzyMatchInput(
                                               join_mapping=[FuzzyMapping('name',threshold_score=75.0)],
            left_select=[transform_schema.SelectInput(old_name='id', keep=True),
                         transform_schema.SelectInput(old_name='name', keep=True),
                         transform_schema.SelectInput(old_name='address', keep=True)],
            right_select=[transform_schema.SelectInput(old_name='id', keep=True),
                          transform_schema.SelectInput(old_name='name', keep=True),
                          transform_schema.SelectInput(old_name='address', keep=True)],
                                           ), auto_keep_all=True)
    flow.add_fuzzy_match(settings)

    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2, input_type="main"))
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2, input_type="right"))

    code = export_func(flow)

    verify_if_execute(code)
    result = normalize_result(get_result_from_generated_code(code))
    expected_df = normalize_result(flow.get_node(2).get_resulting_data().data_frame)
    assert_frame_equal(result, expected_df, check_dtypes=False, check_row_order=False)


@pytest.mark.parametrize("export_func", [
    export_flow_to_polars,
    export_flow_to_flowframe,
], ids=["polars", "flowframe"])
def test_fuzzy_match_single_multiple_columns_file(fuzzy_join_left_data, export_func):
    flow = create_basic_flow(1)
    flow.add_manual_input(fuzzy_join_left_data)
    settings = input_schema.NodeFuzzyMatch(flow_id=1, node_id=2, description='', auto_generate_selection=True,
                                           join_input=transform_schema.FuzzyMatchInput(
                                               join_mapping=[FuzzyMapping('name',threshold_score=75.0)],
            left_select=[transform_schema.SelectInput(old_name='name', keep=True),
                         transform_schema.SelectInput(old_name='id', keep=True)],
            right_select=[transform_schema.SelectInput(old_name='name', keep=True),
                          transform_schema.SelectInput(old_name='id', keep=False)],
                                           ), auto_keep_all=True)
    flow.add_fuzzy_match(settings)
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2, input_type="main"))
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2, input_type="right"))

    code = export_func(flow)

    verify_if_execute(code)
    result = normalize_result(get_result_from_generated_code(code))
    expected_df = normalize_result(flow.get_node(2).get_resulting_data().data_frame)
    assert_frame_equal(result, expected_df, check_dtypes=False, check_row_order=False)


def test_explore_data_node_skipped():
    """Test that explore_data nodes are skipped with a comment but code still generates."""
    flow = create_basic_flow()

    # Add manual input
    manual_input = input_schema.NodeManualInput(
        flow_id=1,
        node_id=1,
        raw_data_format=input_schema.RawData(
            columns=[input_schema.MinimalFieldInfo(name="id", data_type="Integer")],
            data=[[1, 2, 3]]
        )
    )
    flow.add_manual_input(manual_input)

    # Add explore_data node
    flow.add_node_promise(input_schema.NodePromise(node_id=2, flow_id=1, node_type="explore_data"))
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2))
    flow.add_explore_data(input_schema.NodeExploreData(flow_id=1, node_id=2, depending_on_id=1))

    # Should generate code successfully
    code = export_flow_to_polars(flow)
    assert "# Node 2: Explore Data (skipped - interactive visualization only)" in code
    assert "df_2 = df_1  # Pass through unchanged" in code
    verify_if_execute(code)


def test_database_reader_reference_mode_code_generation():
    """Test database reader code generation with reference mode.

    This test directly tests the code generator handler without going through
    the full flow.add_database_reader() which tries to resolve connections.
    """
    from flowfile_core.flowfile.code_generator.code_generator import FlowGraphToPolarsConverter

    flow = create_basic_flow()

    # Create a minimal database reader settings for code generation test
    db_settings = input_schema.DatabaseSettings(
        connection_mode="reference",
        database_connection_name="test_connection",
        query_mode="table",
        table_name="users",
        schema_name="public",
    )
    db_reader = input_schema.NodeDatabaseReader(
        flow_id=1,
        node_id=1,
        database_settings=db_settings,
    )

    # Test the handler directly
    converter = FlowGraphToPolarsConverter(flow)
    converter._handle_database_reader(db_reader, "df_1", {})

    # Check generated code
    code_output = "\n".join(converter.code_lines)
    assert 'ff.read_database(' in code_output
    assert '"test_connection"' in code_output
    assert 'table_name="users"' in code_output
    assert 'schema_name="public"' in code_output
    assert "import flowfile as ff" in converter.imports


def test_database_reader_query_mode_code_generation():
    """Test database reader code generation with query mode."""
    from flowfile_core.flowfile.code_generator.code_generator import FlowGraphToPolarsConverter

    flow = create_basic_flow()

    db_settings = input_schema.DatabaseSettings(
        connection_mode="reference",
        database_connection_name="test_connection",
        query_mode="query",
        query="SELECT id, name\nFROM users\nWHERE active = true",
    )
    db_reader = input_schema.NodeDatabaseReader(
        flow_id=1,
        node_id=1,
        database_settings=db_settings,
    )

    converter = FlowGraphToPolarsConverter(flow)
    converter._handle_database_reader(db_reader, "df_1", {})

    code_output = "\n".join(converter.code_lines)
    assert 'ff.read_database(' in code_output
    assert 'query="""' in code_output
    assert "SELECT id, name" in code_output
    assert "FROM users" in code_output


def test_database_reader_inline_mode_adds_to_unsupported():
    """Test that database reader with inline mode is added to unsupported nodes."""
    from flowfile_core.flowfile.code_generator.code_generator import FlowGraphToPolarsConverter

    flow = create_basic_flow()

    db_settings = input_schema.DatabaseSettings(
        connection_mode="inline",
        database_connection=input_schema.DatabaseConnection(
            database_type="postgresql",
            host="localhost",
            port=5432,
            database="test",
            username="user",
        ),
        query_mode="table",
        table_name="users",
    )
    db_reader = input_schema.NodeDatabaseReader(
        flow_id=1,
        node_id=1,
        database_settings=db_settings,
    )

    converter = FlowGraphToPolarsConverter(flow)
    converter._handle_database_reader(db_reader, "df_1", {})

    # Should have added to unsupported nodes
    assert len(converter.unsupported_nodes) == 1
    assert "inline" in converter.unsupported_nodes[0][2].lower()


def test_database_writer_reference_mode_code_generation():
    """Test database writer code generation with reference mode."""
    from flowfile_core.flowfile.code_generator.code_generator import FlowGraphToPolarsConverter

    flow = create_basic_flow()

    db_write_settings = input_schema.DatabaseWriteSettings(
        connection_mode="reference",
        database_connection_name="test_connection",
        table_name="output_table",
        schema_name="public",
        if_exists="replace",
    )
    db_writer = input_schema.NodeDatabaseWriter(
        flow_id=1,
        node_id=2,
        depending_on_id=1,
        database_write_settings=db_write_settings,
    )

    converter = FlowGraphToPolarsConverter(flow)
    converter._handle_database_writer(db_writer, "df_2", {"main": "df_1"})

    code_output = "\n".join(converter.code_lines)
    assert "ff.write_database(" in code_output
    assert '"test_connection"' in code_output
    assert '"output_table"' in code_output
    assert 'schema_name="public"' in code_output
    assert 'if_exists="replace"' in code_output
    assert "import flowfile as ff" in converter.imports


def test_database_writer_inline_mode_adds_to_unsupported():
    """Test that database writer with inline mode is added to unsupported nodes."""
    from flowfile_core.flowfile.code_generator.code_generator import FlowGraphToPolarsConverter

    flow = create_basic_flow()

    db_write_settings = input_schema.DatabaseWriteSettings(
        connection_mode="inline",
        database_connection=input_schema.DatabaseConnection(
            database_type="postgresql",
            host="localhost",
            port=5432,
            database="test",
            username="user",
        ),
        table_name="output_table",
    )
    db_writer = input_schema.NodeDatabaseWriter(
        flow_id=1,
        node_id=2,
        depending_on_id=1,
        database_write_settings=db_write_settings,
    )

    converter = FlowGraphToPolarsConverter(flow)
    converter._handle_database_writer(db_writer, "df_2", {"main": "df_1"})

    # Should have added to unsupported nodes
    assert len(converter.unsupported_nodes) == 1
    assert "inline" in converter.unsupported_nodes[0][2].lower()


def test_external_source_adds_to_unsupported():
    """Test that external_source nodes are added to unsupported nodes."""
    from flowfile_core.flowfile.code_generator.code_generator import FlowGraphToPolarsConverter

    flow = create_basic_flow()

    # Create external source settings with required fields
    sample_users_settings = input_schema.SampleUsers(SAMPLE_USERS=True, size=10)
    external_source = input_schema.NodeExternalSource(
        flow_id=1,
        node_id=1,
        identifier="test_source",
        source_settings=sample_users_settings,
    )

    converter = FlowGraphToPolarsConverter(flow)
    converter._handle_external_source(external_source, "df_1", {})

    # Should have added to unsupported nodes
    assert len(converter.unsupported_nodes) == 1
    assert "external_source" in converter.unsupported_nodes[0][1].lower()


# ==================== Node Reference Tests ====================


def test_node_reference_basic():
    """Test that node_reference is used as variable name in generated code when set."""
    flow = create_basic_flow()

    # Create a node with a custom node_reference
    manual_input = input_schema.NodeManualInput(
        flow_id=flow.flow_id,
        node_id=1,
        node_reference="my_data",
        raw_data_format=input_schema.RawData(
            columns=[
                input_schema.MinimalFieldInfo(name="id", data_type="Integer"),
                input_schema.MinimalFieldInfo(name="name", data_type="String"),
            ],
            data=[
                [1, 2, 3],
                ["Alice", "Bob", "Charlie"],
            ]
        )
    )
    flow.add_datasource(manual_input)

    # Convert to Polars code
    code = export_flow_to_polars(flow)

    # Verify the custom reference is used instead of df_1
    verify_code_contains(code, "my_data = pl.LazyFrame")
    assert "df_1" not in code, "Should use node_reference instead of df_1"
    verify_if_execute(code)


def test_node_reference_in_formula():
    """Test that node_reference is used in downstream node references."""
    flow = create_basic_flow()

    # Create input node with custom reference
    manual_input = input_schema.NodeManualInput(
        flow_id=flow.flow_id,
        node_id=1,
        node_reference="source_data",
        raw_data_format=input_schema.RawData(
            columns=[
                input_schema.MinimalFieldInfo(name="price", data_type="Integer"),
                input_schema.MinimalFieldInfo(name="quantity", data_type="Integer"),
            ],
            data=[
                [10, 20, 30],
                [2, 3, 4],
            ]
        )
    )
    flow.add_datasource(manual_input)

    # Add formula node with custom reference
    formula_node = input_schema.NodeFormula(
        flow_id=1,
        node_id=2,
        node_reference="calculated_data",
        depending_on_id=1,
        function=transform_schema.FunctionInput(
            field=transform_schema.FieldInput(name="total", data_type="Auto"),
            function="[price] * [quantity]"
        )
    )
    flow.add_formula(formula_node)
    add_connection(flow, node_connection=input_schema.NodeConnection.create_from_simple_input(1, 2))

    # Convert to Polars code
    code = export_flow_to_polars(flow)

    # Verify custom references are used
    verify_code_contains(code, "source_data = pl.LazyFrame")
    verify_code_contains(code, "calculated_data = source_data.with_columns")
    assert "df_1" not in code, "Should use source_data instead of df_1"
    assert "df_2" not in code, "Should use calculated_data instead of df_2"
    verify_if_execute(code)


def test_node_reference_mixed():
    """Test that nodes with and without node_reference work together."""
    flow = create_basic_flow()
    # First node: custom reference
    manual_input1 = input_schema.NodeManualInput(
        flow_id=flow.flow_id,
        node_id=1,
        node_reference="custom_input",
        raw_data_format=input_schema.RawData(
            columns=[
                input_schema.MinimalFieldInfo(name="id", data_type="Integer"),
                input_schema.MinimalFieldInfo(name="value", data_type="Integer"),
            ],
            data=[
                [1, 2],
                [100, 200],
            ]
        )
    )
    flow.add_datasource(manual_input1)

    # Second node: no custom reference (should use df_2)
    filter_node = input_schema.NodeFilter(
        flow_id=1,
        node_id=2,
        # No node_reference set
        depending_on_id=1,
        filter_input=transform_schema.FilterInput(
            mode="basic",
            basic_filter=transform_schema.BasicFilter(
                field="value",
                filter_type=">=",
                filter_value="100"
            )
        )
    )
    flow.add_filter(filter_node)
    add_connection(flow, node_connection=input_schema.NodeConnection.create_from_simple_input(1, 2))

    # Convert to Polars code
    code = export_flow_to_polars(flow)

    # Verify mixed references
    verify_code_contains(code, "custom_input = pl.LazyFrame")
    verify_code_contains(code, "df_2 = custom_input.filter")
    assert "df_1" not in code, "Should use custom_input instead of df_1"
    verify_if_execute(code)


def test_node_reference_in_join():
    """Test that node_reference works correctly in join operations."""
    flow = create_basic_flow()

    # Left input with custom reference
    manual_input1 = input_schema.NodeManualInput(
        flow_id=flow.flow_id,
        node_id=1,
        node_reference="left_table",
        raw_data_format=input_schema.RawData(
            columns=[
                input_schema.MinimalFieldInfo(name="id", data_type="Integer"),
                input_schema.MinimalFieldInfo(name="name", data_type="String"),
            ],
            data=[
                [1, 2, 3],
                ["Alice", "Bob", "Charlie"],
            ]
        )
    )
    flow.add_datasource(manual_input1)

    # Right input with custom reference
    manual_input2 = input_schema.NodeManualInput(
        flow_id=flow.flow_id,
        node_id=2,
        node_reference="right_table",
        raw_data_format=input_schema.RawData(
            columns=[
                input_schema.MinimalFieldInfo(name="id", data_type="Integer"),
                input_schema.MinimalFieldInfo(name="city", data_type="String"),
            ],
            data=[
                [1, 2, 3],
                ["NYC", "LA", "Chicago"],
            ]
        )
    )
    flow.add_datasource(manual_input2)

    # Join node with custom reference
    join_node = input_schema.NodeJoin(
        flow_id=1,
        node_id=3,
        node_reference="joined_result",
        depending_on_ids=[1, 2],
        join_input=transform_schema.JoinInput(
            join_mapping=[transform_schema.JoinMap("id", "id")],
            left_select=[
                transform_schema.SelectInput("id", "id", keep=True),
                transform_schema.SelectInput("name", "name")
            ],
            right_select=[
                transform_schema.SelectInput("id", "id", keep=False),
                transform_schema.SelectInput("city", "city")
            ],
            how="inner"
        )
    )
    flow.add_join(join_node)
    left_connection = input_schema.NodeConnection.create_from_simple_input(1, 3, 'main')
    right_connection = input_schema.NodeConnection.create_from_simple_input(2, 3, 'right')
    add_connection(flow, left_connection)
    add_connection(flow, right_connection)

    # Convert to Polars code
    code = export_flow_to_polars(flow)

    # Verify custom references in join
    verify_code_contains(code, "left_table = pl.LazyFrame")
    verify_code_contains(code, "right_table = pl.LazyFrame")
    verify_code_contains(code, "joined_result = (left_table")
    assert "df_1" not in code, "Should use left_table instead of df_1"
    assert "df_2" not in code, "Should use right_table instead of df_2"
    assert "df_3" not in code, "Should use joined_result instead of df_3"
    verify_if_execute(code)


def test_node_reference_default_when_empty():
    """Test that empty node_reference falls back to df_{node_id}."""
    flow = create_basic_flow()

    # Node with empty string reference (should use default)
    manual_input = input_schema.NodeManualInput(
        flow_id=flow.flow_id,
        node_id=1,
        node_reference="",  # Empty string
        raw_data_format=input_schema.RawData(
            columns=[
                input_schema.MinimalFieldInfo(name="id", data_type="Integer"),
            ],
            data=[
                [1, 2, 3],
            ]
        )
    )
    flow.add_datasource(manual_input)

    # Convert to Polars code
    code = export_flow_to_polars(flow)

    # Should use default df_1
    verify_code_contains(code, "df_1 = pl.LazyFrame")
    verify_if_execute(code)


def test_node_reference_none_uses_default():
    """Test that None node_reference uses df_{node_id}."""
    flow = create_basic_flow()

    # Node with None reference (should use default)
    manual_input = input_schema.NodeManualInput(
        flow_id=flow.flow_id,
        node_id=1,
        node_reference=None,  # Explicitly None
        raw_data_format=input_schema.RawData(
            columns=[
                input_schema.MinimalFieldInfo(name="id", data_type="Integer"),
            ],
            data=[
                [1, 2, 3],
            ]
        )
    )
    flow.add_datasource(manual_input)

    # Convert to Polars code
    code = export_flow_to_polars(flow)

    # Should use default df_1
    verify_code_contains(code, "df_1 = pl.LazyFrame")
    verify_if_execute(code)


# ============================================================================
# Basic Filter Operator Tests
# ============================================================================

@pytest.fixture
def filter_test_data() -> input_schema.NodeManualInput:
    """Create test data for filter tests with various data types."""
    return input_schema.NodeManualInput(
        flow_id=1,
        node_id=1,
        raw_data_format=input_schema.RawData(
            columns=[
                input_schema.MinimalFieldInfo(name="id", data_type="Integer"),
                input_schema.MinimalFieldInfo(name="name", data_type="String"),
                input_schema.MinimalFieldInfo(name="score", data_type="Double"),
                input_schema.MinimalFieldInfo(name="city", data_type="String"),
            ],
            data=[
                [1, 2, 3, 4, 5],
                ["Alice", "Bob", "Charlie", "David", None],
                [85.5, 90.0, 75.5, 60.0, 95.5],
                ["New York", "Los Angeles", "Chicago", "New York", "Boston"],
            ]
        )
    )


@pytest.mark.parametrize("export_func", [export_flow_to_polars, export_flow_to_flowframe], ids=["polars", "flowframe"])
def test_basic_filter_equals_numeric(filter_test_data, export_func):
    """Test basic filter with equals operator on numeric field."""
    flow = create_basic_flow()
    flow.add_manual_input(filter_test_data)

    filter_node = input_schema.NodeFilter(
        flow_id=1,
        node_id=2,
        depending_on_id=1,
        filter_input=transform_schema.FilterInput(
            mode="basic",
            basic_filter=transform_schema.BasicFilter(
                field="id",
                operator=transform_schema.FilterOperator.EQUALS,
                value="3"
            )
        )
    )
    flow.add_filter(filter_node)
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2))

    code = export_func(flow)
    if export_func is export_flow_to_polars:
        verify_code_contains(code, 'pl.col("id") == 3')
    verify_if_execute(code)
    result_df = normalize_result(get_result_from_generated_code(code))
    expected_df = normalize_result(flow.get_node(2).get_resulting_data().data_frame)
    assert_frame_equal(result_df, expected_df)


@pytest.mark.parametrize("export_func", [export_flow_to_polars, export_flow_to_flowframe], ids=["polars", "flowframe"])
def test_basic_filter_equals_string(filter_test_data, export_func):
    """Test basic filter with equals operator on string field."""
    flow = create_basic_flow()
    flow.add_manual_input(filter_test_data)

    filter_node = input_schema.NodeFilter(
        flow_id=1,
        node_id=2,
        depending_on_id=1,
        filter_input=transform_schema.FilterInput(
            mode="basic",
            basic_filter=transform_schema.BasicFilter(
                field="name",
                operator=transform_schema.FilterOperator.EQUALS,
                value="Alice"
            )
        )
    )
    flow.add_filter(filter_node)
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2))

    code = export_func(flow)
    if export_func is export_flow_to_polars:
        verify_code_contains(code, 'pl.col("name") == "Alice"')
    verify_if_execute(code)
    result_df = normalize_result(get_result_from_generated_code(code))
    expected_df = normalize_result(flow.get_node(2).get_resulting_data().data_frame)
    assert_frame_equal(result_df, expected_df)


@pytest.mark.parametrize("export_func", [export_flow_to_polars, export_flow_to_flowframe], ids=["polars", "flowframe"])
def test_basic_filter_not_equals(filter_test_data, export_func):
    """Test basic filter with not equals operator."""
    flow = create_basic_flow()
    flow.add_manual_input(filter_test_data)

    filter_node = input_schema.NodeFilter(
        flow_id=1,
        node_id=2,
        depending_on_id=1,
        filter_input=transform_schema.FilterInput(
            mode="basic",
            basic_filter=transform_schema.BasicFilter(
                field="id",
                operator=transform_schema.FilterOperator.NOT_EQUALS,
                value="1"
            )
        )
    )
    flow.add_filter(filter_node)
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2))

    code = export_func(flow)
    if export_func is export_flow_to_polars:
        verify_code_contains(code, 'pl.col("id") != 1')
    verify_if_execute(code)
    result_df = normalize_result(get_result_from_generated_code(code))
    expected_df = normalize_result(flow.get_node(2).get_resulting_data().data_frame)
    assert_frame_equal(result_df, expected_df)


@pytest.mark.parametrize("export_func", [export_flow_to_polars, export_flow_to_flowframe], ids=["polars", "flowframe"])
def test_basic_filter_greater_than(filter_test_data, export_func):
    """Test basic filter with greater than operator."""
    flow = create_basic_flow()
    flow.add_manual_input(filter_test_data)

    filter_node = input_schema.NodeFilter(
        flow_id=1,
        node_id=2,
        depending_on_id=1,
        filter_input=transform_schema.FilterInput(
            mode="basic",
            basic_filter=transform_schema.BasicFilter(
                field="score",
                operator=transform_schema.FilterOperator.GREATER_THAN,
                value="80"
            )
        )
    )
    flow.add_filter(filter_node)
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2))

    code = export_func(flow)
    if export_func is export_flow_to_polars:
        verify_code_contains(code, 'pl.col("score") > 80')
    verify_if_execute(code)
    result_df = normalize_result(get_result_from_generated_code(code))
    expected_df = normalize_result(flow.get_node(2).get_resulting_data().data_frame)
    assert_frame_equal(result_df, expected_df)


@pytest.mark.parametrize("export_func", [export_flow_to_polars, export_flow_to_flowframe], ids=["polars", "flowframe"])
def test_basic_filter_greater_than_or_equals(filter_test_data, export_func):
    """Test basic filter with greater than or equals operator."""
    flow = create_basic_flow()
    flow.add_manual_input(filter_test_data)

    filter_node = input_schema.NodeFilter(
        flow_id=1,
        node_id=2,
        depending_on_id=1,
        filter_input=transform_schema.FilterInput(
            mode="basic",
            basic_filter=transform_schema.BasicFilter(
                field="score",
                operator=transform_schema.FilterOperator.GREATER_THAN_OR_EQUALS,
                value="90"
            )
        )
    )
    flow.add_filter(filter_node)
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2))

    code = export_func(flow)
    if export_func is export_flow_to_polars:
        verify_code_contains(code, 'pl.col("score") >= 90')
    verify_if_execute(code)
    result_df = normalize_result(get_result_from_generated_code(code))
    expected_df = normalize_result(flow.get_node(2).get_resulting_data().data_frame)
    assert_frame_equal(result_df, expected_df)


@pytest.mark.parametrize("export_func", [export_flow_to_polars, export_flow_to_flowframe], ids=["polars", "flowframe"])
def test_basic_filter_less_than(filter_test_data, export_func):
    """Test basic filter with less than operator."""
    flow = create_basic_flow()
    flow.add_manual_input(filter_test_data)

    filter_node = input_schema.NodeFilter(
        flow_id=1,
        node_id=2,
        depending_on_id=1,
        filter_input=transform_schema.FilterInput(
            mode="basic",
            basic_filter=transform_schema.BasicFilter(
                field="score",
                operator=transform_schema.FilterOperator.LESS_THAN,
                value="80"
            )
        )
    )
    flow.add_filter(filter_node)
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2))

    code = export_func(flow)
    if export_func is export_flow_to_polars:
        verify_code_contains(code, 'pl.col("score") < 80')
    verify_if_execute(code)
    result_df = normalize_result(get_result_from_generated_code(code))
    expected_df = normalize_result(flow.get_node(2).get_resulting_data().data_frame)
    assert_frame_equal(result_df, expected_df)


@pytest.mark.parametrize("export_func", [export_flow_to_polars, export_flow_to_flowframe], ids=["polars", "flowframe"])
def test_basic_filter_less_than_or_equals(filter_test_data, export_func):
    """Test basic filter with less than or equals operator."""
    flow = create_basic_flow()
    flow.add_manual_input(filter_test_data)

    filter_node = input_schema.NodeFilter(
        flow_id=1,
        node_id=2,
        depending_on_id=1,
        filter_input=transform_schema.FilterInput(
            mode="basic",
            basic_filter=transform_schema.BasicFilter(
                field="score",
                operator=transform_schema.FilterOperator.LESS_THAN_OR_EQUALS,
                value="75.5"
            )
        )
    )
    flow.add_filter(filter_node)
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2))

    code = export_func(flow)
    if export_func is export_flow_to_polars:
        verify_code_contains(code, 'pl.col("score") <= 75.5')
    verify_if_execute(code)
    result_df = normalize_result(get_result_from_generated_code(code))
    expected_df = normalize_result(flow.get_node(2).get_resulting_data().data_frame)
    assert_frame_equal(result_df, expected_df)


@pytest.mark.parametrize("export_func", [export_flow_to_polars, export_flow_to_flowframe], ids=["polars", "flowframe"])
def test_basic_filter_contains(filter_test_data, export_func):
    """Test basic filter with contains operator."""
    flow = create_basic_flow()
    flow.add_manual_input(filter_test_data)

    filter_node = input_schema.NodeFilter(
        flow_id=1,
        node_id=2,
        depending_on_id=1,
        filter_input=transform_schema.FilterInput(
            mode="basic",
            basic_filter=transform_schema.BasicFilter(
                field="city",
                operator=transform_schema.FilterOperator.CONTAINS,
                value="New"
            )
        )
    )
    flow.add_filter(filter_node)
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2))

    code = export_func(flow)
    if export_func is export_flow_to_polars:
        verify_code_contains(code, 'pl.col("city").str.contains("New")')
    verify_if_execute(code)
    result_df = normalize_result(get_result_from_generated_code(code))
    expected_df = normalize_result(flow.get_node(2).get_resulting_data().data_frame)
    assert_frame_equal(result_df, expected_df)


@pytest.mark.parametrize("export_func", [export_flow_to_polars, export_flow_to_flowframe], ids=["polars", "flowframe"])
def test_basic_filter_not_contains(filter_test_data, export_func):
    """Test basic filter with not contains operator."""
    flow = create_basic_flow()
    flow.add_manual_input(filter_test_data)

    filter_node = input_schema.NodeFilter(
        flow_id=1,
        node_id=2,
        depending_on_id=1,
        filter_input=transform_schema.FilterInput(
            mode="basic",
            basic_filter=transform_schema.BasicFilter(
                field="city",
                operator=transform_schema.FilterOperator.NOT_CONTAINS,
                value="New"
            )
        )
    )
    flow.add_filter(filter_node)
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2))

    code = export_func(flow)
    if export_func is export_flow_to_polars:
        verify_code_contains(code, 'pl.col("city").str.contains("New").not_()')
    verify_if_execute(code)
    result_df = normalize_result(get_result_from_generated_code(code))
    expected_df = normalize_result(flow.get_node(2).get_resulting_data().data_frame)
    assert_frame_equal(result_df, expected_df)


@pytest.mark.parametrize("export_func", [export_flow_to_polars, export_flow_to_flowframe], ids=["polars", "flowframe"])
def test_basic_filter_starts_with(filter_test_data, export_func):
    """Test basic filter with starts_with operator."""
    flow = create_basic_flow()
    flow.add_manual_input(filter_test_data)

    filter_node = input_schema.NodeFilter(
        flow_id=1,
        node_id=2,
        depending_on_id=1,
        filter_input=transform_schema.FilterInput(
            mode="basic",
            basic_filter=transform_schema.BasicFilter(
                field="name",
                operator=transform_schema.FilterOperator.STARTS_WITH,
                value="A"
            )
        )
    )
    flow.add_filter(filter_node)
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2))

    code = export_func(flow)
    if export_func is export_flow_to_polars:
        verify_code_contains(code, 'pl.col("name").str.starts_with("A")')
    verify_if_execute(code)
    result_df = normalize_result(get_result_from_generated_code(code))
    expected_df = normalize_result(flow.get_node(2).get_resulting_data().data_frame)
    assert_frame_equal(result_df, expected_df)


@pytest.mark.parametrize("export_func", [export_flow_to_polars, export_flow_to_flowframe], ids=["polars", "flowframe"])
def test_basic_filter_ends_with(filter_test_data, export_func):
    """Test basic filter with ends_with operator."""
    flow = create_basic_flow()
    flow.add_manual_input(filter_test_data)

    filter_node = input_schema.NodeFilter(
        flow_id=1,
        node_id=2,
        depending_on_id=1,
        filter_input=transform_schema.FilterInput(
            mode="basic",
            basic_filter=transform_schema.BasicFilter(
                field="name",
                operator=transform_schema.FilterOperator.ENDS_WITH,
                value="e"
            )
        )
    )
    flow.add_filter(filter_node)
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2))

    code = export_func(flow)
    if export_func is export_flow_to_polars:
        verify_code_contains(code, 'pl.col("name").str.ends_with("e")')
    verify_if_execute(code)
    result_df = normalize_result(get_result_from_generated_code(code))
    expected_df = normalize_result(flow.get_node(2).get_resulting_data().data_frame)
    assert_frame_equal(result_df, expected_df)


@pytest.mark.parametrize("export_func", [export_flow_to_polars, export_flow_to_flowframe], ids=["polars", "flowframe"])
def test_basic_filter_is_null(filter_test_data, export_func):
    """Test basic filter with is_null operator."""
    flow = create_basic_flow()
    flow.add_manual_input(filter_test_data)

    filter_node = input_schema.NodeFilter(
        flow_id=1,
        node_id=2,
        depending_on_id=1,
        filter_input=transform_schema.FilterInput(
            mode="basic",
            basic_filter=transform_schema.BasicFilter(
                field="name",
                operator=transform_schema.FilterOperator.IS_NULL,
                value=""
            )
        )
    )
    flow.add_filter(filter_node)
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2))

    code = export_func(flow)
    if export_func is export_flow_to_polars:
        verify_code_contains(code, 'pl.col("name").is_null()')
    verify_if_execute(code)
    result_df = normalize_result(get_result_from_generated_code(code))
    expected_df = normalize_result(flow.get_node(2).get_resulting_data().data_frame)
    assert_frame_equal(result_df, expected_df)


@pytest.mark.parametrize("export_func", [export_flow_to_polars, export_flow_to_flowframe], ids=["polars", "flowframe"])
def test_basic_filter_is_not_null(filter_test_data, export_func):
    """Test basic filter with is_not_null operator."""
    flow = create_basic_flow()
    flow.add_manual_input(filter_test_data)

    filter_node = input_schema.NodeFilter(
        flow_id=1,
        node_id=2,
        depending_on_id=1,
        filter_input=transform_schema.FilterInput(
            mode="basic",
            basic_filter=transform_schema.BasicFilter(
                field="name",
                operator=transform_schema.FilterOperator.IS_NOT_NULL,
                value=""
            )
        )
    )
    flow.add_filter(filter_node)
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2))

    code = export_func(flow)
    if export_func is export_flow_to_polars:
        verify_code_contains(code, 'pl.col("name").is_not_null()')
    verify_if_execute(code)
    result_df = normalize_result(get_result_from_generated_code(code))
    expected_df = normalize_result(flow.get_node(2).get_resulting_data().data_frame)
    assert_frame_equal(result_df, expected_df)


@pytest.mark.parametrize("export_func", [export_flow_to_polars, export_flow_to_flowframe], ids=["polars", "flowframe"])
def test_basic_filter_in_numeric(filter_test_data, export_func):
    """Test basic filter with in operator for numeric values."""
    flow = create_basic_flow()
    flow.add_manual_input(filter_test_data)

    filter_node = input_schema.NodeFilter(
        flow_id=1,
        node_id=2,
        depending_on_id=1,
        filter_input=transform_schema.FilterInput(
            mode="basic",
            basic_filter=transform_schema.BasicFilter(
                field="id",
                operator=transform_schema.FilterOperator.IN,
                value="1, 3, 5"
            )
        )
    )
    flow.add_filter(filter_node)
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2))

    code = export_func(flow)
    if export_func is export_flow_to_polars:
        verify_code_contains(code, 'pl.col("id").is_in([1, 3, 5])')
    verify_if_execute(code)
    result_df = normalize_result(get_result_from_generated_code(code))
    expected_df = normalize_result(flow.get_node(2).get_resulting_data().data_frame)
    assert_frame_equal(result_df, expected_df)


@pytest.mark.parametrize("export_func", [export_flow_to_polars, export_flow_to_flowframe], ids=["polars", "flowframe"])
def test_basic_filter_in_string(filter_test_data, export_func):
    """Test basic filter with in operator for string values."""
    flow = create_basic_flow()
    flow.add_manual_input(filter_test_data)

    filter_node = input_schema.NodeFilter(
        flow_id=1,
        node_id=2,
        depending_on_id=1,
        filter_input=transform_schema.FilterInput(
            mode="basic",
            basic_filter=transform_schema.BasicFilter(
                field="name",
                operator=transform_schema.FilterOperator.IN,
                value="Alice, Bob"
            )
        )
    )
    flow.add_filter(filter_node)
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2))

    code = export_func(flow)
    if export_func is export_flow_to_polars:
        verify_code_contains(code, 'pl.col("name").is_in(["Alice", "Bob"])')
    verify_if_execute(code)
    result_df = normalize_result(get_result_from_generated_code(code))
    expected_df = normalize_result(flow.get_node(2).get_resulting_data().data_frame)
    assert_frame_equal(result_df, expected_df)


@pytest.mark.parametrize("export_func", [export_flow_to_polars, export_flow_to_flowframe], ids=["polars", "flowframe"])
def test_basic_filter_not_in(filter_test_data, export_func):
    """Test basic filter with not_in operator."""
    flow = create_basic_flow()
    flow.add_manual_input(filter_test_data)

    filter_node = input_schema.NodeFilter(
        flow_id=1,
        node_id=2,
        depending_on_id=1,
        filter_input=transform_schema.FilterInput(
            mode="basic",
            basic_filter=transform_schema.BasicFilter(
                field="id",
                operator=transform_schema.FilterOperator.NOT_IN,
                value="1, 2"
            )
        )
    )
    flow.add_filter(filter_node)
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2))

    code = export_func(flow)
    if export_func is export_flow_to_polars:
        verify_code_contains(code, 'pl.col("id").is_in([1, 2]).not_()')
    verify_if_execute(code)
    result_df = normalize_result(get_result_from_generated_code(code))
    expected_df = normalize_result(flow.get_node(2).get_resulting_data().data_frame)
    assert_frame_equal(result_df, expected_df)


@pytest.mark.parametrize("export_func", [export_flow_to_polars, export_flow_to_flowframe], ids=["polars", "flowframe"])
def test_basic_filter_between(filter_test_data, export_func):
    """Test basic filter with between operator."""
    flow = create_basic_flow()
    flow.add_manual_input(filter_test_data)

    filter_node = input_schema.NodeFilter(
        flow_id=1,
        node_id=2,
        depending_on_id=1,
        filter_input=transform_schema.FilterInput(
            mode="basic",
            basic_filter=transform_schema.BasicFilter(
                field="score",
                operator=transform_schema.FilterOperator.BETWEEN,
                value="75",
                value2="90"
            )
        )
    )
    flow.add_filter(filter_node)
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2))

    code = export_func(flow)
    if export_func is export_flow_to_polars:
        verify_code_contains(code, '(pl.col("score") >= 75) & (pl.col("score") <= 90)')
    verify_if_execute(code)
    result_df = normalize_result(get_result_from_generated_code(code))
    expected_df = normalize_result(flow.get_node(2).get_resulting_data().data_frame)
    assert_frame_equal(result_df, expected_df)


# ============================================================================
# Fuzzy Match Edge Case Tests
# ============================================================================

@pytest.mark.parametrize("export_func", [
    export_flow_to_polars,
    export_flow_to_flowframe,
], ids=["polars", "flowframe"])
def test_fuzzy_match_with_multiple_columns(export_func):
    """Test fuzzy match with multiple matching columns."""
    flow = create_basic_flow()

    left_data = input_schema.NodeManualInput(
        flow_id=1,
        node_id=1,
        raw_data_format=input_schema.RawData(
            columns=[
                input_schema.MinimalFieldInfo(name="id", data_type="Integer"),
                input_schema.MinimalFieldInfo(name="first_name", data_type="String"),
                input_schema.MinimalFieldInfo(name="last_name", data_type="String"),
            ],
            data=[
                [1, 2, 3],
                ["John", "Jane", "Bob"],
                ["Smith", "Doe", "Johnson"]
            ]
        )
    )

    right_data = input_schema.NodeManualInput(
        flow_id=1,
        node_id=2,
        raw_data_format=input_schema.RawData(
            columns=[
                input_schema.MinimalFieldInfo(name="fname", data_type="String"),
                input_schema.MinimalFieldInfo(name="lname", data_type="String"),
                input_schema.MinimalFieldInfo(name="city", data_type="String"),
            ],
            data=[
                ["Jon", "Janet", "Bobby"],
                ["Smyth", "Dough", "Johnsen"],
                ["NYC", "LA", "Chicago"]
            ]
        )
    )

    flow.add_manual_input(left_data)
    flow.add_manual_input(right_data)

    fuzzy_node = input_schema.NodeFuzzyMatch(
        flow_id=1,
        node_id=3,
        depending_on_ids=[1, 2],
        join_input=transform_schema.FuzzyMatchInput(
            join_mapping=[
                transform_schema.FuzzyMap(
                    left_col="first_name",
                    right_col="fname",
                    fuzzy_type="levenshtein",
                    threshold_score=0.6
                ),
                transform_schema.FuzzyMap(
                    left_col="last_name",
                    right_col="lname",
                    fuzzy_type="levenshtein",
                    threshold_score=0.6
                )
            ],
            left_select=[
                transform_schema.SelectInput("id"),
                transform_schema.SelectInput("first_name"),
                transform_schema.SelectInput("last_name"),
            ],
            right_select=[
                transform_schema.SelectInput("fname"),
                transform_schema.SelectInput("lname"),
                transform_schema.SelectInput("city"),
            ]
        )
    )
    flow.add_fuzzy_match(fuzzy_node)
    left_connection = input_schema.NodeConnection.create_from_simple_input(1, 3, 'main')
    right_connection = input_schema.NodeConnection.create_from_simple_input(2, 3, 'right')
    add_connection(flow, left_connection)
    add_connection(flow, right_connection)

    code = export_func(flow)
    if export_func is export_flow_to_polars:
        verify_code_contains(
            code,
            "from pl_fuzzy_frame_match import FuzzyMapping, fuzzy_match_dfs",
            "fuzzy_match_dfs("
        )
    elif export_func is export_flow_to_flowframe:
        verify_code_contains(code, ".fuzzy_join(", "ff.FuzzyMapping(")
    verify_if_execute(code)
    result_df = normalize_result(get_result_from_generated_code(code))
    expected_df = normalize_result(flow.get_node(3).get_resulting_data().data_frame)
    assert_frame_equal(result_df, expected_df, check_row_order=False)


@pytest.mark.parametrize("export_func", [
    export_flow_to_polars,
    export_flow_to_flowframe,
], ids=["polars", "flowframe"])
def test_fuzzy_match_with_column_drops(export_func):
    """Test fuzzy match with columns marked as not keep."""
    flow = create_basic_flow()

    left_data = input_schema.NodeManualInput(
        flow_id=1,
        node_id=1,
        raw_data_format=input_schema.RawData(
            columns=[
                input_schema.MinimalFieldInfo(name="id", data_type="Integer"),
                input_schema.MinimalFieldInfo(name="name", data_type="String"),
                input_schema.MinimalFieldInfo(name="extra_col", data_type="String"),
            ],
            data=[
                [1, 2, 3],
                ["John", "Jane", "Bob"],
                ["A", "B", "C"]
            ]
        )
    )

    right_data = input_schema.NodeManualInput(
        flow_id=1,
        node_id=2,
        raw_data_format=input_schema.RawData(
            columns=[
                input_schema.MinimalFieldInfo(name="full_name", data_type="String"),
                input_schema.MinimalFieldInfo(name="city", data_type="String"),
                input_schema.MinimalFieldInfo(name="extra_right", data_type="String"),
            ],
            data=[
                ["Jon", "Janet", "Bobby"],
                ["NYC", "LA", "Chicago"],
                ["X", "Y", "Z"]
            ]
        )
    )

    flow.add_manual_input(left_data)
    flow.add_manual_input(right_data)

    fuzzy_node = input_schema.NodeFuzzyMatch(
        flow_id=1,
        node_id=3,
        depending_on_ids=[1, 2],
        join_input=transform_schema.FuzzyMatchInput(
            join_mapping=[
                transform_schema.FuzzyMap(
                    left_col="name",
                    right_col="full_name",
                    fuzzy_type="levenshtein",
                    threshold_score=0.6
                )
            ],
            left_select=[
                transform_schema.SelectInput("id"),
                transform_schema.SelectInput("name"),
                transform_schema.SelectInput("extra_col", keep=False),  # Drop this column
            ],
            right_select=[
                transform_schema.SelectInput("full_name"),
                transform_schema.SelectInput("city"),
                transform_schema.SelectInput("extra_right", keep=False),  # Drop this column
            ]
        )
    )
    flow.add_fuzzy_match(fuzzy_node)
    left_connection = input_schema.NodeConnection.create_from_simple_input(1, 3, 'main')
    right_connection = input_schema.NodeConnection.create_from_simple_input(2, 3, 'right')
    add_connection(flow, left_connection)
    add_connection(flow, right_connection)

    code = export_func(flow)
    # Verify that drop operations are included (polars-specific)
    if export_func is export_flow_to_polars:
        verify_code_contains(code, ".drop(")
    verify_if_execute(code)
    result_df = normalize_result(get_result_from_generated_code(code))
    expected_df = normalize_result(flow.get_node(3).get_resulting_data().data_frame)
    assert_frame_equal(result_df, expected_df, check_row_order=False)


@pytest.mark.parametrize("export_func", [
    export_flow_to_polars,
    export_flow_to_flowframe,
], ids=["polars", "flowframe"])
def test_fuzzy_match_jaro_winkler(export_func):
    """Test fuzzy match with jaro_winkler fuzzy type."""
    flow = create_basic_flow()

    left_data = input_schema.NodeManualInput(
        flow_id=1,
        node_id=1,
        raw_data_format=input_schema.RawData(
            columns=[
                input_schema.MinimalFieldInfo(name="company", data_type="String"),
            ],
            data=[["Apple Inc", "Microsoft Corp", "Google LLC"]]
        )
    )

    right_data = input_schema.NodeManualInput(
        flow_id=1,
        node_id=2,
        raw_data_format=input_schema.RawData(
            columns=[
                input_schema.MinimalFieldInfo(name="firm", data_type="String"),
            ],
            data=[["Apple Inc.", "Microsft Corporation", "Googl LLC"]]
        )
    )

    flow.add_manual_input(left_data)
    flow.add_manual_input(right_data)

    fuzzy_node = input_schema.NodeFuzzyMatch(
        flow_id=1,
        node_id=3,
        depending_on_ids=[1, 2],
        join_input=transform_schema.FuzzyMatchInput(
            join_mapping=[
                transform_schema.FuzzyMap(
                    left_col="company",
                    right_col="firm",
                    fuzzy_type="jaro_winkler",
                    threshold_score=0.8
                )
            ],
            left_select=[transform_schema.SelectInput("company")],
            right_select=[transform_schema.SelectInput("firm")]
        )
    )
    flow.add_fuzzy_match(fuzzy_node)
    left_connection = input_schema.NodeConnection.create_from_simple_input(1, 3, 'main')
    right_connection = input_schema.NodeConnection.create_from_simple_input(2, 3, 'right')
    add_connection(flow, left_connection)
    add_connection(flow, right_connection)

    code = export_func(flow)
    if export_func is export_flow_to_polars:
        verify_code_contains(code, "fuzzy_type='jaro_winkler'")
        verify_code_contains(code, "threshold_score=0.8")
    elif export_func is export_flow_to_flowframe:
        verify_code_contains(code, ".fuzzy_join(", "ff.FuzzyMapping(")
    verify_if_execute(code)


# ============================================================================
# CSV Encoding Tests
# ============================================================================

@pytest.mark.parametrize("export_func", [
    export_flow_to_polars,
    export_flow_to_flowframe,
], ids=["polars", "flowframe"])
def test_csv_read_non_utf8_encoding(tmp_path, export_func):
    """Test CSV read with non-UTF8 encoding uses read_csv instead of scan_csv."""
    flow = create_basic_flow()

    # Create CSV with Latin-1 encoding
    csv_path = tmp_path / "latin1_data.csv"
    df = pl.DataFrame({
        "name": ["José", "François", "Müller"],
        "value": [1, 2, 3]
    })
    df.write_csv(csv_path)

    read_node = input_schema.NodeRead(
        flow_id=flow.flow_id,
        node_id=1,
        received_file=input_schema.ReceivedTable(
            name="latin1_data.csv",
            path=str(csv_path),
            file_type="csv",
            table_settings=input_schema.InputCsvTable(
                delimiter=",",
                has_headers=True,
                encoding="latin-1"  # Non-UTF8 encoding
            )
        )
    )
    flow.add_read(read_node)

    code = export_func(flow)
    # Non-UTF8 should use read_csv instead of scan_csv
    if export_func is export_flow_to_polars:
        verify_code_contains(code, "pl.read_csv(")
        verify_code_contains(code, 'encoding="latin-1"')
        verify_code_contains(code, ".lazy()")
    verify_if_execute(code)


def test_csv_read_with_skip_rows(tmp_path):
    """Test CSV read with skip_rows parameter."""
    flow = create_basic_flow()

    # Create CSV with header rows to skip
    csv_path = tmp_path / "skip_rows_data.csv"
    with open(csv_path, 'w') as f:
        f.write("This is a comment line\n")
        f.write("Another comment\n")
        f.write("name,value\n")
        f.write("Alice,1\n")
        f.write("Bob,2\n")

    read_node = input_schema.NodeRead(
        flow_id=flow.flow_id,
        node_id=1,
        received_file=input_schema.ReceivedTable(
            name="skip_rows_data.csv",
            path=str(csv_path),
            file_type="csv",
            table_settings=input_schema.InputCsvTable(
                delimiter=",",
                has_headers=True,
                encoding="utf-8",
                starting_from_line=2  # Skip 2 lines
            )
        )
    )
    flow.add_read(read_node)

    code = export_flow_to_polars(flow)
    verify_code_contains(code, "skip_rows=2")
    verify_if_execute(code)
    result_df = get_result_from_generated_code(code)
    assert len(result_df.collect()) == 2


# ============================================================================
# Custom Node DataFrame Input/Output Tests
# ============================================================================

def test_custom_node_dataframe_signature_detection():
    """Test that custom nodes with DataFrame signature get correct collect/lazy handling."""
    from flowfile_core.flowfile.code_generator.code_generator import FlowGraphToPolarsConverter

    # Create a node that uses DataFrame (not LazyFrame)
    class DataFrameProcessNode:
        def process(self, *inputs: pl.DataFrame) -> pl.DataFrame:
            return inputs[0]

    flow = create_basic_flow()
    converter = FlowGraphToPolarsConverter(flow)

    needs_collect, needs_lazy = converter._check_process_method_signature(DataFrameProcessNode)

    assert needs_collect is True, "Should need collect for DataFrame input"
    assert needs_lazy is True, "Should need lazy for DataFrame output"


def test_custom_node_lazyframe_signature_detection():
    """Test that custom nodes with LazyFrame signature skip unnecessary collect/lazy."""
    from flowfile_core.flowfile.code_generator.code_generator import FlowGraphToPolarsConverter

    class LazyFrameProcessNode:
        def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
            return inputs[0]

    flow = create_basic_flow()
    converter = FlowGraphToPolarsConverter(flow)

    needs_collect, needs_lazy = converter._check_process_method_signature(LazyFrameProcessNode)

    assert needs_collect is False, "Should not need collect for LazyFrame input"
    assert needs_lazy is False, "Should not need lazy for LazyFrame output"


def test_custom_node_mixed_signature_detection():
    """Test custom node with LazyFrame input and DataFrame output."""
    from flowfile_core.flowfile.code_generator.code_generator import FlowGraphToPolarsConverter

    class MixedProcessNode:
        def process(self, *inputs: pl.LazyFrame) -> pl.DataFrame:
            return inputs[0].collect()

    flow = create_basic_flow()
    converter = FlowGraphToPolarsConverter(flow)

    needs_collect, needs_lazy = converter._check_process_method_signature(MixedProcessNode)

    assert needs_collect is False, "Should not need collect for LazyFrame input"
    assert needs_lazy is True, "Should need lazy for DataFrame output"


# ============================================================================
# Edge Cases and Error Handling Tests
# ============================================================================

def test_filter_with_empty_basic_filter():
    """Test filter with empty basic filter (no field specified) passes through unchanged."""
    flow = create_basic_flow()
    flow = create_sample_dataframe_node(flow)

    filter_node = input_schema.NodeFilter(
        flow_id=1,
        node_id=2,
        depending_on_id=1,
        filter_input=transform_schema.FilterInput(
            mode="basic",
            basic_filter=None  # Validator creates empty BasicFilter with field=''
        )
    )
    flow.add_filter(filter_node)
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2))

    code = export_flow_to_polars(flow)
    verify_code_contains(code, "# No filter applied")
    verify_if_execute(code)


@pytest.mark.parametrize("export_func", [export_flow_to_polars, export_flow_to_flowframe], ids=["polars", "flowframe"])
def test_filter_advanced_mode(export_func):
    """Test filter with advanced mode expression generates correct code."""
    flow = create_basic_flow()
    flow = create_sample_dataframe_node(flow)

    filter_node = input_schema.NodeFilter(
        flow_id=1,
        node_id=2,
        depending_on_id=1,
        filter_input=transform_schema.FilterInput(
            mode="advanced",
            advanced_filter="[age] > 25 AND [salary] > 60000"
        )
    )
    flow.add_filter(filter_node)
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2))

    code = export_func(flow)
    if export_func is export_flow_to_polars:
        verify_code_contains(code, "simple_function_to_expr")
        verify_code_contains(code, "[age] > 25 AND [salary] > 60000")
    verify_if_execute(code)
    result_df = normalize_result(get_result_from_generated_code(code))
    expected_df = normalize_result(flow.get_node(2).get_resulting_data().data_frame)
    assert_frame_equal(result_df, expected_df)


@pytest.mark.parametrize("export_func", [export_flow_to_polars, export_flow_to_flowframe], ids=["polars", "flowframe"])
def test_unique_without_columns(export_func):
    """Test unique node without specifying columns (unique on all columns)."""
    flow = create_basic_flow()

    data = input_schema.NodeManualInput(
        flow_id=1,
        node_id=1,
        raw_data_format=input_schema.RawData(
            columns=[
                input_schema.MinimalFieldInfo(name="a", data_type="Integer"),
                input_schema.MinimalFieldInfo(name="b", data_type="String")
            ],
            data=[[1, 1, 2, 2], ["x", "x", "y", "z"]]
        )
    )
    flow.add_manual_input(data)

    unique_node = input_schema.NodeUnique(
        flow_id=1,
        node_id=2,
        depending_on_id=1,
        unique_input=transform_schema.UniqueInput(
            columns=[],  # Empty columns = unique on all
            strategy="first"
        )
    )
    flow.add_unique(unique_node)
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2))

    code = export_func(flow)
    if export_func is export_flow_to_polars:
        verify_code_contains(code, "unique(keep='first')")
    verify_if_execute(code)
    result_df = normalize_result(get_result_from_generated_code(code))
    # Should have 3 unique rows: (1, x), (2, y), (2, z)
    assert len(result_df) == 3


def test_select_with_no_columns_kept():
    """Test select node where no columns are marked as keep."""
    flow = create_basic_flow()
    flow = create_sample_dataframe_node(flow)

    select_node = input_schema.NodeSelect(
        flow_id=1,
        node_id=2,
        depending_on_id=1,
        select_input=[
            transform_schema.SelectInput("id", "id", keep=False),
            transform_schema.SelectInput("name", "name", keep=False),
        ]
    )
    flow.add_select(select_node)
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2))

    code = export_flow_to_polars(flow)
    # Should just pass through the dataframe without select
    verify_code_contains(code, "df_2 = df_1")
    verify_if_execute(code)


@pytest.mark.parametrize("export_func", [export_flow_to_polars, export_flow_to_flowframe], ids=["polars", "flowframe"])
def test_group_by_concat_aggregation(export_func):
    """Test group by with concat aggregation function (special mapping)."""
    flow = create_basic_flow()
    flow = create_sales_dataframe_node(flow)

    groupby_node = input_schema.NodeGroupBy(
        flow_id=1,
        node_id=2,
        depending_on_id=1,
        groupby_input=transform_schema.GroupByInput(
            agg_cols=[
                transform_schema.AggColl("product", "groupby"),
                transform_schema.AggColl("region", "concat", "all_regions"),
            ]
        )
    )
    flow.add_group_by(groupby_node)
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2))

    code = export_func(flow)
    if export_func is export_flow_to_polars:
        # concat should be mapped to str.concat
        verify_code_contains(code, 'pl.col("region").str.concat().alias("all_regions")')
    verify_if_execute(code)


@pytest.mark.parametrize("export_func", [export_flow_to_polars, export_flow_to_flowframe], ids=["polars", "flowframe"])
def test_union_relaxed_vs_strict(export_func):
    """Test union with strict mode (diagonal) vs relaxed mode."""
    flow = create_basic_flow()

    # Add two manual inputs with different columns
    data1 = input_schema.NodeManualInput(
        flow_id=1,
        node_id=1,
        raw_data_format=input_schema.RawData(
            columns=[input_schema.MinimalFieldInfo(name="a", data_type="Integer")],
            data=[[1, 2]]
        )
    )
    flow.add_manual_input(data1)

    data2 = input_schema.NodeManualInput(
        flow_id=1,
        node_id=2,
        raw_data_format=input_schema.RawData(
            columns=[input_schema.MinimalFieldInfo(name="b", data_type="Integer")],
            data=[[3, 4]]
        )
    )
    flow.add_manual_input(data2)

    # Test strict mode
    union_node = input_schema.NodeUnion(
        flow_id=1,
        node_id=3,
        depending_on_ids=[1, 2],
        union_input=transform_schema.UnionInput(mode="selective")
    )
    flow.add_union(union_node)
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 3))
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(2, 3))

    code = export_func(flow)
    if export_func is export_flow_to_polars:
        verify_code_contains(code, "how='diagonal'")


# ========================================
# Catalog Reader Tests
# ========================================


def test_catalog_reader_by_table_name():
    """Test catalog reader code generation with a table name."""
    from flowfile_core.flowfile.code_generator.code_generator import FlowGraphToFlowFrameConverter

    flow = create_basic_flow()

    catalog_reader = input_schema.NodeCatalogReader(
        flow_id=1,
        node_id=1,
        catalog_table_name="my_table",
    )

    converter = FlowGraphToFlowFrameConverter(flow)
    converter.node_var_mapping[1] = "df_1"
    converter.last_node_var = "df_1"
    converter._handle_catalog_reader(catalog_reader, "df_1", {})

    code_output = "\n".join(converter.code_lines)
    verify_code_contains(code_output, "ff.read_catalog_table(", '"my_table"')
    assert "import flowfile as ff" in converter.imports


def test_catalog_reader_with_namespace_and_version():
    """Test catalog reader code generation with namespace and delta version."""
    from flowfile_core.flowfile.code_generator.code_generator import FlowGraphToFlowFrameConverter

    flow = create_basic_flow()

    catalog_reader = input_schema.NodeCatalogReader(
        flow_id=1,
        node_id=1,
        catalog_table_name="versioned_table",
        catalog_namespace_id=5,
        delta_version=3,
    )

    converter = FlowGraphToFlowFrameConverter(flow)
    converter.node_var_mapping[1] = "df_1"
    converter.last_node_var = "df_1"
    converter._handle_catalog_reader(catalog_reader, "df_1", {})

    code_output = "\n".join(converter.code_lines)
    verify_code_contains(
        code_output, "ff.read_catalog_table(", '"versioned_table"', "namespace_id=5", "delta_version=3"
    )


def test_catalog_reader_missing_table_name_adds_to_unsupported():
    """Test that catalog reader with no table name or ID is added to unsupported nodes."""
    from flowfile_core.flowfile.code_generator.code_generator import FlowGraphToFlowFrameConverter

    flow = create_basic_flow()

    catalog_reader = input_schema.NodeCatalogReader(
        flow_id=1,
        node_id=1,
    )

    converter = FlowGraphToFlowFrameConverter(flow)
    converter._handle_catalog_reader(catalog_reader, "df_1", {})

    assert len(converter.unsupported_nodes) == 1
    assert "no table name" in converter.unsupported_nodes[0][2].lower()


# ========================================
# Catalog Writer Tests
# ========================================


def test_catalog_writer_overwrite_mode():
    """Test catalog writer code generation with overwrite mode."""
    from flowfile_core.flowfile.code_generator.code_generator import FlowGraphToFlowFrameConverter

    flow = create_basic_flow()

    catalog_writer = input_schema.NodeCatalogWriter(
        flow_id=1,
        node_id=2,
        depending_on_id=1,
        catalog_write_settings=input_schema.CatalogWriteSettings(
            table_name="output_table",
            write_mode="overwrite",
        ),
    )

    converter = FlowGraphToFlowFrameConverter(flow)
    converter.node_var_mapping[2] = "df_2"
    converter.last_node_var = "df_2"
    converter._handle_catalog_writer(catalog_writer, "df_2", {"main": "df_1"})

    code_output = "\n".join(converter.code_lines)
    verify_code_contains(
        code_output, "ff.write_catalog_table(", "df_1,", '"output_table"',
        'write_mode="overwrite"',
    )
    assert "import flowfile as ff" in converter.imports


def test_catalog_writer_upsert_with_merge_keys():
    """Test catalog writer code generation with upsert mode and merge keys."""
    from flowfile_core.flowfile.code_generator.code_generator import FlowGraphToFlowFrameConverter

    flow = create_basic_flow()

    catalog_writer = input_schema.NodeCatalogWriter(
        flow_id=1,
        node_id=2,
        depending_on_id=1,
        catalog_write_settings=input_schema.CatalogWriteSettings(
            table_name="target_table",
            namespace_id=3,
            write_mode="upsert",
            merge_keys=["id", "name"],
            description="My upsert table",
        ),
    )

    converter = FlowGraphToFlowFrameConverter(flow)
    converter.node_var_mapping[2] = "df_2"
    converter.last_node_var = "df_2"
    converter._handle_catalog_writer(catalog_writer, "df_2", {"main": "df_1"})

    code_output = "\n".join(converter.code_lines)
    verify_code_contains(
        code_output, "ff.write_catalog_table(", '"target_table"', "namespace_id=3",
        'write_mode="upsert"', "merge_keys=[", 'description="My upsert table"',
    )


def test_catalog_writer_missing_table_name_adds_to_unsupported():
    """Test that catalog writer with no table name is added to unsupported nodes."""
    from flowfile_core.flowfile.code_generator.code_generator import FlowGraphToFlowFrameConverter

    flow = create_basic_flow()

    catalog_writer = input_schema.NodeCatalogWriter(
        flow_id=1,
        node_id=2,
        depending_on_id=1,
        catalog_write_settings=input_schema.CatalogWriteSettings(
            table_name="",
        ),
    )

    converter = FlowGraphToFlowFrameConverter(flow)
    converter._handle_catalog_writer(catalog_writer, "df_2", {"main": "df_1"})

    assert len(converter.unsupported_nodes) == 1
    assert "no table name" in converter.unsupported_nodes[0][2].lower()


def test_catalog_writer_pass_through():
    """Test that catalog writer generates pass-through assignment."""
    from flowfile_core.flowfile.code_generator.code_generator import FlowGraphToFlowFrameConverter

    flow = create_basic_flow()

    catalog_writer = input_schema.NodeCatalogWriter(
        flow_id=1,
        node_id=2,
        depending_on_id=1,
        catalog_write_settings=input_schema.CatalogWriteSettings(
            table_name="output_table",
        ),
    )

    converter = FlowGraphToFlowFrameConverter(flow)
    converter.node_var_mapping[2] = "df_2"
    converter.last_node_var = "df_2"
    converter._handle_catalog_writer(catalog_writer, "df_2", {"main": "df_1"})

    code_output = "\n".join(converter.code_lines)
    verify_code_contains(code_output, "df_2 = df_1")


# ========================================
# Catalog Code Generation Integration Tests
# ========================================


def _catalog_cleanup():
    """Remove all catalog rows so tests start clean."""
    from flowfile_core.database.connection import get_db_context
    from flowfile_core.database.models import CatalogNamespace, CatalogTable, CatalogTableReadLink, FlowRegistration

    with get_db_context() as db:
        db.query(CatalogTableReadLink).delete()
        db.query(CatalogTable).delete()
        db.query(FlowRegistration).delete()
        db.query(CatalogNamespace).delete()
        db.commit()


def _create_catalog_namespace() -> int:
    """Create a namespace hierarchy and return the schema-level id."""
    from flowfile_core.database.connection import get_db_context
    from flowfile_core.database.models import CatalogNamespace

    with get_db_context() as db:
        cat = CatalogNamespace(name="CodeGenCat", level=0, owner_id=1)
        db.add(cat)
        db.commit()
        db.refresh(cat)
        schema = CatalogNamespace(name="CodeGenSch", level=1, parent_id=cat.id, owner_id=1)
        db.add(schema)
        db.commit()
        db.refresh(schema)
        return schema.id


def _register_catalog_table(name: str, ns_id: int, data: list[dict]) -> int:
    """Write data as a parquet file, register it in the catalog, return the table id."""
    import tempfile

    from flowfile_core.catalog.repository import SQLAlchemyCatalogRepository
    from flowfile_core.catalog.service import CatalogService
    from flowfile_core.database.connection import get_db_context

    df = pl.DataFrame(data)
    tmp = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
    df.write_parquet(tmp.name)
    tmp.close()

    with get_db_context() as db:
        repo = SQLAlchemyCatalogRepository(db)
        svc = CatalogService(repo)
        table_out = svc.register_table(name=name, file_path=tmp.name, owner_id=1, namespace_id=ns_id)
    return table_out.id


def test_catalog_reader_code_executes():
    """Integration test: register a catalog table, export reader flow to code, execute it.

    Follows the pattern from test_catalog_flow_graph.py TestCatalogReader.
    """
    _catalog_cleanup()
    ns_id = _create_catalog_namespace()
    _register_catalog_table("code_gen_table", ns_id, [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}])

    # Build flow with catalog_reader (table exists, so path resolves)
    flow = create_basic_flow()
    promise = input_schema.NodePromise(flow_id=flow.flow_id, node_id=1, node_type="catalog_reader")
    flow.add_node_promise(promise)
    reader = input_schema.NodeCatalogReader(
        flow_id=flow.flow_id,
        node_id=1,
        catalog_table_name="code_gen_table",
        catalog_namespace_id=ns_id,
    )
    flow.add_catalog_reader(reader)

    # Export and execute the generated code
    code = export_flow_to_flowframe(flow)
    result = get_result_from_generated_code(code)
    if hasattr(result, "collect"):
        result = result.collect()
    assert len(result) == 2
    assert set(result.columns) == {"name", "age"}

    _catalog_cleanup()


def test_catalog_writer_code_executes():
    """Integration test: export manual_input -> catalog_writer to code, execute it, verify catalog."""
    from flowfile_core.catalog.repository import SQLAlchemyCatalogRepository
    from flowfile_core.catalog.service import CatalogService
    from flowfile_core.database.connection import get_db_context

    _catalog_cleanup()
    ns_id = _create_catalog_namespace()

    # Build flow: manual_input -> catalog_writer
    flow = create_basic_flow()
    create_sample_dataframe_node(flow, node_id=1)

    promise = input_schema.NodePromise(flow_id=flow.flow_id, node_id=2, node_type="catalog_writer")
    flow.add_node_promise(promise)
    writer = input_schema.NodeCatalogWriter(
        flow_id=flow.flow_id,
        node_id=2,
        depending_on_id=1,
        catalog_write_settings=input_schema.CatalogWriteSettings(
            table_name="written_from_code",
            namespace_id=ns_id,
            write_mode="overwrite",
        ),
        user_id=1,
    )
    flow.add_catalog_writer(writer)
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2))

    # Export and execute the generated code
    code = export_flow_to_flowframe(flow)
    verify_if_execute(code)

    # Verify the table was written to the catalog
    with get_db_context() as db:
        repo = SQLAlchemyCatalogRepository(db)
        svc = CatalogService(repo)
        tables = svc.list_tables(namespace_id=ns_id)
        assert len(tables) == 1
        assert tables[0].name == "written_from_code"
        assert tables[0].row_count == 5

    _catalog_cleanup()


def test_catalog_round_trip_code_generation():
    """Integration test: write to catalog via generated code, then read back via generated code."""
    _catalog_cleanup()
    ns_id = _create_catalog_namespace()

    # Step 1: Write data to catalog via generated code
    write_flow = create_basic_flow()
    create_sample_dataframe_node(write_flow, node_id=1)

    promise = input_schema.NodePromise(flow_id=write_flow.flow_id, node_id=2, node_type="catalog_writer")
    write_flow.add_node_promise(promise)
    writer = input_schema.NodeCatalogWriter(
        flow_id=write_flow.flow_id,
        node_id=2,
        depending_on_id=1,
        catalog_write_settings=input_schema.CatalogWriteSettings(
            table_name="roundtrip_code_table",
            namespace_id=ns_id,
            write_mode="overwrite",
        ),
        user_id=1,
    )
    write_flow.add_catalog_writer(writer)
    add_connection(write_flow, input_schema.NodeConnection.create_from_simple_input(1, 2))

    write_code = export_flow_to_flowframe(write_flow)
    verify_if_execute(write_code)

    # Step 2: Read it back via generated code
    read_flow = create_basic_flow(flow_id=2, name="read_flow")
    read_promise = input_schema.NodePromise(flow_id=read_flow.flow_id, node_id=1, node_type="catalog_reader")
    read_flow.add_node_promise(read_promise)
    reader = input_schema.NodeCatalogReader(
        flow_id=read_flow.flow_id,
        node_id=1,
        catalog_table_name="roundtrip_code_table",
        catalog_namespace_id=ns_id,
    )
    read_flow.add_catalog_reader(reader)

    read_code = export_flow_to_flowframe(read_flow)
    result = get_result_from_generated_code(read_code)
    if hasattr(result, "collect"):
        result = result.collect()
    assert len(result) == 5
    assert set(result.columns) == {"id", "name", "age", "city", "salary"}

    _catalog_cleanup()


def test_kafka_source_with_connection_name():
    """Test kafka source code generation with a named connection."""
    from flowfile_core.flowfile.code_generator.code_generator import FlowGraphToFlowFrameConverter

    flow = create_basic_flow()

    kafka_source = input_schema.NodeKafkaSource(
        flow_id=1,
        node_id=1,
        kafka_settings=input_schema.KafkaSourceSettings(
            kafka_connection_name="my_kafka",
            topic_name="events",
        ),
    )

    converter = FlowGraphToFlowFrameConverter(flow)
    converter.node_var_mapping[1] = "df_1"
    converter.last_node_var = "df_1"
    converter._handle_kafka_source(kafka_source, "df_1", {})

    code_output = "\n".join(converter.code_lines)
    verify_code_contains(code_output, "ff.read_kafka(", '"my_kafka"', 'topic_name="events"')
    assert "import flowfile as ff" in converter.imports


def test_kafka_source_with_all_parameters():
    """Test kafka source code generation with non-default parameters."""
    from flowfile_core.flowfile.code_generator.code_generator import FlowGraphToFlowFrameConverter

    flow = create_basic_flow()

    kafka_source = input_schema.NodeKafkaSource(
        flow_id=1,
        node_id=1,
        kafka_settings=input_schema.KafkaSourceSettings(
            kafka_connection_name="my_kafka",
            topic_name="events",
            max_messages=50_000,
            start_offset="earliest",
            poll_timeout_seconds=60.0,
        ),
    )

    converter = FlowGraphToFlowFrameConverter(flow)
    converter.node_var_mapping[1] = "df_1"
    converter.last_node_var = "df_1"
    converter._handle_kafka_source(kafka_source, "df_1", {})

    code_output = "\n".join(converter.code_lines)
    verify_code_contains(code_output, "ff.read_kafka(", "max_messages=50000", 'start_offset="earliest"', "poll_timeout_seconds=60.0")


def test_kafka_source_default_parameters_omitted():
    """Test that kafka source omits parameters with default values."""
    from flowfile_core.flowfile.code_generator.code_generator import FlowGraphToFlowFrameConverter

    flow = create_basic_flow()

    kafka_source = input_schema.NodeKafkaSource(
        flow_id=1,
        node_id=1,
        kafka_settings=input_schema.KafkaSourceSettings(
            kafka_connection_name="my_kafka",
            topic_name="events",
            max_messages=100_000,
            start_offset="latest",
            poll_timeout_seconds=30.0,
        ),
    )

    converter = FlowGraphToFlowFrameConverter(flow)
    converter.node_var_mapping[1] = "df_1"
    converter.last_node_var = "df_1"
    converter._handle_kafka_source(kafka_source, "df_1", {})

    code_output = "\n".join(converter.code_lines)
    assert "max_messages" not in code_output
    assert "start_offset" not in code_output
    assert "poll_timeout_seconds" not in code_output


def test_kafka_source_missing_connection_adds_to_unsupported():
    """Test that kafka source with no connection is added to unsupported nodes."""
    from flowfile_core.flowfile.code_generator.code_generator import FlowGraphToFlowFrameConverter

    flow = create_basic_flow()

    kafka_source = input_schema.NodeKafkaSource(
        flow_id=1,
        node_id=1,
        kafka_settings=input_schema.KafkaSourceSettings(
            topic_name="events",
        ),
    )

    converter = FlowGraphToFlowFrameConverter(flow)
    converter._handle_kafka_source(kafka_source, "df_1", {})

    assert len(converter.unsupported_nodes) == 1
    assert "no connection" in converter.unsupported_nodes[0][2].lower()


def test_kafka_source_id_only_adds_to_unsupported():
    """Test that kafka source with only connection ID (no name) is added to unsupported nodes."""
    from flowfile_core.flowfile.code_generator.code_generator import FlowGraphToFlowFrameConverter

    flow = create_basic_flow()

    kafka_source = input_schema.NodeKafkaSource(
        flow_id=1,
        node_id=1,
        kafka_settings=input_schema.KafkaSourceSettings(
            kafka_connection_id=42,
            topic_name="events",
        ),
    )

    converter = FlowGraphToFlowFrameConverter(flow)
    converter._handle_kafka_source(kafka_source, "df_1", {})

    assert len(converter.unsupported_nodes) == 1
    assert "named connection" in converter.unsupported_nodes[0][2].lower()


def test_cloud_storage_reader_handler_unified():
    """Test cloud storage reader generates unified read_from_cloud_storage call."""
    from flowfile_core.flowfile.code_generator.code_generator import FlowGraphToFlowFrameConverter

    flow = create_basic_flow()
    settings = input_schema.NodeCloudStorageReader(
        flow_id=1,
        node_id=1,
        cloud_storage_settings=cloud_ss.CloudStorageReadSettings(
            resource_path="s3://bucket/data.parquet",
            connection_name="my_conn",
            file_format="parquet",
        ),
    )

    converter = FlowGraphToFlowFrameConverter(flow)
    converter.node_var_mapping[1] = "df_1"
    converter.last_node_var = "df_1"
    converter._handle_cloud_storage_reader(settings, "df_1", {})

    code_output = "\n".join(converter.code_lines)
    verify_code_contains(code_output, "ff.read_from_cloud_storage(", "s3://bucket/data.parquet", "my_conn")
    assert "import flowfile as ff" in converter.imports
    # Should NOT contain old format-specific calls
    assert "scan_parquet_from_cloud_storage" not in code_output
    assert "scan_csv_from_cloud_storage" not in code_output


def test_cloud_storage_reader_polars_unsupported():
    """Test that cloud storage reader is unsupported in Polars mode."""
    from flowfile_core.flowfile.code_generator.code_generator import FlowGraphToPolarsConverter

    flow = create_basic_flow()
    settings = input_schema.NodeCloudStorageReader(
        flow_id=1,
        node_id=1,
        cloud_storage_settings=cloud_ss.CloudStorageReadSettings(
            resource_path="s3://bucket/data.parquet",
            connection_name="my_conn",
            file_format="parquet",
        ),
    )

    converter = FlowGraphToPolarsConverter(flow)
    converter._handle_cloud_storage_reader(settings, "df_1", {})

    assert len(converter.unsupported_nodes) == 1
    assert "not supported by polars" in converter.unsupported_nodes[0][2].lower()


def test_cloud_storage_writer_handler_unified():
    """Test cloud storage writer generates unified write_to_cloud_storage call."""
    from flowfile_core.flowfile.code_generator.code_generator import FlowGraphToFlowFrameConverter

    flow = create_basic_flow()
    settings = input_schema.NodeCloudStorageWriter(
        flow_id=1,
        node_id=2,
        user_id=1,
        cloud_storage_settings=cloud_ss.CloudStorageWriteSettings(
            resource_path="s3://bucket/output.parquet",
            connection_name="my_conn",
            file_format="parquet",
        ),
    )

    converter = FlowGraphToFlowFrameConverter(flow)
    converter.node_var_mapping[1] = "df_1"
    converter.last_node_var = "df_1"
    converter._handle_cloud_storage_writer(settings, "df_2", {"main": "df_1"})

    code_output = "\n".join(converter.code_lines)
    verify_code_contains(code_output, "ff.write_to_cloud_storage(", "s3://bucket/output.parquet", "my_conn")
    assert "import flowfile as ff" in converter.imports
    # Should NOT contain old FlowFrame wrapper pattern
    assert "ff.FlowFrame(" not in code_output
    # Should have pass-through assignment
    assert "df_2 = df_1" in code_output


def test_cloud_storage_writer_polars_unsupported():
    """Test that cloud storage writer is unsupported in Polars mode."""
    from flowfile_core.flowfile.code_generator.code_generator import FlowGraphToPolarsConverter

    flow = create_basic_flow()
    settings = input_schema.NodeCloudStorageWriter(
        flow_id=1,
        node_id=2,
        user_id=1,
        cloud_storage_settings=cloud_ss.CloudStorageWriteSettings(
            resource_path="s3://bucket/output.parquet",
            connection_name="my_conn",
            file_format="parquet",
        ),
    )

    converter = FlowGraphToPolarsConverter(flow)
    converter._handle_cloud_storage_writer(settings, "df_2", {"main": "df_1"})

    assert len(converter.unsupported_nodes) == 1
    assert "not supported by polars" in converter.unsupported_nodes[0][2].lower()


def test_kafka_source_polars_unsupported():
    """Test that kafka source is unsupported in Polars mode."""
    from flowfile_core.flowfile.code_generator.code_generator import FlowGraphToPolarsConverter

    flow = create_basic_flow()

    kafka_source = input_schema.NodeKafkaSource(
        flow_id=1,
        node_id=1,
        kafka_settings=input_schema.KafkaSourceSettings(
            kafka_connection_name="my_kafka",
            topic_name="events",
        ),
    )

    converter = FlowGraphToPolarsConverter(flow)
    converter._handle_kafka_source(kafka_source, "df_1", {})

    assert len(converter.unsupported_nodes) == 1
    assert "not supported by polars" in converter.unsupported_nodes[0][2].lower()


@pytest.mark.kafka
@pytest.mark.skipif(not is_docker_available(), reason="Docker is not available")
def test_kafka_source_code_executes():
    """Test that kafka_source code generation produces executable code that reads from Redpanda."""
    from flowfile_core.database.connection import get_db_context
    from flowfile_core.database.models import KafkaConnection
    from flowfile_core.kafka.connection_manager import store_kafka_connection
    from flowfile_core.schemas.kafka_schemas import KafkaConnectionCreate
    from test_utils.kafka.fixtures import (
        BOOTSTRAP_SERVERS,
        REDPANDA_CONTAINER_NAME,
        create_topic,
        is_container_running,
        produce_json_messages,
        start_redpanda_container,
    )
    # Ensure Redpanda is running
    if not is_container_running(REDPANDA_CONTAINER_NAME):
        if not start_redpanda_container():
            pytest.skip("Could not start Redpanda")

    # Clean up any existing kafka connections
    with get_db_context() as db:
        db.query(KafkaConnection).delete()
        db.commit()

    try:
        # Store a named Kafka connection in DB
        with get_db_context() as db:
            conn = store_kafka_connection(
                db,
                KafkaConnectionCreate(
                    connection_name="test-codegen-kafka",
                    bootstrap_servers=BOOTSTRAP_SERVERS,
                    security_protocol="PLAINTEXT",
                ),
                user_id=1,
            )
        connection_id = conn.id

        # Create topic and produce messages
        topic_name = f"codegen_test_{uuid4().hex[:8]}"
        create_topic(topic_name, num_partitions=1)
        messages = [
            {"name": "Alice", "age": 30},
            {"name": "Bob", "age": 25},
            {"name": "Charlie", "age": 35},
        ]
        produce_json_messages(topic_name, messages)

        # Build flow graph with kafka_source node
        flow = create_basic_flow()
        promise = input_schema.NodePromise(flow_id=1, node_id=1, node_type="kafka_source")
        flow.add_node_promise(promise)
        kafka_settings = input_schema.KafkaSourceSettings(
            kafka_connection_id=connection_id,
            kafka_connection_name="test-codegen-kafka",
            topic_name=topic_name,
            start_offset="earliest",
            poll_timeout_seconds=10.0,
        )
        node_kafka = input_schema.NodeKafkaSource(
            flow_id=1,
            node_id=1,
            kafka_settings=kafka_settings,
            user_id=1,
        )
        flow.add_kafka_source(node_kafka)

        # Export to code
        code = export_flow_to_flowframe(flow)
        assert "ff.read_kafka(" in code
        assert "test-codegen-kafka" in code
        assert topic_name in code

        # Execute generated code and verify
        result = get_result_from_generated_code(code)
        assert isinstance(result, (pl.DataFrame, pl.LazyFrame, FlowFrame))
        if isinstance(result, FlowFrame):
            result = result.collect()
        elif isinstance(result, pl.LazyFrame):
            result = result.collect()
        assert len(result) == 3
        assert "name" in result.columns
        assert "age" in result.columns
        assert set(result["name"].to_list()) == {"Alice", "Bob", "Charlie"}
    finally:
        # Cleanup
        with get_db_context() as db:
            db.query(KafkaConnection).delete()
            db.commit()


if __name__ == "__main__":
    pytest.main([__file__])
