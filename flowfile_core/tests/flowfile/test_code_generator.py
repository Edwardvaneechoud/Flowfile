import pytest
from typing import List, Tuple
import polars as pl
from polars.testing import assert_frame_equal
from pathlib import Path
from uuid import uuid4
from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection
from flowfile_core.schemas import input_schema, transform_schema, schemas, cloud_storage_schemas as cloud_ss
from flowfile_core.flowfile.code_generator.code_generator import export_flow_to_polars
from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine

try:
    import os
    from tests.flowfile_core_test_utils import (is_docker_available, ensure_password_is_available)
    from tests.utils import ensure_cloud_storage_connection_is_available_and_get_connection, get_cloud_connection
except ModuleNotFoundError:
    import os
    import sys
    sys.path.append(os.path.dirname(os.path.abspath("flowfile_core/tests/flowfile_core_test_utils.py")))
    sys.path.append(os.path.dirname(os.path.abspath("flowfile_core/tests/utils.py")))
    # noinspection PyUnresolvedReferences
    from flowfile_core_test_utils import (is_docker_available, ensure_password_is_available)
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
        raise Exception(f"Code execution should not raise an exception:\n {e}")


def get_result_from_generated_code(code: str) -> pl.DataFrame | pl.LazyFrame | List[pl.DataFrame | pl.LazyFrame] | None:
    exec_globals = {}
    exec(code, exec_globals)
    return exec_globals['run_etl_pipeline']()


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
        received_file=input_schema.ReceivedTable.model_validate(input_schema.ReceivedCsvTable(
            name=filename,
            path=str(csv_path),
            file_type="csv",
            delimiter=",",
            has_headers=True,
            encoding="utf-8"
        ).__dict__)
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


@pytest.mark.parametrize("test_name,join_scenario",  generate_parameterized_join_tests())
def test_join_operation(test_name, join_scenario, join_input_dataset):
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

    # Convert to Polars code and verify
    code = export_flow_to_polars(flow)
    verify_if_execute(code)
    result = get_result_from_generated_code(code)
    expected_df = flow.get_node(3).get_resulting_data().data_frame
    assert_frame_equal(result, expected_df, check_column_order=False, check_row_order=False)


@pytest.mark.parametrize("test_name,join_scenario",  generate_parameterized_join_tests_same_df())
def test_join_operation_same_df(test_name, join_scenario, join_input_same_df):
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

    # Convert to Polars code and verify
    code = export_flow_to_polars(flow)
    verify_if_execute(code)
    result = get_result_from_generated_code(code)
    expected_df = flow.get_node(2).get_resulting_data().data_frame
    assert_frame_equal(result, expected_df, check_column_order=False, check_row_order=False)


def test_simple_manual_input():
    flow = create_basic_flow()
    flow = create_sample_dataframe_node(flow)
    code = export_flow_to_polars(flow)
    verify_if_execute(code)
    result = get_result_from_generated_code(code)
    assert_frame_equal(result, get_reference_polars_dataframe())


# Test functions
def test_simple_csv_read_and_filter(tmp_path):
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
    # Convert to Polars code
    code = export_flow_to_polars(flow)
    verify_if_execute(code)
    result_df = get_result_from_generated_code(code)
    expected_df = flow.get_node(2).get_resulting_data().data_frame
    assert_frame_equal(result_df, expected_df)


def test_manual_input_with_select():
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
    # Convert to Polars code
    code = export_flow_to_polars(flow)
    # Verify the generated code
    verify_code_contains(code,
                         'pl.col("name").alias("full_name")',
                         'pl.col("age")',
                         'pl.col("salary").cast(pl.Float64)'
                         )
    # Verify city is not selected
    assert 'pl.col("city")' not in code.split("df_2 = ")[1].split("\n")[0]
    verify_if_execute(code)
    result = get_result_from_generated_code(code)
    expected_result = (get_reference_polars_dataframe()
                       .select(pl.col('name').alias("full_name"), "age", pl.col("salary").cast(pl.Float64))
                       )
    assert_frame_equal(result, expected_result)


def test_number_of_records():
    """Test manual data input with column selection and renaming"""
    flow = create_basic_flow()
    flow = create_sample_dataframe_node(flow)
    record_count_node = input_schema.NodeRecordCount(flow_id=1, node_id=2, depending_on_id=1)

    flow.add_record_count(record_count_node)
    add_connection(flow, node_connection=input_schema.NodeConnection.create_from_simple_input(1, 2))
    # Convert to Polars code
    code = export_flow_to_polars(flow)
    # Verify the generated code

    verify_if_execute(code)
    result = get_result_from_generated_code(code)
    expected_result = flow.get_node(2).get_resulting_data().data_frame
    assert_frame_equal(result, expected_result)


def test_graph_solver():
    flow = create_basic_flow()
    create_sample_dataframe_for_graph_solver(flow)
    graph_solver_input = transform_schema.GraphSolverInput(col_from='from', col_to='to', output_column_name='g')
    flow.add_graph_solver(input_schema.NodeGraphSolver(flow_id=1, node_id=2, graph_solver_input=graph_solver_input))
    add_connection(flow, node_connection=input_schema.NodeConnection.create_from_simple_input(1, 2))
    code = export_flow_to_polars(flow)

    verify_if_execute(code)
    result = get_result_from_generated_code(code)
    expected_result = flow.get_node(2).get_resulting_data().data_frame
    assert_frame_equal(result, expected_result)


def test_join_operation_left(join_input_dataset):
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

    # Convert to Polars code
    code = export_flow_to_polars(flow)

    # Verify join code
    verify_code_contains(code,
                         "df_1.join(",
                         "df_2,",
                         'left_on=["id"]',
                         'right_on=["id"]',
                         'how="left"'
                         )
    verify_if_execute(code)
    result = get_result_from_generated_code(code)
    expected_df = flow.get_node(3).get_resulting_data().data_frame
    assert_frame_equal(result, expected_df)


def test_join_operation_right(join_input_dataset):
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

    # Convert to Polars code
    code = export_flow_to_polars(flow)
    # Verify join code
    verify_code_contains(code,
                         "df_1.join(",
                         "df_2,",
                         'left_on=["__jk_id"]',
                         'right_on=["id_right"]',
                         'how="right"'
                         )
    verify_if_execute(code)
    result = get_result_from_generated_code(code)
    expected_df = flow.get_node(3).get_resulting_data().data_frame
    assert_frame_equal(result, expected_df, check_column_order=False, check_row_order=False)


def create_comprehensive_join_scenarios() -> List[Tuple[str, input_schema.NodeJoin]]:
    """Generate comprehensive join test scenarios including complex cases"""

    join_scenarios: List[Tuple[str, input_schema.NodeJoin]] = []
    how_options: List[transform_schema.JoinStrategy] = ["left", "right", "inner", "outer"]

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


def create_comprehensive_anti_semi_join_scenarios() -> List[Tuple[str, input_schema.NodeJoin]]:
    """Generate comprehensive join test scenarios including complex cases"""

    join_scenarios: List[Tuple[str, input_schema.NodeJoin]] = []
    how_options: List[transform_schema.JoinStrategy] = ["semi", "anti"]

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



@pytest.mark.parametrize("test_name, join_scenario", create_comprehensive_join_scenarios())
def test_join_operation_complex(test_name, join_scenario, join_input_large_dataset):
    flow = create_basic_flow()
    # if test_name == 'right_unselect_all_right_overlapping_col_left_multi_join':
    left_data, right_data = join_input_large_dataset
    flow.add_manual_input(left_data)
    flow.add_manual_input(right_data)
    flow.add_join(join_scenario)
    left_connection = input_schema.NodeConnection.create_from_simple_input(1, 3, 'main')
    right_connection = input_schema.NodeConnection.create_from_simple_input(2, 3, 'right')
    add_connection(flow, left_connection)
    add_connection(flow, right_connection)

    # Convert to Polars code
    code = export_flow_to_polars(flow)
    result = get_result_from_generated_code(code)
    expected_df = flow.get_node(3).get_resulting_data().data_frame
    assert_frame_equal(result, expected_df, check_column_order=False, check_row_order=False)


@pytest.mark.parametrize("test_name, join_scenario", create_comprehensive_anti_semi_join_scenarios())
def test_semi_and_anti_join(test_name, join_scenario, join_input_large_dataset):
    flow = create_basic_flow()
    # if test_name == 'right_unselect_all_right_overlapping_col_left_multi_join':
    left_data, right_data = join_input_large_dataset
    flow.add_manual_input(left_data)
    flow.add_manual_input(right_data)
    flow.add_join(join_scenario)
    left_connection = input_schema.NodeConnection.create_from_simple_input(1, 3, 'main')
    right_connection = input_schema.NodeConnection.create_from_simple_input(2, 3, 'right')
    add_connection(flow, left_connection)
    add_connection(flow, right_connection)

    # Convert to Polars code
    code = export_flow_to_polars(flow)
    result = get_result_from_generated_code(code)
    expected_df = flow.get_node(3).get_resulting_data().data_frame
    assert_frame_equal(result, expected_df, check_column_order=False, check_row_order=False)


def test_join_operation_left_rename(join_input_dataset):
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

    # Convert to Polars code
    code = export_flow_to_polars(flow)

    verify_if_execute(code)
    result = get_result_from_generated_code(code)
    expected_df = flow.get_node(3).get_resulting_data().data_frame
    assert_frame_equal(result, expected_df, check_column_order=False, check_row_order=False)


def test_group_by_aggregation():
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

    # Convert to Polars code
    code = export_flow_to_polars(flow)

    # Verify group by code
    verify_code_contains(code,
                         'group_by(["product", "region"])',
                         'pl.col("quantity").sum().alias("total_quantity")',
                         'pl.col("price").mean().alias("avg_price")',
                         'pl.col("quantity").count().alias("num_transactions")'
                         )
    verify_if_execute(code)
    result_df = get_result_from_generated_code(code)
    expected_df = (flow.get_node(1).get_resulting_data().data_frame
                   .group_by(['product', 'region'])
                   .agg([pl.col("quantity").sum().alias("total_quantity"),
                         pl.col("price").mean().alias("avg_price"),
                         pl.col("quantity").count().alias("num_transactions")]))
    assert_frame_equal(result_df, expected_df, check_row_order=False)


def test_formula_node_cast():
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

    # Convert to Polars code
    code = export_flow_to_polars(flow)
    # Verify formula code
    verify_code_contains(code,
                         "with_columns",
                         "from polars_expr_transformer.process.polars_expr_transformer import simple_function_to_expr",
                         "df_2 = df_1.with_columns(",
                         'alias("total")',
                         'cast(pl.Int64)'
                         )
    verify_if_execute(code)
    result_df = get_result_from_generated_code(code)
    expected_df = flow.get_node(2).get_resulting_data().data_frame
    assert_frame_equal(result_df, expected_df)


def test_formula_node():
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

    # Convert to Polars code
    code = export_flow_to_polars(flow)
    # Verify formula code
    verify_code_contains(code,
                         "with_columns",
                         "from polars_expr_transformer.process.polars_expr_transformer import simple_function_to_expr",
                         "df_2 = df_1.with_columns(",
                         'alias("total")',
                         )
    verify_if_execute(code)
    result_df = get_result_from_generated_code(code)
    expected_df = flow.get_node(2).get_resulting_data().data_frame
    assert_frame_equal(result_df, expected_df)


def test_pivot_operation():
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

    # Convert to Polars code
    code = export_flow_to_polars(flow)

    # Verify pivot code
    verify_code_contains(code,
                         "pivot(",
                         "values='quantity'",
                         'index=["date"]',
                         "columns='product'",
                         "aggregate_function='sum'"
                         )
    verify_if_execute(code)
    result_df = get_result_from_generated_code(code)
    expected_df = flow.get_node(2).get_resulting_data().data_frame
    assert_frame_equal(result_df, expected_df, check_row_order=False)


def test_pivot_no_index_operation():
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
    # Convert to Polars code
    code = export_flow_to_polars(flow)
    # Verify pivot code
    verify_code_contains(code,
                         "pivot(",
                         "values='quantity'",
                         'index=["__temp_index__"]',
                         "columns='product'",
                         "aggregate_function='sum'"
                         )
    verify_if_execute(code)
    result_df = get_result_from_generated_code(code)
    expected_df = flow.get_node(2).get_resulting_data().data_frame
    assert_frame_equal(result_df, expected_df, check_row_order=False)


def test_union_multiple_dataframes():
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

    code = export_flow_to_polars(flow)

    # Verify union code
    verify_code_contains(code,
                         "pl.concat([",
                         "df_1,",
                         "df_2,",
                         "df_3,",
                         "how='diagonal_relaxed'"
                         )
    verify_if_execute(code)
    result_df = get_result_from_generated_code(code)
    expected_df = flow.get_node(4).get_resulting_data().data_frame
    assert_frame_equal(expected_df, result_df, check_row_order=False)


def test_custom_polars_code():
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

    # Convert to Polars code
    code = export_flow_to_polars(flow)

    # Verify custom code handling
    verify_code_contains(code,
                         "def _polars_code_2(input_df: pl.LazyFrame):",
                         "return input_df.with_columns((pl.col('age') * 2).alias('double_age'))",
                         "df_2 = _polars_code_2(df_1)"
                         )
    verify_if_execute(code)
    result_df = get_result_from_generated_code(code)
    expected_df = flow.get_node(2).get_resulting_data().data_frame
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


@pytest.mark.parametrize("polars_code, test_id", polars_test_cases)
def test_code(polars_code, test_id):
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

    # Convert to Polars code
    code = export_flow_to_polars(flow)

    verify_if_execute(code)
    result_df = get_result_from_generated_code(code)
    expected_df = flow.get_node(2).get_resulting_data().data_frame
    assert_frame_equal(expected_df, result_df, check_row_order=False)


def test_formula_with_string():
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

    # Convert to Polars code
    code = export_flow_to_polars(flow)
    verify_if_execute(code)
    result_df = get_result_from_generated_code(code)
    expected_df = flow.get_node(2).get_resulting_data().data_frame
    assert_frame_equal(expected_df, result_df, check_row_order=False)


def test_custom_polars_code_multiple_inputs():
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
    # Convert to Polars code
    code = export_flow_to_polars(flow)

    # Verify custom code handling
    verify_code_contains(code,
                         "def _polars_code_3(input_df_1: pl.LazyFrame, input_df_2: pl.LazyFrame):",
                         "output_df = input_df_1.join(input_df_2, how='cross')",
                         "return output_df",
                         "df_3 = _polars_code_3(df_1, df_2)"
                         )
    verify_if_execute(code)
    result_df = get_result_from_generated_code(code)
    expected_df = flow.get_node(3).get_resulting_data().data_frame
    assert_frame_equal(expected_df, result_df, check_row_order=False)


def test_custom_polars_no_inputs():
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
    # Convert to Polars code
    code = export_flow_to_polars(flow)

    verify_if_execute(code)
    result_df = get_result_from_generated_code(code)
    expected_df = flow.get_node(1).get_resulting_data().data_frame
    assert_frame_equal(expected_df, result_df, check_row_order=False)  # TODO: ensure that the polars_code with


def test_complex_workflow(tmp_path):
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
            output_csv_table=input_schema.OutputCsvTable(),
            output_parquet_table=input_schema.OutputParquetTable(),
            output_excel_table=input_schema.OutputExcelTable()
        )
    )
    flow.add_output(output_node)
    add_connection(flow, node_connection=input_schema.NodeConnection.create_from_simple_input(4, 5))
    # Convert to Polars code
    code = export_flow_to_polars(flow)

    # Verify the complete workflow is represented
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
    result_df = get_result_from_generated_code(code)
    expected_df = flow.get_node(4).get_resulting_data().data_frame
    assert_frame_equal(result_df, expected_df, check_row_order=False)


def test_complex_workflow_unordered(tmp_path):
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
            output_csv_table=input_schema.OutputCsvTable(),
            output_parquet_table=input_schema.OutputParquetTable(),
            output_excel_table=input_schema.OutputExcelTable()
        )
    )
    flow.add_output(output_node)
    # Convert to Polars code

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

    code = export_flow_to_polars(flow)
    # Verify the complete workflow is represented
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
    result_df = get_result_from_generated_code(code)
    expected_df = flow.get_node(4).get_resulting_data().data_frame
    assert_frame_equal(result_df, expected_df, check_row_order=False)


def test_text_to_rows_operation():
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
    # Convert to Polars code
    code = export_flow_to_polars(flow)

    # Verify text to rows code
    verify_code_contains(code,
                         'str.split(",")',
                         'alias("tag")',
                         "explode('tag')"
                         )
    verify_if_execute(code)
    result_df = get_result_from_generated_code(code)
    expected_df = flow.get_node(2).get_resulting_data().data_frame
    assert_frame_equal(result_df, expected_df)


def test_text_to_rows_operation_no_rename():
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
    # Convert to Polars code
    code = export_flow_to_polars(flow)

    # Verify text to rows code
    verify_code_contains(code,
                         'str.split(",")',
                         "explode('tags')"
                         )
    verify_if_execute(code)
    result_df = get_result_from_generated_code(code)
    expected_df = flow.get_node(2).get_resulting_data().data_frame
    assert_frame_equal(result_df, expected_df)


def test_sort_operation():
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

    # Convert to Polars code
    code = export_flow_to_polars(flow)

    # Verify sort and unique code
    verify_code_contains(code,
                         'sort(["score", "name"], descending=[True, False])',
                         )
    verify_if_execute(code)
    result_df = get_result_from_generated_code(code)
    expected_df = flow.get_node(2).get_resulting_data().data_frame
    assert_frame_equal(result_df, expected_df)


def test_sort_and_unique_operations():
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

    # Convert to Polars code
    code = export_flow_to_polars(flow)

    # Verify sort and unique code
    verify_code_contains(code,
                         'sort(["score", "name"], descending=[True, False])',
                         "unique(subset=['name'], keep='first')"
                         )
    verify_if_execute(code)
    result_df = get_result_from_generated_code(code)
    expected_df = flow.get_node(3).get_resulting_data().data_frame
    assert_frame_equal(result_df, expected_df, check_row_order=False)


def test_record_id_generation_with_grouping():
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

    # Convert to Polars code
    code = export_flow_to_polars(flow)

    verify_if_execute(code)
    result_df = get_result_from_generated_code(code)
    expected_df = flow.get_node(2).get_resulting_data().data_frame
    assert_frame_equal(result_df, expected_df, check_row_order=False)


def test_record_id_generation_without_grouping():
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

    # Convert to Polars code
    code = export_flow_to_polars(flow)

    verify_if_execute(code)
    result_df = get_result_from_generated_code(code)
    expected_df = flow.get_node(2).get_resulting_data().data_frame
    assert_frame_equal(result_df, expected_df, check_row_order=False)


def test_sample_operation():
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

    # Convert to Polars code
    code = export_flow_to_polars(flow)

    # Verify sample code
    verify_code_contains(code, "head(n=3)")
    verify_if_execute(code)
    result_df = get_result_from_generated_code(code)
    expected_df = flow.get_node(2).get_resulting_data().data_frame
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


def test_cross_join_operation():
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
    # Convert to Polars code
    code = export_flow_to_polars(flow)

    # Verify cross join code
    verify_code_contains(code,
                         "join(",
                         "how='cross'"
                         )
    verify_if_execute(code)
    result_df = get_result_from_generated_code(code)
    expected_df = flow.get_node(3).get_resulting_data().data_frame
    assert_frame_equal(result_df, expected_df, check_row_order=False)


def test_cross_join_operation_equal_column():
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
    # Convert to Polars code
    code = export_flow_to_polars(flow)

    # Verify cross join code
    verify_code_contains(code,
                         "join(",
                         "how='cross'"
                         )
    verify_if_execute(code)
    result_df = get_result_from_generated_code(code)
    expected_df = flow.get_node(3).get_resulting_data().data_frame
    assert_frame_equal(result_df, expected_df, check_row_order=False)


def test_unpivot_operation():
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
    # Convert to Polars code
    code = export_flow_to_polars(flow)

    # Verify unpivot code
    verify_code_contains(code,
                         "unpivot(",
                         'index=["id"]',
                         'on=["jan", "feb", "mar"]',
                         "variable_name='variable'",
                         "value_name='value'"
                         )
    verify_if_execute(code)
    result_df = get_result_from_generated_code(code)
    expected_df = flow.get_node(2).get_resulting_data().data_frame
    assert_frame_equal(result_df, expected_df, check_row_order=False)


def test_multiple_output_formats(tmp_path):
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
            output_csv_table=input_schema.OutputCsvTable(delimiter="|"),
            output_parquet_table=input_schema.OutputParquetTable(),
            output_excel_table=input_schema.OutputExcelTable()
        )
    )
    flow.add_output(csv_output)
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2))

    code = export_flow_to_polars(flow)
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
            output_csv_table=input_schema.OutputCsvTable(),
            output_parquet_table=input_schema.OutputParquetTable(),
            output_excel_table=input_schema.OutputExcelTable(sheet_name="Results")
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
            output_csv_table=input_schema.OutputCsvTable(),
            output_parquet_table=input_schema.OutputParquetTable(),
            output_excel_table=input_schema.OutputExcelTable()
        )
    )
    flow.add_output(parquet_output)
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 4))
    code = export_flow_to_polars(flow)
    verify_if_execute(code)
    try:
        pl.read_csv(str(tmp_path) + os.sep + "output.csv", separator="|")
    except Exception as e:
        raise Exception("Could not read the CSV file that should have been written")
    try:
        pl.read_excel(str(tmp_path) + os.sep + "output.xlsx", sheet_name='Results')
    except Exception as e:
        raise Exception("Could not read the xlsx file that should have been written")
    try:
        pl.read_parquet(str(tmp_path) + os.sep + "output.parquet")
    except Exception as e:
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


def test_data_type_conversions():
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
    # Convert to Polars code
    code = export_flow_to_polars(flow)

    # Verify type conversions
    verify_code_contains(code,
                         'pl.col("age").alias("age_float").cast(pl.Float64)',
                         'pl.col("salary").alias("salary_int").cast(pl.Int32)',
                         'pl.col("id").alias("id_str").cast(pl.Utf8)'
                         )
    verify_if_execute(code)
    result_df = get_result_from_generated_code(code)
    expected_df = flow.get_node(2).get_resulting_data().data_frame
    assert_frame_equal(result_df, expected_df)


def test_csv_read_utf_8():
    """Test reading parquet files"""
    flow = create_basic_flow()
    flowfile_core_path = find_parent_directory('Flowfile')

    file_path = str((Path(flowfile_core_path) / 'flowfile_core' / 'tests' / 'support_files' / 'data' / 'fake_data.csv'))
    # Add parquet read node
    read_node = input_schema.NodeRead(
        flow_id=1,
        node_id=1,
        received_file=input_schema.ReceivedTable.model_validate(input_schema.ReceivedCsvTable(
            name="fake_data.csv",
            path=file_path,
            file_type="csv"
        ).__dict__)
    )
    flow.add_read(read_node)
    flow.get_node(1).get_resulting_data()
    # Convert to Polars code
    code = export_flow_to_polars(flow)
    verify_if_execute(code)
    df = get_result_from_generated_code(code)
    assert len(df.collect()) > 0


def test_parquet_read():
    """Test reading parquet files"""
    flow = create_basic_flow()
    flowfile_core_path = find_parent_directory('Flowfile')
    file_path = str(
        (Path(flowfile_core_path) / 'flowfile_core' / 'tests' / 'support_files' / 'data' / 'fake_data.parquet'))
    # Add parquet read node
    read_node = input_schema.NodeRead(
        flow_id=1,
        node_id=1,
        received_file=input_schema.ReceivedTable.model_validate(input_schema.ReceivedParquetTable(
            name="fake_data.parquet",
            path=file_path,
            file_type="parquet"
        ).__dict__)
    )

    flow.add_read(read_node)
    flow.get_node(1).get_resulting_data()
    # Convert to Polars code
    code = export_flow_to_polars(flow)
    verify_if_execute(code)
    df = get_result_from_generated_code(code)
    assert len(df.collect()) > 0


def test_excel_read():
    """Test reading Excel files"""
    flow = create_basic_flow()
    flowfile_core_path = find_parent_directory('Flowfile')

    file_path = str(
        (Path(flowfile_core_path) / 'flowfile_core' / 'tests' / 'support_files' / 'data' / 'fake_data.xlsx'))

    # Add Excel read node
    read_node = input_schema.NodeRead(
        flow_id=1,
        node_id=1,
        received_file=input_schema.ReceivedTable.model_validate(input_schema.ReceivedExcelTable(
            name="data.xlsx",
            path=file_path,
            file_type="xlsx",
            sheet_name="Sheet1"
        ).__dict__)
    )
    flow.add_read(read_node)

    # Convert to Polars code
    code = export_flow_to_polars(flow)

    # Verify Excel read
    verify_code_contains(code,
                         "pl.read_excel(",
                         'sheet_name="Sheet1"'
                         )
    verify_if_execute(code)
    df = get_result_from_generated_code(code)
    assert len(df.collect()) > 0


def test_aggregation_functions():
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
    # Convert to Polars code
    code = export_flow_to_polars(flow)

    # Verify all aggregation functions
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
    get_result_from_generated_code(code)
    result_df = get_result_from_generated_code(code)
    expected_df = flow.get_node(2).get_resulting_data().data_frame
    assert_frame_equal(result_df, expected_df, check_row_order=False)

def test_print_tree():
    """ Test made to the FlowGraph's print_tree method"""
    flow = create_basic_flow()

    manual_input_data_1 = [
    [1, 2, 3],                  
    ["North", "South", "East"], 
    [10, 5, 8],                 
    [150, 300, 200]             
]

manual_input_data_2 = [
    [4, 5, 6],                  
    ["West", "North", "South"], 
    [15, 12, 7],                
    [100, 200, 250]             
]

manual_input_data_3 = [
    [7, 8, 9],                  
    ["East", "West", "North"],  
    [10, 6, 3],                 
    [180, 220, 350]             
]

manual_input_data_4 = [
    [10, 11, 12],               
    ["South", "East", "West"],  
    [9, 4, 7],                  
    [280, 260, 230]             
]


for i,j in zip([manual_input_data_1,manual_input_data_2,manual_input_data_3,manual_input_data_4], range(1,5)):
        data = input_schema.NodeManualInput(
            flow_id=1,
            node_id=j,
            raw_data_format=input_schema.RawData(
                columns=[
                    input_schema.MinimalFieldInfo(name="id", data_type="Integer"),
                    input_schema.MinimalFieldInfo(name="region", data_type="String"),
                    input_schema.MinimalFieldInfo(name="quantity", data_type="Integer"),
                    input_schema.MinimalFieldInfo(name="price", data_type="Integer")
                ],
                data=i
            )
        )
        graph.add_manual_input(data)

# Add union node
union_node = input_schema.NodeUnion(
    flow_id=1,
    node_id=5,
    depending_on_ids=[1, 2, 3, 4],
    union_input=transform_schema.UnionInput(mode="relaxed")
)
graph.add_union(union_node)
for i in range(1, 5):
    connection = input_schema.NodeConnection.create_from_simple_input(i, 5, 'main')
    add_connection(graph, connection)



# Add group by node
groupby_node = input_schema.NodeGroupBy(
    flow_id=1,
    node_id=6,
    depending_on_id=1,
    groupby_input=transform_schema.GroupByInput(
        agg_cols=[
            transform_schema.AggColl("region", "groupby"),
            transform_schema.AggColl("quantity", "sum", "total_quantity"),
            transform_schema.AggColl("price", "mean", "avg_price"),
            transform_schema.AggColl("quantity", "count", "num_transactions")
        ]
    )
)
graph.add_group_by(groupby_node)
add_connection(graph, node_connection=input_schema.NodeConnection.create_from_simple_input(5, 6, 'main'))


def test_flow_with_disconnected_nodes():
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

    # Convert to Polars code - should handle both nodes
    code = export_flow_to_polars(flow)

    # Both dataframes should be created
    verify_code_contains(code,
                         "df_1 = pl.LazyFrame(",
                         "df_2 = pl.LazyFrame("
                         )


def test_custom_code_with_assignment():
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

    # Convert to Polars code
    code = export_flow_to_polars(flow)

    # Verify the code structure
    verify_code_contains(code,
                         "def _polars_code_2(input_df: pl.LazyFrame):",
                         "filtered = input_df.filter(pl.col('age') > 25)",
                         "sorted = filtered.sort('salary', descending=True)",
                         "output_df = sorted.select(['name', 'salary'])",
                         "return output_df"
                         )
    verify_if_execute(code)
    get_result_from_generated_code(code)
    result_df = get_result_from_generated_code(code)
    expected_df = flow.get_node(2).get_resulting_data().data_frame
    assert_frame_equal(result_df, expected_df, check_row_order=False)


def test_text_to_rows_without_output_name():
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

    # Convert to Polars code
    code = export_flow_to_polars(flow)

    # Should explode the original column
    verify_code_contains(code,
                         'pl.col("items").str.split(",")',
                         "explode('items')"
                         )
    result_df = get_result_from_generated_code(code)
    expected_df = flow.get_node(2).get_resulting_data().data_frame
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

    code = export_flow_to_polars(flow)
    result_df = get_result_from_generated_code(code)
    expected_df = flow.get_node(2).get_resulting_data().data_frame
    assert_frame_equal(result_df, expected_df, check_row_order=False)


@pytest.mark.skipif(not is_docker_available(), reason="Docker is not available or not running")
def test_cloud_storage_writer():
    output_file_name = f"s3://flowfile-test/flowfile_generated_data_{uuid4()}.parquet"
    conn = ensure_cloud_storage_connection_is_available_and_get_connection()
    write_settings = cloud_ss.CloudStorageWriteSettings(
        resource_path=output_file_name,
        file_format="parquet",
        connection_name=conn.connection_name
    )
    read_settings = cloud_ss.CloudStorageReadSettings(
        resource_path=output_file_name,
        file_format="parquet",
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
    code = export_flow_to_polars(flow)
    verify_if_execute(code)
    fde = FlowDataEngine.from_cloud_storage_obj(
        cloud_ss.CloudStorageReadSettingsInternal(read_settings=read_settings, connection=get_cloud_connection())
    )
    assert fde.collect()[0, 0] == 5


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
    code = export_flow_to_polars(flow)
    verify_if_execute(code)
    fde = FlowDataEngine.from_cloud_storage_obj(
        cloud_ss.CloudStorageReadSettingsInternal(read_settings=read_settings, connection=get_cloud_connection())
    )
    assert fde.collect()[0, 0] == 5
