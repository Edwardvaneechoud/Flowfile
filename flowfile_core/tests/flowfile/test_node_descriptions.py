"""
Tests for the get_default_description() method on node schema classes.

Run with:
    pytest flowfile_core/tests/flowfile/test_node_descriptions.py -v
"""

import pytest

from flowfile_core.schemas import input_schema, transform_schema


# =============================================================================
# Helpers
# =============================================================================

BASE_KWARGS = dict(flow_id=1, node_id=1)


# =============================================================================
# NodeBase (default)
# =============================================================================

class TestNodeBaseDescription:
    def test_default_returns_empty_string(self):
        node = input_schema.NodeBase(**BASE_KWARGS)
        assert node.get_default_description() == ""


# =============================================================================
# NodeFilter
# =============================================================================

class TestNodeFilterDescription:
    def test_basic_filter(self):
        node = input_schema.NodeFilter(
            **BASE_KWARGS,
            filter_input=transform_schema.FilterInput(
                mode="basic",
                basic_filter=transform_schema.BasicFilter(
                    field="age", operator="greater_than", value="30"
                ),
            ),
        )
        assert node.get_default_description() == "age > 30"

    def test_basic_filter_equals(self):
        node = input_schema.NodeFilter(
            **BASE_KWARGS,
            filter_input=transform_schema.FilterInput(
                mode="basic",
                basic_filter=transform_schema.BasicFilter(
                    field="status", operator="equals", value="active"
                ),
            ),
        )
        assert node.get_default_description() == "status = active"

    def test_basic_filter_is_null(self):
        node = input_schema.NodeFilter(
            **BASE_KWARGS,
            filter_input=transform_schema.FilterInput(
                mode="basic",
                basic_filter=transform_schema.BasicFilter(
                    field="email", operator="is_null"
                ),
            ),
        )
        assert node.get_default_description() == "email is_null"

    def test_basic_filter_between(self):
        node = input_schema.NodeFilter(
            **BASE_KWARGS,
            filter_input=transform_schema.FilterInput(
                mode="basic",
                basic_filter=transform_schema.BasicFilter(
                    field="price", operator="between", value="10", value2="50"
                ),
            ),
        )
        assert node.get_default_description() == "price between 10 and 50"

    def test_advanced_filter(self):
        node = input_schema.NodeFilter(
            **BASE_KWARGS,
            filter_input=transform_schema.FilterInput(
                mode="advanced",
                advanced_filter="col('age') > 30 & col('name').is_not_null()",
            ),
        )
        assert node.get_default_description() == "col('age') > 30 & col('name').is_not_null()"

    def test_advanced_filter_long_expression_truncated(self):
        long_expr = "a" * 100
        node = input_schema.NodeFilter(
            **BASE_KWARGS,
            filter_input=transform_schema.FilterInput(
                mode="advanced",
                advanced_filter=long_expr,
            ),
        )
        desc = node.get_default_description()
        assert len(desc) <= 80
        assert desc.endswith("...")

    def test_empty_basic_filter(self):
        node = input_schema.NodeFilter(
            **BASE_KWARGS,
            filter_input=transform_schema.FilterInput(mode="basic"),
        )
        assert node.get_default_description() == ""


# =============================================================================
# NodeJoin
# =============================================================================

class TestNodeJoinDescription:
    def test_inner_join_single_key(self):
        node = input_schema.NodeJoin(
            **BASE_KWARGS,
            join_input=transform_schema.JoinInput(
                join_mapping=[transform_schema.JoinMap(left_col="id", right_col="user_id")],
                left_select=[],
                right_select=[],
                how="inner",
            ),
        )
        assert node.get_default_description() == "inner join on id = user_id"

    def test_left_join_same_key(self):
        node = input_schema.NodeJoin(
            **BASE_KWARGS,
            join_input=transform_schema.JoinInput(
                join_mapping=[transform_schema.JoinMap(left_col="id", right_col="id")],
                left_select=[],
                right_select=[],
                how="left",
            ),
        )
        assert node.get_default_description() == "left join on id"

    def test_join_multiple_keys(self):
        node = input_schema.NodeJoin(
            **BASE_KWARGS,
            join_input=transform_schema.JoinInput(
                join_mapping=[
                    transform_schema.JoinMap(left_col="a", right_col="a"),
                    transform_schema.JoinMap(left_col="b", right_col="c"),
                ],
                left_select=[],
                right_select=[],
                how="full",
            ),
        )
        desc = node.get_default_description()
        assert desc == "full join on a, b = c"


# =============================================================================
# NodeFuzzyMatch
# =============================================================================

class TestNodeFuzzyMatchDescription:
    def test_fuzzy_match_description(self):
        from pl_fuzzy_frame_match.models import FuzzyMapping

        node = input_schema.NodeFuzzyMatch(
            **BASE_KWARGS,
            join_input=transform_schema.FuzzyMatchInput(
                join_mapping=[FuzzyMapping(left_col="name", right_col="company_name")],
                left_select=[],
                right_select=[],
                how="left",
            ),
        )
        assert node.get_default_description() == "Fuzzy left join on name ~ company_name"


# =============================================================================
# NodeFormula
# =============================================================================

class TestNodeFormulaDescription:
    def test_formula_description(self):
        node = input_schema.NodeFormula(
            **BASE_KWARGS,
            function=transform_schema.FunctionInput(
                field=transform_schema.FieldInput(name="total", data_type="Float64"),
                function="col('price') * col('quantity')",
            ),
        )
        assert node.get_default_description() == "total = col('price') * col('quantity')"

    def test_formula_default(self):
        node = input_schema.NodeFormula(**BASE_KWARGS)
        assert node.get_default_description() == ""

    def test_formula_long_expression_truncated(self):
        long_expr = "x" * 100
        node = input_schema.NodeFormula(
            **BASE_KWARGS,
            function=transform_schema.FunctionInput(
                field=transform_schema.FieldInput(name="result"),
                function=long_expr,
            ),
        )
        desc = node.get_default_description()
        assert len(desc) <= 70  # name + " = " + truncated
        assert "..." in desc


# =============================================================================
# NodeGroupBy
# =============================================================================

class TestNodeGroupByDescription:
    def test_groupby_description(self):
        node = input_schema.NodeGroupBy(
            **BASE_KWARGS,
            groupby_input=transform_schema.GroupByInput(
                agg_cols=[
                    transform_schema.AggColl(old_name="category", agg="groupby"),
                    transform_schema.AggColl(old_name="amount", agg="sum"),
                    transform_schema.AggColl(old_name="id", agg="count"),
                ]
            ),
        )
        assert node.get_default_description() == "By category: sum(amount), count(id)"

    def test_groupby_default(self):
        node = input_schema.NodeGroupBy(**BASE_KWARGS)
        assert node.get_default_description() == ""


# =============================================================================
# NodeSort
# =============================================================================

class TestNodeSortDescription:
    def test_sort_description(self):
        node = input_schema.NodeSort(
            **BASE_KWARGS,
            sort_input=[
                transform_schema.SortByInput(column="name", how="asc"),
                transform_schema.SortByInput(column="date", how="desc"),
            ],
        )
        assert node.get_default_description() == "Sort by name asc, date desc"

    def test_sort_empty(self):
        node = input_schema.NodeSort(**BASE_KWARGS, sort_input=[])
        assert node.get_default_description() == ""


# =============================================================================
# NodeSelect
# =============================================================================

class TestNodeSelectDescription:
    def test_select_with_renames(self):
        node = input_schema.NodeSelect(
            **BASE_KWARGS,
            select_input=[
                transform_schema.SelectInput(old_name="first_name", new_name="name"),
                transform_schema.SelectInput(old_name="col2", new_name="col2"),
            ],
        )
        assert node.get_default_description() == "Rename: first_name -> name"

    def test_select_with_drops(self):
        node = input_schema.NodeSelect(
            **BASE_KWARGS,
            select_input=[
                transform_schema.SelectInput(old_name="col1", new_name="col1", keep=False),
                transform_schema.SelectInput(old_name="col2", new_name="col2", keep=False),
            ],
        )
        assert node.get_default_description() == "Drop: col1, col2"

    def test_select_empty(self):
        node = input_schema.NodeSelect(**BASE_KWARGS, select_input=[])
        assert node.get_default_description() == ""


# =============================================================================
# NodeRead
# =============================================================================

class TestNodeReadDescription:
    def test_read_csv(self):
        node = input_schema.NodeRead(
            **BASE_KWARGS,
            received_file=input_schema.ReceivedTable(
                name="users.csv",
                path="/data/users.csv",
                file_type="csv",
                table_settings=input_schema.InputCsvTable(),
            ),
        )
        assert node.get_default_description() == "users.csv (csv)"

    def test_read_parquet(self):
        node = input_schema.NodeRead(
            **BASE_KWARGS,
            received_file=input_schema.ReceivedTable(
                path="/data/sales.parquet",
                file_type="parquet",
                table_settings=input_schema.InputParquetTable(),
            ),
        )
        desc = node.get_default_description()
        assert "sales.parquet" in desc
        assert "(parquet)" in desc


# =============================================================================
# NodeOutput
# =============================================================================

class TestNodeOutputDescription:
    def test_output_csv(self):
        node = input_schema.NodeOutput(
            **BASE_KWARGS,
            output_settings=input_schema.OutputSettings(
                name="output.csv",
                directory="/tmp",
                file_type="csv",
                table_settings=input_schema.OutputCsvTable(),
            ),
        )
        assert node.get_default_description() == "output.csv (csv)"


# =============================================================================
# NodeManualInput
# =============================================================================

class TestNodeManualInputDescription:
    def test_manual_input_with_data(self):
        node = input_schema.NodeManualInput(
            **BASE_KWARGS,
            raw_data_format=input_schema.RawData(
                columns=[
                    input_schema.MinimalFieldInfo(name="id"),
                    input_schema.MinimalFieldInfo(name="name"),
                ],
                data=[[1, 2, 3], ["a", "b", "c"]],
            ),
        )
        desc = node.get_default_description()
        assert "2 cols" in desc
        assert "3 rows" in desc
        assert "id" in desc
        assert "name" in desc

    def test_manual_input_empty(self):
        node = input_schema.NodeManualInput(**BASE_KWARGS, raw_data_format=None)
        assert node.get_default_description() == ""


# =============================================================================
# NodeSample
# =============================================================================

class TestNodeSampleDescription:
    def test_sample_description(self):
        node = input_schema.NodeSample(**BASE_KWARGS, sample_size=500)
        assert node.get_default_description() == "Sample 500 rows"


# =============================================================================
# NodePivot
# =============================================================================

class TestNodePivotDescription:
    def test_pivot_description(self):
        node = input_schema.NodePivot(
            **BASE_KWARGS,
            pivot_input=transform_schema.PivotInput(
                index_columns=["region"],
                pivot_column="year",
                value_col="sales",
                aggregations=["sum"],
            ),
        )
        assert node.get_default_description() == "Pivot sales by year (sum)"

    def test_pivot_default(self):
        node = input_schema.NodePivot(**BASE_KWARGS)
        assert node.get_default_description() == ""


# =============================================================================
# NodeUnpivot
# =============================================================================

class TestNodeUnpivotDescription:
    def test_unpivot_with_columns(self):
        node = input_schema.NodeUnpivot(
            **BASE_KWARGS,
            unpivot_input=transform_schema.UnpivotInput(
                index_columns=["id"],
                value_columns=["q1", "q2", "q3"],
            ),
        )
        assert node.get_default_description() == "Unpivot q1, q2, q3"

    def test_unpivot_with_data_type_selector(self):
        node = input_schema.NodeUnpivot(
            **BASE_KWARGS,
            unpivot_input=transform_schema.UnpivotInput(
                data_type_selector="numeric",
                data_type_selector_mode="data_type",
            ),
        )
        assert node.get_default_description() == "Unpivot numeric columns"


# =============================================================================
# NodeUnion
# =============================================================================

class TestNodeUnionDescription:
    def test_union_relaxed(self):
        node = input_schema.NodeUnion(**BASE_KWARGS)
        assert node.get_default_description() == "Union (relaxed)"

    def test_union_selective(self):
        node = input_schema.NodeUnion(
            **BASE_KWARGS,
            union_input=transform_schema.UnionInput(mode="selective"),
        )
        assert node.get_default_description() == "Union (selective)"


# =============================================================================
# NodeUnique
# =============================================================================

class TestNodeUniqueDescription:
    def test_unique_with_columns(self):
        node = input_schema.NodeUnique(
            **BASE_KWARGS,
            unique_input=transform_schema.UniqueInput(columns=["email", "name"], strategy="first"),
        )
        assert node.get_default_description() == "Unique by email, name (keep first)"

    def test_unique_all_columns(self):
        node = input_schema.NodeUnique(
            **BASE_KWARGS,
            unique_input=transform_schema.UniqueInput(strategy="any"),
        )
        assert node.get_default_description() == "Unique rows (keep any)"


# =============================================================================
# NodeGraphSolver
# =============================================================================

class TestNodeGraphSolverDescription:
    def test_graph_solver_description(self):
        node = input_schema.NodeGraphSolver(
            **BASE_KWARGS,
            graph_solver_input=transform_schema.GraphSolverInput(
                col_from="source", col_to="target", output_column_name="component_id"
            ),
        )
        assert node.get_default_description() == "source -> target as 'component_id'"


# =============================================================================
# NodeRecordId
# =============================================================================

class TestNodeRecordIdDescription:
    def test_record_id_description(self):
        node = input_schema.NodeRecordId(
            **BASE_KWARGS,
            record_id_input=transform_schema.RecordIdInput(output_column_name="row_num"),
        )
        assert node.get_default_description() == "Add column 'row_num'"

    def test_record_id_with_groupby(self):
        node = input_schema.NodeRecordId(
            **BASE_KWARGS,
            record_id_input=transform_schema.RecordIdInput(
                output_column_name="row_num",
                group_by=True,
                group_by_columns=["category"],
            ),
        )
        desc = node.get_default_description()
        assert "per group" in desc
        assert "category" in desc


# =============================================================================
# NodePolarsCode
# =============================================================================

class TestNodePolarsCodeDescription:
    def test_polars_code_description(self):
        node = input_schema.NodePolarsCode(
            **BASE_KWARGS,
            polars_code_input=transform_schema.PolarsCodeInput(
                polars_code="df.filter(pl.col('age') > 30)"
            ),
        )
        assert node.get_default_description() == "df.filter(pl.col('age') > 30)"

    def test_polars_code_multiline(self):
        node = input_schema.NodePolarsCode(
            **BASE_KWARGS,
            polars_code_input=transform_schema.PolarsCodeInput(
                polars_code="df.filter(\n    pl.col('age') > 30\n)"
            ),
        )
        assert node.get_default_description() == "df.filter("


# =============================================================================
# NodeCrossJoin
# =============================================================================

class TestNodeCrossJoinDescription:
    def test_cross_join_description(self):
        node = input_schema.NodeCrossJoin(
            **BASE_KWARGS,
            cross_join_input=transform_schema.CrossJoinInput(
                left_select=[], right_select=[]
            ),
        )
        assert node.get_default_description() == "Cross join"


# =============================================================================
# NodeTextToRows
# =============================================================================

class TestNodeTextToRowsDescription:
    def test_text_to_rows_description(self):
        node = input_schema.NodeTextToRows(
            **BASE_KWARGS,
            text_to_rows_input=transform_schema.TextToRowsInput(
                column_to_split="tags",
                split_by_fixed_value=True,
                split_fixed_value=",",
            ),
        )
        assert node.get_default_description() == "Split tags by ','"


# =============================================================================
# NodeDatabaseReader
# =============================================================================

class TestNodeDatabaseReaderDescription:
    def test_read_table(self):
        node = input_schema.NodeDatabaseReader(
            **BASE_KWARGS,
            database_settings=input_schema.DatabaseSettings(
                database_connection=input_schema.DatabaseConnection(),
                query_mode="table",
                table_name="users",
                schema_name="public",
            ),
        )
        assert node.get_default_description() == "Read from public.users"

    def test_read_query(self):
        node = input_schema.NodeDatabaseReader(
            **BASE_KWARGS,
            database_settings=input_schema.DatabaseSettings(
                database_connection=input_schema.DatabaseConnection(),
                query_mode="query",
                query="SELECT * FROM users WHERE active = true",
            ),
        )
        desc = node.get_default_description()
        assert desc.startswith("Query: SELECT * FROM users")


# =============================================================================
# NodeDatabaseWriter
# =============================================================================

class TestNodeDatabaseWriterDescription:
    def test_write_table(self):
        node = input_schema.NodeDatabaseWriter(
            **BASE_KWARGS,
            database_write_settings=input_schema.DatabaseWriteSettings(
                database_connection=input_schema.DatabaseConnection(),
                table_name="output_table",
                if_exists="append",
            ),
        )
        assert node.get_default_description() == "Write to output_table (append)"


# =============================================================================
# NodeExternalSource
# =============================================================================

class TestNodeExternalSourceDescription:
    def test_external_source_description(self):
        node = input_schema.NodeExternalSource(
            **BASE_KWARGS,
            identifier="sample_users",
            source_settings=input_schema.SampleUsers(SAMPLE_USERS=True, size=100),
        )
        assert node.get_default_description() == "sample_users"


# =============================================================================
# User description takes priority (integration check)
# =============================================================================

class TestUserDescriptionPriority:
    def test_user_description_is_preserved(self):
        """The description field should remain independent from get_default_description."""
        node = input_schema.NodeFilter(
            **BASE_KWARGS,
            description="My custom filter description",
            filter_input=transform_schema.FilterInput(
                mode="basic",
                basic_filter=transform_schema.BasicFilter(
                    field="age", operator="greater_than", value="30"
                ),
            ),
        )
        # User description is stored in the field
        assert node.description == "My custom filter description"
        # Auto-generated description is still available
        assert node.get_default_description() == "age > 30"
