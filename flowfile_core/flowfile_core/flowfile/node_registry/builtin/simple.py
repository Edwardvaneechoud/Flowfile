"""Single-call transform nodes.

Generated from the pre-registry catalogs (NodeTemplate list, settings map,
AI classification map); maintained by hand from here on.

Compute factories build the closures FlowGraph._add_from_spec wires into
add_node_step. FlowDataEngine and friends are imported inside the factories:
this module is loaded while configs.node_store is still initializing, so a
module-level import would create a cycle. Every closure is named ``_func`` —
add_node_step uses function.__name__ for the node name.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl

from flowfile_core.flowfile.node_registry.spec import NodeBuildContext, NodeSpec
from flowfile_core.schemas import input_schema
from flowfile_core.schemas.schemas import NodeTag, NodeTemplate

if TYPE_CHECKING:
    from collections.abc import Callable

    from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine


def _sort_compute(settings: input_schema.NodeSort, ctx: NodeBuildContext) -> Callable:
    def _func(table: FlowDataEngine) -> FlowDataEngine:
        return table.do_sort(settings.sort_input)

    return _func


def _sample_compute(settings: input_schema.NodeSample, ctx: NodeBuildContext) -> Callable:
    def _func(table: FlowDataEngine) -> FlowDataEngine:
        return table.get_sample(settings.sample_size)

    return _func


def _record_count_compute(settings: input_schema.NodeRecordCount, ctx: NodeBuildContext) -> Callable:
    def _func(fl: FlowDataEngine) -> FlowDataEngine:
        return fl.get_record_count()

    return _func


def _filter_compute(settings: input_schema.NodeFilter, ctx: NodeBuildContext) -> Callable:
    def _func(fl: FlowDataEngine) -> FlowDataEngine:
        from flowfile_core.configs import logger
        from flowfile_core.flowfile.filter_expressions import build_filter_expression

        is_advanced = settings.filter_input.is_advanced()

        if is_advanced:
            expression = settings.filter_input.advanced_filter
        else:
            basic_filter = settings.filter_input.basic_filter
            if basic_filter is None:
                logger.warning("Basic filter is None, returning unfiltered data")
                return fl

            try:
                field_data_type = fl.get_schema_column(basic_filter.field).generic_datatype()
            except Exception:
                field_data_type = None

            expression = build_filter_expression(basic_filter, field_data_type)
            settings.filter_input.advanced_filter = expression

        if settings.split_mode:
            return fl.filter_split(expression)
        return fl.do_filter(expression)

    return _func


def _union_compute(settings: input_schema.NodeUnion, ctx: NodeBuildContext) -> Callable:
    def _func(*flowfile_tables: FlowDataEngine) -> FlowDataEngine:
        from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine

        dfs: list[pl.LazyFrame] | list[pl.DataFrame] = [flt.data_frame for flt in flowfile_tables]
        return FlowDataEngine(pl.concat(dfs, how="diagonal_relaxed"))

    return _func


SPECS: list[NodeSpec] = [
    NodeSpec(
        node_type="record_id",
        settings_class=input_schema.NodeRecordId,
        template=NodeTemplate(
            name="Add record Id",
            item="record_id",
            input=1,
            output=1,
            image="record_id.svg",
            node_type="process",
            transform_type="wide",
            node_group="transform",
            drawer_title="Add Record ID",
            drawer_intro="Generate unique identifiers for each row",
            laziness="lazy",
            tags=[NodeTag.RECORD_ID, NodeTag.ROW_NUMBER, NodeTag.INDEX],
        ),
        ai_classification="static",
    ),
    NodeSpec(
        node_type="record_count",
        settings_class=input_schema.NodeRecordCount,
        template=NodeTemplate(
            name="Count records",
            item="record_count",
            input=1,
            output=1,
            image="record_count.svg",
            node_type="process",
            transform_type="wide",
            node_group="aggregate",
            drawer_title="Count Records",
            drawer_intro="Calculate the total number of rows",
            laziness="lazy",
            tags=[NodeTag.RECORD_COUNT, NodeTag.COUNT, NodeTag.ROWS],
        ),
        has_default_settings=True,
        ai_classification="static",
        compute_factory=_record_count_compute,
    ),
    NodeSpec(
        node_type="cross_join",
        settings_class=input_schema.NodeCrossJoin,
        template=NodeTemplate(
            name="Cross join",
            item="cross_join",
            input=2,
            output=1,
            image="cross_join.svg",
            node_type="process",
            transform_type="wide",
            node_group="combine",
            drawer_title="Cross Join",
            drawer_intro="Create all possible combinations between two datasets",
            laziness="lazy",
            tags=[NodeTag.CROSS_JOIN, NodeTag.CARTESIAN, NodeTag.JOIN],
        ),
        ai_classification="static",
    ),
    NodeSpec(
        node_type="unique",
        settings_class=input_schema.NodeUnique,
        template=NodeTemplate(
            name="Drop duplicates",
            item="unique",
            input=1,
            output=1,
            image="unique.svg",
            node_type="process",
            transform_type="wide",
            node_group="transform",
            drawer_title="Drop Duplicates",
            drawer_intro="Remove duplicate rows based on selected columns",
            laziness="lazy",
            tags=[NodeTag.UNIQUE, NodeTag.DEDUPE, NodeTag.DISTINCT, NodeTag.DROP_DUPLICATES],
        ),
        ai_classification="static",
    ),
    NodeSpec(
        node_type="filter",
        settings_class=input_schema.NodeFilter,
        template=NodeTemplate(
            name="Filter data",
            item="filter",
            input=1,
            output=1,
            image="filter.svg",
            node_type="process",
            transform_type="narrow",
            node_group="transform",
            drawer_title="Filter Rows",
            drawer_intro="Keep only rows that match your conditions",
            laziness="lazy",
            tags=[NodeTag.FILTER, NodeTag.WHERE, NodeTag.SUBSET],
        ),
        ai_classification="static",
        compute_factory=_filter_compute,
        renew_schema=False,
    ),
    NodeSpec(
        node_type="formula",
        settings_class=input_schema.NodeFormula,
        template=NodeTemplate(
            name="Formula",
            item="formula",
            input=1,
            output=1,
            image="formula.svg",
            node_type="process",
            transform_type="narrow",
            node_group="transform",
            drawer_title="Formula Editor",
            drawer_intro="Create or modify columns using custom expressions",
            laziness="lazy",
            tags=[
                NodeTag.FORMULA,
                NodeTag.EXPRESSION,
                NodeTag.TRANSFORM,
                NodeTag.CALCULATE,
                NodeTag.MATH,
                NodeTag.CONCAT,
                NodeTag.SUM,
            ],
        ),
        ai_classification="static",
    ),
    NodeSpec(
        node_type="fuzzy_match",
        settings_class=input_schema.NodeFuzzyMatch,
        template=NodeTemplate(
            name="Fuzzy match",
            item="fuzzy_match",
            input=2,
            output=1,
            image="fuzzy_match.svg",
            node_type="process",
            transform_type="wide",
            node_group="combine",
            drawer_title="Fuzzy Match",
            drawer_intro="Join datasets based on similar values instead of exact matches",
            tags=[NodeTag.FUZZY, NodeTag.SIMILARITY, NodeTag.LEVENSHTEIN, NodeTag.JOIN, NodeTag.LOOKUP],
        ),
        ai_classification="static",
    ),
    NodeSpec(
        node_type="graph_solver",
        settings_class=input_schema.NodeGraphSolver,
        template=NodeTemplate(
            name="Graph solver",
            item="graph_solver",
            input=1,
            output=1,
            image="graph_solver.svg",
            node_type="process",
            transform_type="other",
            node_group="combine",
            drawer_title="Graph Solver",
            drawer_intro="Group related records in graph-structured data",
            laziness="lazy",
            tags=[NodeTag.GRAPH, NodeTag.NETWORK, NodeTag.CLUSTER, NodeTag.CONNECTED_COMPONENTS],
        ),
        ai_classification="dynamic",
    ),
    NodeSpec(
        node_type="group_by",
        settings_class=input_schema.NodeGroupBy,
        template=NodeTemplate(
            name="Group by",
            item="group_by",
            input=1,
            output=1,
            image="group_by.svg",
            node_type="process",
            transform_type="wide",
            node_group="aggregate",
            drawer_title="Group By",
            drawer_intro="Aggregate data by grouping and calculating statistics",
            laziness="lazy",
            tags=[
                NodeTag.GROUP_BY,
                NodeTag.AGGREGATE,
                NodeTag.SUM,
                NodeTag.MEAN,
                NodeTag.AVERAGE,
                NodeTag.COUNT,
                NodeTag.MIN,
                NodeTag.MAX,
                NodeTag.MEDIAN,
                NodeTag.SUMMARIZE,
            ],
        ),
        ai_classification="static",
    ),
    NodeSpec(
        node_type="join",
        settings_class=input_schema.NodeJoin,
        template=NodeTemplate(
            name="Join",
            item="join",
            input=2,
            output=1,
            image="join.svg",
            node_type="process",
            transform_type="wide",
            node_group="combine",
            drawer_title="Join Datasets",
            drawer_intro="Merge two datasets based on matching column values",
            laziness="lazy",
            tags=[NodeTag.JOIN, NodeTag.MERGE, NodeTag.LOOKUP, NodeTag.VLOOKUP, NodeTag.INNER, NodeTag.OUTER],
        ),
        ai_classification="static",
    ),
    NodeSpec(
        node_type="pivot",
        settings_class=input_schema.NodePivot,
        template=NodeTemplate(
            name="Pivot data",
            item="pivot",
            input=1,
            output=1,
            image="pivot.svg",
            node_type="process",
            transform_type="wide",
            node_group="aggregate",
            drawer_title="Pivot Data",
            drawer_intro="Convert data from long format to wide format",
            tags=[NodeTag.PIVOT, NodeTag.CROSSTAB, NodeTag.RESHAPE],
        ),
        ai_classification="dynamic",
    ),
    NodeSpec(
        node_type="random_split",
        settings_class=input_schema.NodeRandomSplit,
        template=NodeTemplate(
            name="Random Split",
            item="random_split",
            input=1,
            output=2,
            image="random_split.svg",
            node_type="process",
            transform_type="narrow",
            node_group="ml",
            drawer_title="Random Split",
            drawer_intro="Randomly partition rows into named groups (e.g. train/test)",
            laziness="lazy",
            output_names=["train", "test"],
            tags=[NodeTag.SPLIT, NodeTag.TRAIN, NodeTag.TEST, NodeTag.ML, NodeTag.PARTITION],
        ),
        ai_classification="static",
    ),
    NodeSpec(
        node_type="dynamic_rename",
        settings_class=input_schema.NodeDynamicRename,
        template=NodeTemplate(
            name="Rename columns",
            item="dynamic_rename",
            input=1,
            output=1,
            image="dynamic_rename.svg",
            node_type="process",
            transform_type="narrow",
            node_group="transform",
            drawer_title="Rename Columns",
            drawer_intro="Bulk-rename columns by prefix, suffix, or a formula",
            laziness="lazy",
            tags=[NodeTag.RENAME, NodeTag.COLUMNS],
        ),
        ai_classification="dynamic",
    ),
    NodeSpec(
        node_type="select",
        settings_class=input_schema.NodeSelect,
        template=NodeTemplate(
            name="Select data",
            item="select",
            input=1,
            output=1,
            image="select.svg",
            node_type="process",
            transform_type="narrow",
            node_group="transform",
            drawer_title="Select Columns",
            drawer_intro="Choose, rename, and reorder columns to keep",
            laziness="lazy",
            tags=[NodeTag.SELECT, NodeTag.COLUMNS, NodeTag.RENAME, NodeTag.REORDER, NodeTag.PROJECTION],
        ),
        has_default_settings=True,
        ai_classification="static",
    ),
    NodeSpec(
        node_type="sort",
        settings_class=input_schema.NodeSort,
        template=NodeTemplate(
            name="Sort data",
            item="sort",
            input=1,
            output=1,
            image="sort.svg",
            node_type="process",
            transform_type="wide",
            node_group="transform",
            drawer_title="Sort Data",
            drawer_intro="Order your data by one or more columns",
            laziness="lazy",
            tags=[NodeTag.SORT, NodeTag.ORDER, NodeTag.RANK, NodeTag.ASCENDING, NodeTag.DESCENDING],
        ),
        has_default_settings=True,
        ai_classification="static",
        compute_factory=_sort_compute,
    ),
    NodeSpec(
        node_type="sample",
        settings_class=input_schema.NodeSample,
        template=NodeTemplate(
            name="Take Sample",
            item="sample",
            input=1,
            output=1,
            image="sample.svg",
            node_type="process",
            transform_type="narrow",
            node_group="transform",
            drawer_title="Take Sample",
            drawer_intro="Work with a subset of your data",
            laziness="lazy",
            tags=[NodeTag.SAMPLE, NodeTag.SUBSET, NodeTag.LIMIT, NodeTag.HEAD],
        ),
        has_default_settings=True,
        ai_classification="static",
        compute_factory=_sample_compute,
    ),
    NodeSpec(
        node_type="text_to_rows",
        settings_class=input_schema.NodeTextToRows,
        template=NodeTemplate(
            name="Text to rows",
            item="text_to_rows",
            input=1,
            output=1,
            image="text_to_rows.svg",
            node_type="process",
            transform_type="wide",
            node_group="transform",
            drawer_title="Text to Rows",
            drawer_intro="Split text into multiple rows based on a delimiter",
            laziness="lazy",
            tags=[NodeTag.TEXT_TO_ROWS, NodeTag.SPLIT, NodeTag.EXPLODE],
        ),
        ai_classification="dynamic",
    ),
    NodeSpec(
        node_type="union",
        settings_class=input_schema.NodeUnion,
        template=NodeTemplate(
            name="Union data",
            item="union",
            input=10,
            output=1,
            image="union.svg",
            multi=True,
            node_type="process",
            transform_type="narrow",
            node_group="combine",
            drawer_title="Union Data",
            drawer_intro="Stack multiple datasets by combining rows",
            laziness="lazy",
            tags=[NodeTag.UNION, NodeTag.CONCAT, NodeTag.APPEND],
        ),
        has_default_settings=True,
        ai_classification="static",
        compute_factory=_union_compute,
    ),
    NodeSpec(
        node_type="unpivot",
        settings_class=input_schema.NodeUnpivot,
        template=NodeTemplate(
            name="Unpivot data",
            item="unpivot",
            input=1,
            output=1,
            image="unpivot.svg",
            node_type="process",
            transform_type="wide",
            node_group="aggregate",
            drawer_title="Unpivot Data",
            drawer_intro="Transform data from wide format to long format",
            laziness="lazy",
            tags=[NodeTag.UNPIVOT, NodeTag.MELT, NodeTag.RESHAPE],
        ),
        ai_classification="dynamic",
    ),
    NodeSpec(
        node_type="wait_for",
        settings_class=input_schema.NodeWaitFor,
        template=NodeTemplate(
            name="Wait For",
            item="wait_for",
            input=2,
            output=1,
            image="wait_for.svg",
            node_type="process",
            transform_type="other",
            node_group="combine",
            drawer_title="Wait For",
            drawer_intro="Pass the left input through; the right input only enforces ordering",
            tags=[NodeTag.WAIT, NodeTag.DEPENDENCY],
        ),
        ai_classification="static",
    ),
    NodeSpec(
        node_type="window_functions",
        settings_class=input_schema.NodeWindowFunctions,
        template=NodeTemplate(
            name="Window functions",
            item="window_functions",
            input=1,
            output=1,
            image="window_functions.svg",
            node_type="process",
            transform_type="wide",
            node_group="aggregate",
            drawer_title="Window Functions",
            drawer_intro="Rolling, cumulative, rank and tile calculations (optionally per partition)",
            laziness="lazy",
            tags=[
                NodeTag.WINDOW,
                NodeTag.ROLLING,
                NodeTag.CUMULATIVE,
                NodeTag.RANK,
                NodeTag.PARTITION,
                NodeTag.LAG,
                NodeTag.LEAD,
            ],
        ),
        ai_classification="static",
    ),
]
