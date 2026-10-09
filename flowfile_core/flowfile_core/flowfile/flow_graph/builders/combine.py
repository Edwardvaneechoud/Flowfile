"""Nodes that combine several inputs: union, join, cross join and fuzzy match."""

from copy import deepcopy
from typing import TYPE_CHECKING

import polars as pl

from flowfile_core.configs import logger
from flowfile_core.flowfile.flow_data_engine.flow_data_engine import (
    FlowDataEngine,
)
from flowfile_core.flowfile.flow_graph._base import GraphMixinBase
from flowfile_core.flowfile.flow_graph.history import with_history_capture
from flowfile_core.flowfile.schema_callbacks import (
    calculate_cross_join_schema,
    calculate_fuzzy_match_schema,
    calculate_join_schema,
)
from flowfile_core.schemas import input_schema
from flowfile_core.schemas.history_schema import (
    HistoryActionType,
)
from flowfile_core.schemas.transform_schema import CrossJoinInputManager, FuzzyMatchInputManager, JoinInputManager

if TYPE_CHECKING:
    from flowfile_core.flowfile.flow_graph.graph import FlowGraph


class CombineBuildersMixin(GraphMixinBase):
    @with_history_capture(HistoryActionType.UPDATE_SETTINGS)
    def add_union(self, union_settings: input_schema.NodeUnion):
        """Adds a union node to combine multiple data streams.

        Args:
            union_settings: The settings for the union operation.
        """

        def _func(*flowfile_tables: FlowDataEngine):
            dfs: list[pl.LazyFrame] | list[pl.DataFrame] = [flt.data_frame for flt in flowfile_tables]
            return FlowDataEngine(pl.concat(dfs, how="diagonal_relaxed"))

        self.add_node_step(
            node_id=union_settings.node_id,
            function=_func,
            node_type="union",
            setting_input=union_settings,
            input_node_ids=union_settings.depending_on_ids,
        )

    @with_history_capture(HistoryActionType.UPDATE_SETTINGS)
    def add_cross_join(self, cross_join_settings: input_schema.NodeCrossJoin) -> "FlowGraph":
        """Adds a cross join node to the graph.

        Args:
            cross_join_settings: The settings for the cross join operation.

        Returns:
            The `FlowGraph` instance for method chaining.
        """

        def _func(main: FlowDataEngine, right: FlowDataEngine) -> FlowDataEngine:
            cross_join_input = deepcopy(cross_join_settings.cross_join_input)
            for left_select in cross_join_input.left_select.renames:
                left_select.is_available = True if left_select.old_name in main.schema else False
            for right_select in cross_join_input.right_select.renames:
                right_select.is_available = True if right_select.old_name in right.schema else False
            return main.do_cross_join(
                cross_join_input=cross_join_input,
                auto_generate_selection=cross_join_settings.auto_generate_selection,
                verify_integrity=False,
                other=right,
            )

        def schema_callback():
            cj_copy = CrossJoinInputManager(cross_join_settings.cross_join_input)
            node = self.get_node(node_id=cross_join_settings.node_id)
            return calculate_cross_join_schema(
                cj_copy,
                left_schema=node.node_inputs.main_inputs[0].schema,
                right_schema=node.node_inputs.right_input.schema,
            )

        self.add_node_step(
            node_id=cross_join_settings.node_id,
            function=_func,
            input_columns=[],
            node_type="cross_join",
            setting_input=cross_join_settings,
            input_node_ids=cross_join_settings.depending_on_ids,
            schema_callback=schema_callback,
        )
        return self

    @with_history_capture(HistoryActionType.UPDATE_SETTINGS)
    def add_join(self, join_settings: input_schema.NodeJoin) -> "FlowGraph":
        """Adds a join node to combine two data streams based on key columns.

        Args:
            join_settings: The settings for the join operation.

        Returns:
            The `FlowGraph` instance for method chaining.
        """

        def _func(main: FlowDataEngine, right: FlowDataEngine) -> FlowDataEngine:
            join_input = deepcopy(join_settings.join_input)
            for left_select in join_input.left_select.renames:
                left_select.is_available = True if left_select.old_name in main.schema else False
            for right_select in join_input.right_select.renames:
                right_select.is_available = True if right_select.old_name in right.schema else False
            return main.join(
                join_input=join_input,
                auto_generate_selection=join_settings.auto_generate_selection,
                verify_integrity=False,
                other=right,
            )

        def schema_callback():
            j_copy = JoinInputManager(join_settings.join_input)
            node = self.get_node(node_id=join_settings.node_id)
            return calculate_join_schema(
                j_copy,
                left_schema=node.node_inputs.main_inputs[0].schema,
                right_schema=node.node_inputs.right_input.schema,
                auto_generate_selection=join_settings.auto_generate_selection,
            )

        self.add_node_step(
            node_id=join_settings.node_id,
            function=_func,
            input_columns=[],
            node_type="join",
            setting_input=join_settings,
            input_node_ids=join_settings.depending_on_ids,
            schema_callback=schema_callback,
        )
        return self

    @with_history_capture(HistoryActionType.UPDATE_SETTINGS)
    def add_fuzzy_match(self, fuzzy_settings: input_schema.NodeFuzzyMatch) -> "FlowGraph":
        """Adds a fuzzy matching node to join data on approximate string matches.

        Args:
            fuzzy_settings: The settings for the fuzzy match operation.

        Returns:
            The `FlowGraph` instance for method chaining.
        """

        def _func(main: FlowDataEngine, right: FlowDataEngine) -> FlowDataEngine:
            node = self.get_node(node_id=fuzzy_settings.node_id)
            if self.execution_location == "local":
                return main.fuzzy_join(
                    fuzzy_match_input=deepcopy(fuzzy_settings.join_input),
                    other=right,
                    node_logger=self.flow_logger.get_node_logger(fuzzy_settings.node_id),
                )

            f = main.start_fuzzy_join(
                fuzzy_match_input=deepcopy(fuzzy_settings.join_input),
                other=right,
                file_ref=node.hash,
                flow_id=self.flow_id,
                node_id=fuzzy_settings.node_id,
            )
            logger.info("Started the fuzzy match action")
            node._fetch_cached_df = f  # Add to the node so it can be cancelled and fetch later if needed
            return FlowDataEngine(f.get_result())

        def schema_callback():
            fm_input_copy = FuzzyMatchInputManager(
                fuzzy_settings.join_input
            )  # Deepcopy create an unique object per func
            node = self.get_node(node_id=fuzzy_settings.node_id)
            return calculate_fuzzy_match_schema(
                fm_input_copy,
                left_schema=node.node_inputs.main_inputs[0].schema,
                right_schema=node.node_inputs.right_input.schema,
            )

        self.add_node_step(
            node_id=fuzzy_settings.node_id,
            function=_func,
            input_columns=[],
            node_type="fuzzy_match",
            setting_input=fuzzy_settings,
            input_node_ids=fuzzy_settings.depending_on_ids,
            schema_callback=schema_callback,
        )

        return self
