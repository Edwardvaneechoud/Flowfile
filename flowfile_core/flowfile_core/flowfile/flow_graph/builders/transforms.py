"""Row and column transform nodes."""

from copy import deepcopy
from typing import TYPE_CHECKING

from flowfile_core.configs import logger
from flowfile_core.flowfile.filter_expressions import (
    build_filter_expression,
    resolve_filter_field_type,
    supports_native_membership,
)
from flowfile_core.flowfile.flow_data_engine.flow_data_engine import (
    FlowDataEngine,
)
from flowfile_core.flowfile.flow_data_engine.flow_file_column.main import FlowfileColumn
from flowfile_core.flowfile.flow_data_engine.formula_entries import formula_entry
from flowfile_core.flowfile.flow_graph._base import GraphMixinBase
from flowfile_core.flowfile.flow_graph.history import with_history_capture
from flowfile_core.flowfile.flow_node.flow_node import (
    kernel_block_reason,
)
from flowfile_core.flowfile.schema_callbacks import (
    pre_calculate_pivot_schema,
)
from flowfile_core.schemas import input_schema, transform_schema
from flowfile_core.schemas.history_schema import (
    HistoryActionType,
)

if TYPE_CHECKING:
    from flowfile_core.flowfile.flow_graph.graph import FlowGraph


class TransformBuildersMixin(GraphMixinBase):
    @with_history_capture(HistoryActionType.UPDATE_SETTINGS)
    def add_pivot(self, pivot_settings: input_schema.NodePivot):
        """Adds a pivot node to the graph.

        Args:
            pivot_settings: The settings for the pivot operation.
        """

        def _func(fl: FlowDataEngine):
            return fl.do_pivot(pivot_settings.pivot_input, self.flow_logger.get_node_logger(pivot_settings.node_id))

        self.add_node_step(
            node_id=pivot_settings.node_id,
            function=_func,
            node_type="pivot",
            setting_input=pivot_settings,
            input_node_ids=[pivot_settings.depending_on_id],
        )

        node = self.get_node(pivot_settings.node_id)
        node._prediction_requires_data = True

        def schema_callback():
            node._schema_prediction_blocked = None
            reason = kernel_block_reason(node, include_self=False)
            if reason:
                # Pivot columns need real data; never run a kernel implicitly for it.
                node._schema_prediction_blocked = reason
                node.results.warnings = reason
                return []
            input_data = node.singular_main_input.get_resulting_data()
            # Background thread: never mutate the shared memoized engine; build a local lazy frame.
            input_lf = input_data.data_frame.lazy()
            return pre_calculate_pivot_schema(input_data.schema, pivot_settings.pivot_input, input_lf=input_lf)

        node.schema_callback = schema_callback

    @with_history_capture(HistoryActionType.UPDATE_SETTINGS)
    def add_unpivot(self, unpivot_settings: input_schema.NodeUnpivot):
        """Adds an unpivot node to the graph.

        Args:
            unpivot_settings: The settings for the unpivot operation.
        """

        def _func(fl: FlowDataEngine) -> FlowDataEngine:
            return fl.unpivot(unpivot_settings.unpivot_input)

        self.add_node_step(
            node_id=unpivot_settings.node_id,
            function=_func,
            node_type="unpivot",
            setting_input=unpivot_settings,
            input_node_ids=[unpivot_settings.depending_on_id],
        )

    @with_history_capture(HistoryActionType.UPDATE_SETTINGS)
    def add_group_by(self, group_by_settings: input_schema.NodeGroupBy):
        """Adds a group-by aggregation node to the graph.

        Args:
            group_by_settings: The settings for the group-by operation.
        """

        def _func(fl: FlowDataEngine) -> FlowDataEngine:
            return fl.do_group_by(group_by_settings.groupby_input, False)

        self.add_node_step(
            node_id=group_by_settings.node_id,
            function=_func,
            node_type="group_by",
            setting_input=group_by_settings,
            input_node_ids=[group_by_settings.depending_on_id],
        )

        node = self.get_node(group_by_settings.node_id)

        def schema_callback():
            output_columns = [(c.old_name, c.new_name, c.output_type) for c in group_by_settings.groupby_input.agg_cols]
            depends_on = node.node_inputs.main_inputs[0]
            input_schema_dict: dict[str, str] = {s.name: s.data_type for s in depends_on.schema}
            output_schema = []
            for old_name, new_name, data_type in output_columns:
                data_type = input_schema_dict[old_name] if data_type is None else data_type
                output_schema.append(FlowfileColumn.from_input(data_type=data_type, column_name=new_name))
            return output_schema

        node.schema_callback = schema_callback

    @with_history_capture(HistoryActionType.UPDATE_SETTINGS)
    def add_filter(self, filter_settings: input_schema.NodeFilter):
        """Adds a filter node to the graph.

        Args:
            filter_settings: The settings for the filter operation.
        """

        def _func(fl: FlowDataEngine):
            is_advanced = filter_settings.filter_input.is_advanced()

            if is_advanced:
                expression = filter_settings.filter_input.advanced_filter
            else:
                basic_filter = filter_settings.filter_input.basic_filter
                if basic_filter is None:
                    logger.warning("Basic filter is None, returning unfiltered data")
                    return fl

                try:
                    column = fl.get_schema_column(basic_filter.field)
                    field_data_type = resolve_filter_field_type(column)
                    native_membership = supports_native_membership(column)
                except Exception:
                    field_data_type, native_membership = None, False

                expression = build_filter_expression(basic_filter, field_data_type, native_membership)

            if filter_settings.split_mode:
                return fl.filter_split(expression)
            return fl.do_filter(expression)

        self.add_node_step(
            filter_settings.node_id,
            _func,
            node_type="filter",
            renew_schema=False,
            setting_input=filter_settings,
            input_node_ids=[filter_settings.depending_on_id],
        )
        from flowfile_core.flowfile.settings_validation import node_expression_issue

        issue = node_expression_issue(self.get_node(filter_settings.node_id), allow_prediction=True)
        if issue is not None and issue.kind in ("type", "parse"):
            return False, issue.message
        return True, ""

    @with_history_capture(HistoryActionType.UPDATE_SETTINGS)
    def add_record_count(self, node_number_of_records: input_schema.NodeRecordCount):
        """Adds a filter node to the graph.

        Args:
            node_number_of_records: The settings for the record count operation.
        """

        def _func(fl: FlowDataEngine) -> FlowDataEngine:
            return fl.get_record_count()

        self.add_node_step(
            node_id=node_number_of_records.node_id,
            function=_func,
            node_type="record_count",
            setting_input=node_number_of_records,
            input_node_ids=[node_number_of_records.depending_on_id],
        )

    @with_history_capture(HistoryActionType.UPDATE_SETTINGS)
    def add_unique(self, unique_settings: input_schema.NodeUnique):
        """Adds a node to find and remove duplicate rows.

        Args:
            unique_settings: The settings for the unique operation.
        """

        def _func(fl: FlowDataEngine) -> FlowDataEngine:
            return fl.make_unique(unique_settings.unique_input)

        self.add_node_step(
            node_id=unique_settings.node_id,
            function=_func,
            input_columns=[],
            node_type="unique",
            setting_input=unique_settings,
            input_node_ids=[unique_settings.depending_on_id],
        )

    @with_history_capture(HistoryActionType.UPDATE_SETTINGS)
    def add_graph_solver(self, graph_solver_settings: input_schema.NodeGraphSolver):
        """Adds a node that solves graph-like problems within the data.

        This node can be used for operations like finding network paths,
        calculating connected components, or performing other graph algorithms
        on relational data that represents nodes and edges.

        Args:
            graph_solver_settings: The settings object defining the graph inputs
                and the specific algorithm to apply.
        """

        def _func(fl: FlowDataEngine) -> FlowDataEngine:
            return fl.solve_graph(graph_solver_settings.graph_solver_input)

        self.add_node_step(
            node_id=graph_solver_settings.node_id,
            function=_func,
            node_type="graph_solver",
            setting_input=graph_solver_settings,
            input_node_ids=[graph_solver_settings.depending_on_id],
        )

    @with_history_capture(HistoryActionType.UPDATE_SETTINGS)
    def add_explode_hierarchy(self, settings: input_schema.NodeExplodeHierarchy) -> "FlowGraph":
        """Adds a node that explodes a parent -> child hierarchy into its transitive closure.

        The output is a new table (ancestor, descendant, level, quantity, ...) rather than the
        input with extra columns. No schema callback is registered: prediction runs the lazy
        plugin expression against a schema-only frame, which resolves the output columns and
        their dtypes without reading data.

        Args:
            settings: The explode-hierarchy node configuration.

        Returns:
            The `FlowGraph` instance for method chaining.
        """

        def _func(fl: FlowDataEngine) -> FlowDataEngine:
            return fl.explode_hierarchy(settings.explode_hierarchy_input)

        self.add_node_step(
            node_id=settings.node_id,
            function=_func,
            node_type="explode_hierarchy",
            setting_input=settings,
            input_node_ids=[settings.depending_on_id],
        )
        return self

    @with_history_capture(HistoryActionType.UPDATE_SETTINGS)
    def add_formula(self, function_settings: input_schema.NodeFormula):
        """Adds a node that applies an ordered list of formulas to create or modify columns.

        The entries are chained, so entry N can reference the columns entries 1..N-1 produced.
        Entries with a blank expression are skipped; zero active entries is a pass-through.

        Args:
            function_settings: The settings for the formula operation.
        """
        new_cols: dict[str, FlowfileColumn] = {}
        for _, item in function_settings.active_entries():
            entry = formula_entry(1, item)
            # Overwriting an existing name keeps its first position, like polars with_columns.
            new_cols[entry.output_name] = (
                FlowfileColumn.from_input(column_name=entry.output_name, data_type=str(entry.output_data_type))
                if entry.output_data_type is not None
                else FlowfileColumn.from_input(entry.output_name, "String")
            )

        def _func(fl: FlowDataEngine):
            # Read the settings at call time: the run substitutes ${param} refs in place first.
            entries = [formula_entry(position, item) for position, item in function_settings.active_entries()]
            return fl.apply_sql_formulas(entries)

        self.add_node_step(
            function_settings.node_id,
            _func,
            output_schema=list(new_cols.values()),
            node_type="formula",
            renew_schema=False,
            setting_input=function_settings,
            input_node_ids=[function_settings.depending_on_id],
        )
        from flowfile_core.flowfile.settings_validation import node_formula_chain_issue

        message = node_formula_chain_issue(self.get_node(function_settings.node_id), allow_prediction=True)
        if message is not None:
            return False, message
        return True, ""

    @with_history_capture(HistoryActionType.UPDATE_SETTINGS)
    def add_text_to_rows(self, node_text_to_rows: input_schema.NodeTextToRows) -> "FlowGraph":
        """Adds a node that splits cell values into multiple rows.

        This is useful for un-nesting data where a single field contains multiple
        values separated by a delimiter.

        Args:
            node_text_to_rows: The settings object that specifies the column to split
                and the delimiter to use.

        Returns:
            The `FlowGraph` instance for method chaining.
        """

        def _func(table: FlowDataEngine) -> FlowDataEngine:
            return table.split(node_text_to_rows.text_to_rows_input)

        self.add_node_step(
            node_id=node_text_to_rows.node_id,
            function=_func,
            node_type="text_to_rows",
            setting_input=node_text_to_rows,
            input_node_ids=[node_text_to_rows.depending_on_id],
        )
        return self

    @with_history_capture(HistoryActionType.UPDATE_SETTINGS)
    def add_window_functions(self, settings: input_schema.NodeWindowFunctions) -> "FlowGraph":
        """Adds a window-functions node (rolling, cumulative, rank, tile, partition aggregates).

        Args:
            settings: The settings for the window-functions operation.

        Returns:
            The `FlowGraph` instance for method chaining.
        """

        def _func(fl: FlowDataEngine) -> FlowDataEngine:
            return fl.do_window_functions(settings.window_input, False)

        self.add_node_step(
            node_id=settings.node_id,
            function=_func,
            node_type="window_functions",
            setting_input=settings,
            input_node_ids=[settings.depending_on_id],
        )

        node = self.get_node(settings.node_id)

        def schema_callback():
            depends_on = node.node_inputs.main_inputs[0]
            input_schema_list = list(depends_on.schema)
            input_types = {s.name: s.data_type for s in depends_on.schema}
            output_schema = list(input_schema_list)
            for w in settings.window_input.window_functions:
                src_type = input_types.get(w.column) if w.column else None
                out_type = w.output_type or transform_schema.get_window_output_type(w.function, src_type)
                if out_type is None:
                    out_type = src_type or "Float64"
                output_schema.append(FlowfileColumn.from_input(data_type=out_type, column_name=w.new_column_name))
            return output_schema

        node.schema_callback = schema_callback
        return self

    @with_history_capture(HistoryActionType.UPDATE_SETTINGS)
    def add_sort(self, sort_settings: input_schema.NodeSort) -> "FlowGraph":
        """Adds a node to sort the data based on one or more columns.

        Args:
            sort_settings: The settings for the sort operation.

        Returns:
            The `FlowGraph` instance for method chaining.
        """

        def _func(table: FlowDataEngine) -> FlowDataEngine:
            return table.do_sort(sort_settings.sort_input)

        self.add_node_step(
            node_id=sort_settings.node_id,
            function=_func,
            node_type="sort",
            setting_input=sort_settings,
            input_node_ids=[sort_settings.depending_on_id],
        )
        return self

    @with_history_capture(HistoryActionType.UPDATE_SETTINGS)
    def add_sample(self, sample_settings: input_schema.NodeSample) -> "FlowGraph":
        """Adds a node to take a random or top-N sample of the data.

        Every method stays lazy, so the node needs no local/remote branch: the
        sample is part of the plan the worker receives, not a materialised frame.

        Args:
            sample_settings: The settings object specifying the sampling method,
                the size or fraction to keep, and an optional seed.

        Returns:
            The `FlowGraph` instance for method chaining.
        """

        def _func(table: FlowDataEngine) -> FlowDataEngine:
            if sample_settings.sample_method == "random":
                return table.random_sample(n=sample_settings.sample_size, seed=sample_settings.seed)
            if sample_settings.sample_method == "random_fraction":
                return table.random_sample(fraction=sample_settings.fraction / 100.0, seed=sample_settings.seed)
            return table.get_sample(sample_settings.sample_size)

        self.add_node_step(
            node_id=sample_settings.node_id,
            function=_func,
            node_type="sample",
            setting_input=sample_settings,
            input_node_ids=[sample_settings.depending_on_id],
        )
        return self

    @with_history_capture(HistoryActionType.UPDATE_SETTINGS)
    def add_random_split(self, settings: input_schema.NodeRandomSplit) -> "FlowGraph":
        """Adds a node that randomly partitions rows into N labeled outputs.

        Returns a ``NamedOutputs``; the framework unpacks it into
        ``_named_outputs`` so each split is reachable via its own output handle.

        Args:
            settings: The settings object specifying the splits and optional seed.

        Returns:
            The `FlowGraph` instance for method chaining.
        """
        from flowfile_core.flowfile.flow_node.multi_output import NamedOutputs

        def _func(table: FlowDataEngine) -> NamedOutputs:
            split_pairs = [(s.name, s.percentage) for s in settings.splits]
            if self.execution_location == "local":
                return table.random_split(split_pairs, settings.seed)
            return table.random_split_external(
                split_pairs,
                settings.seed,
                flow_id=self.flow_id,
                node_id=settings.node_id,
            )

        self.add_node_step(
            node_id=settings.node_id,
            function=_func,
            node_type="random_split",
            setting_input=settings,
            input_node_ids=[settings.depending_on_id],
        )
        return self

    @with_history_capture(HistoryActionType.UPDATE_SETTINGS)
    def add_record_id(self, record_id_settings: input_schema.NodeRecordId) -> "FlowGraph":
        """Adds a node to create a new column with a unique ID for each record.

        Args:
            record_id_settings: The settings object specifying the name of the
                new record ID column.

        Returns:
            The `FlowGraph` instance for method chaining.
        """

        def _func(table: FlowDataEngine) -> FlowDataEngine:
            return table.add_record_id(record_id_settings.record_id_input)

        self.add_node_step(
            node_id=record_id_settings.node_id,
            function=_func,
            node_type="record_id",
            setting_input=record_id_settings,
            input_node_ids=[record_id_settings.depending_on_id],
        )
        return self

    @with_history_capture(HistoryActionType.UPDATE_SETTINGS)
    def add_dynamic_rename(self, settings: input_schema.NodeDynamicRename) -> "FlowGraph":
        """Adds a node that renames many columns at once via a single rule.

        Supports prefix, suffix, formula-based, and first-row renaming across all
        columns, a specific list of columns, or every column of a given data type.
        In `first_row` mode the first row is dropped from the output after its
        values are promoted to column headers.

        Args:
            settings: The dynamic rename configuration.

        Returns:
            The `FlowGraph` instance for method chaining.
        """

        def _func(table: FlowDataEngine) -> FlowDataEngine:
            return table.apply_dynamic_rename(settings.dynamic_rename_input)

        self.add_node_step(
            node_id=settings.node_id,
            function=_func,
            node_type="dynamic_rename",
            setting_input=settings,
            input_node_ids=[settings.depending_on_id],
        )
        return self

    @with_history_capture(HistoryActionType.UPDATE_SETTINGS)
    def add_multi_field_formula(self, settings: input_schema.NodeMultiFieldFormula) -> "FlowGraph":
        """Adds a node that applies one formula to many columns at once.

        The formula's `[_CurrentField_]`, `[_CurrentFieldName_]` and `[_CurrentFieldType_]`
        placeholders bind to each selected column in turn, across all columns, a listed
        subset, or every column of one data type. Results either overwrite their source
        column or land in new prefixed/suffixed columns, optionally cast to a chosen type.

        No schema callback is registered: schema prediction runs the node function against
        schema-only placeholder frames, and the lazy `with_columns` yields the output schema
        — new column names and cast dtypes included — without touching data.

        Args:
            settings: The multi-field formula configuration.

        Returns:
            The `FlowGraph` instance for method chaining.
        """

        def _func(fl: FlowDataEngine) -> FlowDataEngine:
            return fl.apply_multi_field_formula(settings.multi_field_formula_input)

        self.add_node_step(
            node_id=settings.node_id,
            function=_func,
            node_type="multi_field_formula",
            renew_schema=False,
            setting_input=settings,
            input_node_ids=[settings.depending_on_id],
        )
        return self

    @with_history_capture(HistoryActionType.UPDATE_SETTINGS)
    def add_data_cleansing(self, settings: input_schema.NodeDataCleansing) -> "FlowGraph":
        """Adds a node that fixes common data quality issues.

        Drops entirely-null rows and columns, replaces nulls with a blank or zero,
        removes unwanted characters and whitespace, and normalises casing. The predicted
        schema is the input schema; the only run-time divergence is columns dropped by
        `remove_null_columns`, which is data-dependent by nature.

        Args:
            settings: The data cleansing configuration.

        Returns:
            The `FlowGraph` instance for method chaining.
        """

        def _func(fl: FlowDataEngine) -> FlowDataEngine:
            return fl.apply_data_cleansing(settings.cleansing_input)

        self.add_node_step(
            settings.node_id,
            _func,
            node_type="data_cleansing",
            renew_schema=False,
            setting_input=settings,
            input_node_ids=[settings.depending_on_id],
        )
        return self

    @with_history_capture(HistoryActionType.UPDATE_SETTINGS)
    def add_select(self, select_settings: input_schema.NodeSelect) -> "FlowGraph":
        """Adds a node to select, rename, reorder, or drop columns.

        Args:
            select_settings: The settings for the select operation.

        Returns:
            The `FlowGraph` instance for method chaining.
        """

        drop_cols = tuple(s.old_name for s in select_settings.select_input)

        def _func(table: FlowDataEngine) -> FlowDataEngine:
            select_cols = deepcopy(select_settings.select_input)
            input_cols = set(f.name for f in table.schema)
            ids_to_remove = []
            for i, select_col in enumerate(select_cols):
                if select_col.old_name not in input_cols:
                    select_col.is_available = False
                    if not select_col.keep:
                        ids_to_remove.append(i)
                    continue
                select_col.is_available = True
                if select_col.data_type is None:
                    select_col.data_type = table.get_schema_column(select_col.old_name).data_type
            ids_to_remove.reverse()
            for i in ids_to_remove:
                select_cols.pop(i)
            return table.do_select(
                select_inputs=transform_schema.SelectInputs(select_cols), keep_missing=select_settings.keep_missing
            )

        self.add_node_step(
            node_id=select_settings.node_id,
            function=_func,
            input_columns=[],
            node_type="select",
            drop_columns=list(drop_cols),
            setting_input=select_settings,
            input_node_ids=[select_settings.depending_on_id],
        )
        return self
