from pl_fuzzy_frame_match.models import FuzzyMapping
from polars_expr_transformer import PolarsCodeGenError, to_polars_code

from flowfile_core.configs import logger
from flowfile_core.flowfile.code_generator.base import ConverterMixinBase
from flowfile_core.flowfile.flow_data_engine.flow_file_column.main import convert_pl_type_to_string
from flowfile_core.flowfile.flow_data_engine.flow_file_column.utils import cast_str_to_polars_type
from flowfile_core.schemas import input_schema, transform_schema


class TransformHandlersMixin(ConverterMixinBase):
    """Row/column transform handlers (group_by, formula, pivot, sort, window, fuzzy, record_id, ...)."""

    def _handle_group_by(self, settings: input_schema.NodeGroupBy, var_name: str, input_vars: dict[str, str]) -> None:
        """Handle group by nodes."""
        input_df = input_vars.get("main", "df")

        group_cols = []
        agg_exprs = []

        for agg_col in settings.groupby_input.agg_cols:
            if agg_col.agg == "groupby":
                group_cols.append(agg_col.old_name)
            else:
                agg_func = self._get_agg_function(agg_col.agg)
                expr = f'{self.framework}.col("{agg_col.old_name}").{agg_func}().alias("{agg_col.new_name}")'
                agg_exprs.append(expr)

        self._add_code(f"{var_name} = {input_df}.group_by({group_cols}).agg([")
        for expr in agg_exprs:
            self._add_code(f"    {expr},")
        self._add_code("])")
        self._add_code("")

    def _handle_formula(self, settings: input_schema.NodeFormula, var_name: str, input_vars: dict[str, str]) -> None:
        """Handle formula/expression nodes."""
        input_df = input_vars.get("main", "df")
        formula = settings.function.function
        col_name = settings.function.field.name
        can_convert_to_pl_code: bool = False
        pl_code: str | None = None
        try:
            pl_code = to_polars_code(formula)
            if pl_code:
                can_convert_to_pl_code = True
        except PolarsCodeGenError:
            can_convert_to_pl_code = False
        except Exception as e:
            logger.debug(f"Unhandled conversion of the formula to polars expression falling back to expression {e}")
            can_convert_to_pl_code = False

        # TODO(FlowFrame): to_polars_code() generates pl.col/pl.lit expressions that require
        # `import polars as pl`. When framework == "ff", either:
        # (a) add `import polars as pl` to FlowFrame converter imports, or
        # (b) post-process the expression to replace `pl.` with `{self.framework}.`, or
        # (c) make to_polars_code() accept a framework prefix parameter.
        if can_convert_to_pl_code:
            expr_str = f'({pl_code}).alias("{col_name}")'
            if settings.function.field.data_type not in (None, transform_schema.AUTO_DATA_TYPE):
                output_type = convert_pl_type_to_string(cast_str_to_polars_type(settings.function.field.data_type))
                if output_type[:3] != f"{self.framework}.":
                    output_type = f"{self.framework}." + output_type
                expr_str += f".cast({output_type})"
            self._add_code(f"{var_name} = {input_df}.with_columns([{expr_str}])")
            self._add_code("")
        else:
            self.imports.add(
                "from polars_expr_transformer.process.polars_expr_transformer import simple_function_to_expr"
            )
            self._add_code(f"{var_name} = {input_df}.with_columns([")
            self._add_code(f'simple_function_to_expr({repr(formula)}).alias("{col_name}")')
            if settings.function.field.data_type not in (None, transform_schema.AUTO_DATA_TYPE):
                output_type = convert_pl_type_to_string(cast_str_to_polars_type(settings.function.field.data_type))
                if output_type[:3] != f"{self.framework}.":
                    output_type = f"{self.framework}." + output_type
                self._add_code(f"    .cast({output_type})")
            self._add_code("])")
            self._add_code("")

    def _handle_pivot_no_index(self, settings: input_schema.NodePivot, var_name: str, input_df: str, agg_func: str):
        pivot_input = settings.pivot_input
        self._add_code(f"{var_name} = ({input_df}.collect()")
        self._add_code(f'    .with_columns({self.framework}.lit(1).alias("_temp_index_"))')
        self._add_code("    .pivot(")
        self._add_code(f'        values="{pivot_input.value_col}",')
        self._add_code('        index=["_temp_index_"],')
        self._add_code(f'        on="{pivot_input.pivot_column}",')
        self._add_code(f'        aggregate_function="{agg_func}"')
        self._add_code("    )")
        self._add_code('    .drop("_temp_index_")')
        self._add_code(").lazy()")
        self._add_code("")

    def _handle_pivot(self, settings: input_schema.NodePivot, var_name: str, input_vars: dict[str, str]) -> None:
        """Handle pivot nodes."""
        input_df = input_vars.get("main", "df")
        pivot_input = settings.pivot_input
        if len(pivot_input.aggregations) > 1:
            logger.error("Multiple aggregations are not convertable to polars code. " "Taking the first value")
        if len(pivot_input.aggregations) > 0:
            agg_func = pivot_input.aggregations[0]
        else:
            agg_func = "first"
        if len(settings.pivot_input.index_columns) == 0:
            self._handle_pivot_no_index(settings, var_name, input_df, agg_func)
        else:
            self._add_code(f"{var_name} = {input_df}.collect().pivot(")
            self._add_code(f"    values='{pivot_input.value_col}',")
            self._add_code(f"    index={pivot_input.index_columns},")
            self._add_code(f"    on='{pivot_input.pivot_column}',")

            self._add_code(f"    aggregate_function='{agg_func}'")
            self._add_code(").lazy()")
            self._add_code("")

    def _handle_unpivot(self, settings: input_schema.NodeUnpivot, var_name: str, input_vars: dict[str, str]) -> None:
        """Handle unpivot nodes."""
        input_df = input_vars.get("main", "df")
        unpivot_input = settings.unpivot_input

        self._add_code(f"{var_name} = {input_df}.unpivot(")

        if unpivot_input.index_columns:
            self._add_code(f"    index={unpivot_input.index_columns},")

        if unpivot_input.value_columns:
            self._add_code(f"    on={unpivot_input.value_columns},")

        self._add_code("    variable_name='variable',")
        self._add_code("    value_name='value'")
        self._add_code(")")
        self._add_code("")

    def _handle_union(self, settings: input_schema.NodeUnion, var_name: str, input_vars: dict[str, str]) -> None:
        """Handle union nodes."""
        dfs = []
        if "main" in input_vars:
            dfs.append(input_vars["main"])
        else:
            for key, df_var in input_vars.items():
                if key.startswith("main"):
                    dfs.append(df_var)

        if settings.union_input.mode == "relaxed":
            how = "diagonal_relaxed"
        else:
            how = "diagonal"

        self._add_code(f"{var_name} = {self.framework}.concat([")
        for df in dfs:
            self._add_code(f"    {df},")
        self._add_code(f"], how='{how}')")
        self._add_code("")

    def _handle_sort(self, settings: input_schema.NodeSort, var_name: str, input_vars: dict[str, str]) -> None:
        """Handle sort nodes."""
        input_df = input_vars.get("main", "df")

        sort_cols = []
        descending = []

        for sort_input in settings.sort_input:
            sort_cols.append(f'"{sort_input.column}"')
            descending.append(sort_input.descending)

        self._add_code(f"{var_name} = {input_df}.sort([{', '.join(sort_cols)}], descending={descending})")
        self._add_code("")

    def _handle_sample(self, settings: input_schema.NodeSample, var_name: str, input_vars: dict[str, str]) -> None:
        """Handle sample nodes."""
        input_df = input_vars.get("main", "df")
        self._add_code(f"{var_name} = {input_df}.head(n={settings.sample_size})")
        self._add_code("")

    def _build_window_expr_code(
        self,
        w: "transform_schema.WindowFunctionInput",
        partition_by: list[str],
        order_by: list["transform_schema.SortByInput"] | None = None,
    ) -> str:
        """Builds a Polars expression string for a single window-function op."""
        fw = self.framework
        partition_repr = repr(partition_by) if partition_by else None

        def over(expr: str) -> str:
            return f"{expr}.over({partition_repr})" if partition_by else expr

        func = w.function
        if func.startswith("rolling_"):
            behavior = w.edge_behavior or "require_full"
            kwargs = f"window_size={w.window_size}"
            if behavior in ("partial", "fill_zero"):
                kwargs += ", min_samples=1"
            elif w.min_periods is not None:
                kwargs += f", min_samples={w.min_periods}"
            base = f'{fw}.col("{w.column}").{func}({kwargs})'
            if behavior == "fill_zero":
                base = f"{base}.fill_null(0)"
            return f'{over(base)}.alias("{w.new_column_name}")'
        if func.startswith("cum_"):
            base = f'{fw}.col("{w.column}").{func}()'
            return f'{over(base)}.alias("{w.new_column_name}")'
        if func == "rank":
            method = w.rank_method or "ordinal"
            base = f'{fw}.col("{w.column}").rank(method="{method}")'
            return f'{over(base)}.alias("{w.new_column_name}")'
        if func == "tile":
            # Tile uses only Expr methods (cum_count, when/then/otherwise) and the
            # framework-level ``len()`` so it works in both pl and ff codegen.
            if not order_by:
                raise ValueError("tile requires at least one order_by column")
            order_col = order_by[0].column
            n = int(w.number_of_groups)
            pos = over(f'{fw}.col("{order_col}").cum_count()') + " - 1"  # 0..N-1 per group
            group_len = over(f"{fw}.len()")
            big = f"(({group_len}) + {n} - 1) // {n}"
            threshold = f"(({group_len}) % {n}) * ({big})"
            small = (
                f"{fw}.when((({group_len}) // {n}) < 1).then(1)"
                f".otherwise(({group_len}) // {n})"
            )
            expr = (
                f"{fw}.when(({pos}) < ({threshold}))"
                f".then(({pos}) // ({big}) + 1)"
                f".otherwise((({pos}) - ({threshold})) // ({small}) + (({group_len}) % {n}) + 1)"
                f".cast({fw}.Int64)"
            )
            return f'{expr}.alias("{w.new_column_name}")'
        raise ValueError(f"Unsupported window function: {func!r}")

    def _handle_window_functions(
        self, settings: input_schema.NodeWindowFunctions, var_name: str, input_vars: dict[str, str]
    ) -> None:
        """Handle window function nodes (rolling, cumulative, rank, tile)."""
        input_df = input_vars.get("main", "df")
        window_input = settings.window_input

        sorted_df = input_df
        if window_input.order_by:
            sort_cols = [f'"{s.column}"' for s in window_input.order_by]
            descending = [s.descending for s in window_input.order_by]
            self._add_code(f"{var_name} = {input_df}.sort([{', '.join(sort_cols)}], descending={descending})")
            sorted_df = var_name

        exprs = [
            self._build_window_expr_code(w, window_input.partition_by, window_input.order_by)
            for w in window_input.window_functions
        ]
        self._add_code(f"{var_name} = {sorted_df}.with_columns([")
        for expr in exprs:
            self._add_code(f"    {expr},")
        self._add_code("])")
        self._add_code("")

    @staticmethod
    def _transform_fuzzy_mappings_to_string(fuzzy_mappings: list[FuzzyMapping], prefix: str = "") -> str:
        # TODO(FlowFrame): FuzzyMapping fields containing Polars Expr objects
        # (e.g. threshold_expr) are serialized via repr, producing invalid code like
        # `pl.lit(<Expr ['len()'] at 0x...>)`. Need to convert Expr objects to their
        # code string representation.
        output_str = "["
        for i, fuzzy_mapping in enumerate(fuzzy_mappings):
            output_str += (
                f"{prefix}FuzzyMapping(left_col='{fuzzy_mapping.left_col}',"
                f" right_col='{fuzzy_mapping.right_col}', "
                f"threshold_score={fuzzy_mapping.threshold_score}, "
                f"fuzzy_type='{fuzzy_mapping.fuzzy_type}')"
            )
            if i < len(fuzzy_mappings) - 1:
                output_str += ",\n"
        output_str += "]"
        return output_str

    def _handle_fuzzy_match(
        self, settings: input_schema.NodeFuzzyMatch, var_name: str, input_vars: dict[str, str]
    ) -> None:
        """Handle fuzzy match nodes."""
        self.imports.add("from pl_fuzzy_frame_match import FuzzyMapping, fuzzy_match_dfs")
        fuzzy_match_handler = transform_schema.FuzzyMatchInputManager(settings.join_input)
        left_df = input_vars.get("main", input_vars.get("main_0", "df_left"))
        right_df = input_vars.get("right", input_vars.get("main_1", "df_right"))

        if left_df == right_df:
            right_df = "df_right"
            self._add_code(f"{right_df} = {left_df}")

        # Drop into node-local temps so a fanned-out upstream frame isn't rebound.
        if fuzzy_match_handler.left_select.has_drop_cols():
            left_drop_cols = [c.old_name for c in fuzzy_match_handler.left_select.non_jk_drop_columns]
            fuzzy_left = f"_fuzzy_left_{settings.node_id}"
            self._add_code(f"{fuzzy_left} = {left_df}.drop({left_drop_cols})")
            left_df = fuzzy_left
        if fuzzy_match_handler.right_select.has_drop_cols():
            right_drop_cols = [c.old_name for c in fuzzy_match_handler.right_select.non_jk_drop_columns]
            fuzzy_right = f"_fuzzy_right_{settings.node_id}"
            self._add_code(f"{fuzzy_right} = {right_df}.drop({right_drop_cols})")
            right_df = fuzzy_right

        fuzzy_join_mapping_settings = self._transform_fuzzy_mappings_to_string(fuzzy_match_handler.join_mapping)
        self._add_code(
            f"{var_name} = fuzzy_match_dfs(\n"
            f"       left_df={left_df}, right_df={right_df},\n"
            f"       fuzzy_maps={fuzzy_join_mapping_settings}\n"
            f"       ).lazy()"
        )

    def _handle_unique(self, settings: input_schema.NodeUnique, var_name: str, input_vars: dict[str, str]) -> None:
        """Handle unique/distinct nodes."""
        input_df = input_vars.get("main", "df")

        if settings.unique_input.columns:
            self._add_code(
                f"{var_name} = {input_df}.unique("
                f"subset={settings.unique_input.columns}, keep='{settings.unique_input.strategy}')"
            )
        else:
            self._add_code(f"{var_name} = {input_df}.unique(keep='{settings.unique_input.strategy}')")
        self._add_code("")

    def _handle_text_to_rows(
        self, settings: input_schema.NodeTextToRows, var_name: str, input_vars: dict[str, str]
    ) -> None:
        """Handle text to rows (explode) nodes."""
        # TODO(FlowFrame): Verify that {self.framework}.col() expressions work correctly
        # when the input DataFrame may have been converted to pl.LazyFrame (e.g., after
        # pivot .collect().lazy() or right join .collect().lazy() chains).
        input_df = input_vars.get("main", "df")
        text_input = settings.text_to_rows_input

        split_expr = f'{self.framework}.col("{text_input.column_to_split}").str.split("{text_input.split_fixed_value}")'
        if text_input.output_column_name and text_input.output_column_name != text_input.column_to_split:
            split_expr = f'{split_expr}.alias("{text_input.output_column_name}")'
            explode_col = text_input.output_column_name
        else:
            explode_col = text_input.column_to_split

        self._add_code(f"{var_name} = {input_df}.with_columns({split_expr}).explode('{explode_col}')")
        self._add_code("")

    # .with_columns(
    #     (pl.cum_count(record_id_settings.output_column_name)
    #      .over(record_id_settings.group_by_columns) + record_id_settings.offset - 1)
    #     .alias(record_id_settings.output_column_name)
    # )
    def _handle_record_id(self, settings: input_schema.NodeRecordId, var_name: str, input_vars: dict[str, str]) -> None:
        """Handle record ID nodes."""
        input_df = input_vars.get("main", "df")
        record_input = settings.record_id_input
        if record_input.group_by and record_input.group_by_columns:
            self._add_code(f"{var_name} = ({input_df}")
            self._add_code(f"    .with_columns({self.framework}.lit(1).alias('{record_input.output_column_name}'))")
            self._add_code("    .with_columns([")
            # cum_count is 1-indexed, so the net shift is (offset - 1); emit it only
            # when it does not cancel out (default offset=1 -> bare cum_count).
            delta = record_input.offset - 1
            offset_expr = "" if delta == 0 else (f" + {delta}" if delta > 0 else f" - {abs(delta)}")
            self._add_code(
                f"    ({self.framework}.cum_count('{record_input.output_column_name}')"
                f".over({record_input.group_by_columns}){offset_expr})"
            )
            self._add_code(f"    .alias('{record_input.output_column_name}')")
            self._add_code("])")
            out_col = record_input.output_column_name
            self._add_code(f".select(['{out_col}'] + [col for col in {input_df}.columns if col != '{out_col}'])")
            self._add_code(")")
        else:
            self._add_code(
                f"{var_name} = {input_df}.with_row_count("
                f"name='{record_input.output_column_name}', offset={record_input.offset})"
            )
        self._add_code("")

    def _handle_cross_join(
        self, settings: input_schema.NodeCrossJoin, var_name: str, input_vars: dict[str, str]
    ) -> None:
        """Handle cross join nodes."""
        left_df = input_vars.get("main", input_vars.get("main_0", "df_left"))
        right_df = input_vars.get("right", input_vars.get("main_1", "df_right"))

        self._add_code(f"{var_name} = {left_df}.join({right_df}, how='cross')")
        self._add_code("")
