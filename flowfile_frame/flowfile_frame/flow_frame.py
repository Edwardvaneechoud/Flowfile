from __future__ import annotations

import inspect
import os
import re
from collections.abc import Iterable, Iterator, Mapping
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Literal, Union, get_args, get_origin

import polars as pl
from pl_fuzzy_frame_match import FuzzyMapping
from polars._typing import CsvEncoding, FrameInitTypes, Orientation, SchemaDefinition, SchemaDict
from polars_expr_transformer import simple_function_to_expr

if TYPE_CHECKING:
    from flowfile_frame.catalog_reference import SchemaReference

from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
from flowfile_core.flowfile.flow_graph import FlowGraph
from flowfile_core.flowfile.flow_node.flow_node import DeferredNodeError, FlowNode
from flowfile_core.flowfile.formula_dependencies import entries_are_independent
from flowfile_core.flowfile.param_types import ParamValue
from flowfile_core.flowfile.parameter_resolver import resolve_expression_parameters
from flowfile_core.schemas import input_schema, transform_schema
from flowfile_core.schemas.schemas import GroupColor
from flowfile_frame.callable_utils import process_callable_args
from flowfile_frame.cloud_storage.frame_helpers import add_write_ff_to_cloud_storage
from flowfile_frame.config import logger
from flowfile_frame.expr import Column, Expr, col, lit
from flowfile_frame.group_frame import GroupByFrame
from flowfile_frame.join import _create_join_mappings, _normalize_columns_to_list
from flowfile_frame.lazy_methods import add_lazyframe_methods
from flowfile_frame.native import (
    DEFERRED_NODE_TYPES,
    NativeNodeError,
    add_connection_checked,
    ancestors,
    is_side_effect_node_type,
    lost_placeholder_error,
    materialise,
    merge_frames,
    seed_from_predicted_schema,
)
from flowfile_frame.parameters import refuse_parameter_as_column, refuse_parameter_column_in_formula
from flowfile_frame.selectors import Selector
from flowfile_frame.utils import (
    _check_if_convertible_to_code,
    _parse_inputs_as_iterable,
    create_flow_graph,
    ensure_inputs_as_iterable,
    generate_node_id,
    stringify_values,
)


def can_be_expr(param: inspect.Parameter) -> bool:
    """Check if a parameter can be of type pl.Expr"""
    if param.annotation == inspect.Parameter.empty:
        return False

    # Check direct match or in Union args
    types = get_args(param.annotation) if get_origin(param.annotation) is Union else [param.annotation]
    return any(t in (pl.Expr, pl.expr.expr.Expr) for t in types)


def _contains_lambda_pattern(text: str) -> bool:
    return "<lambda> at" in text


def _formula_parses(formula: str, params: dict[str, ParamValue] | None = None) -> bool:
    """Whether core's formula parser accepts ``formula``.

    Lowering onto a native Formula/Filter node is only safe when the generated
    formula parses: a Filter node silently keeps no rows on a parse error, so an
    unparseable formula stays on the Polars-code path instead. ``params`` (the
    flow's current parameter values) are substituted for this check only: the
    node keeps the ``${name}`` reference, which the run resolves.
    """
    try:
        simple_function_to_expr(resolve_expression_parameters(formula, params or {}))
    except Exception:
        return False
    return True


def _filter_exprs_to_formula(exprs: list[Expr], params: dict[str, ParamValue] | None = None) -> str | None:
    """AND-join the formula forms of filter predicates; None when any predicate lacks one."""
    if not exprs or any(e._ff_repr is None or e._function_sources for e in exprs):
        return None
    formula = " and ".join(e._ff_repr for e in exprs)
    return formula if _formula_parses(formula, params) else None


_POLARS_CODE_LABELS = {
    "with_columns": "Add columns",
    "select": "Select columns",
    "filter": "Filter on",
    "sort": "Sort by",
    "group_by": "Group by",
}


def _describe_polars_code(method_name: str | None, exprs: Any) -> str | None:
    """Canvas label for a Polars-code node emitted without a description.

    Names the operation and the columns its expressions produce (or, for a
    filter, test) instead of the first line of generated code, which the UI
    would otherwise show truncated.
    """
    if method_name is None:
        return None
    label = _POLARS_CODE_LABELS.get(method_name, f"{method_name.replace('_', ' ').capitalize()} (Polars code)")
    names: list[str] = []
    for expr in ensure_inputs_as_iterable(exprs) if exprs is not None else []:
        try:
            name = expr.meta.output_name()
        except Exception:
            continue
        if name and name not in names:
            names.append(name)
    if not names:
        return label
    shown = ", ".join(names[:3]) + (f" (+{len(names) - 3} more)" if len(names) > 3 else "")
    return f"{label}: {shown}"


def _try_translate_flowfile_formulas(
    flowfile_formulas: list[str],
    output_column_names: list[str],
) -> list[Expr] | None:
    """Translate flowfile-formula strings into native FlowFrame expressions.

    Returns a list of :class:`Expr` (one per formula, aliased to its output
    column name) when every formula translates successfully, otherwise
    ``None`` so the caller can fall back to the legacy formula path.

    Uses :func:`polars_expr_transformer.to_flowframe_code` (>= 0.5.4) which
    emits ``ff.col(...)``/``ff.lit(...)`` style strings. Generated code is
    eval'd in a restricted namespace (``ff``, ``pl``, ``datetime``) with
    builtins disabled.
    """
    try:
        from polars_expr_transformer import to_flowframe_code
    except ImportError:
        logger.debug("polars_expr_transformer.to_flowframe_code unavailable; using legacy formula path")
        return None

    import datetime as _datetime

    import flowfile_frame as _ff_module

    eval_namespace = {"ff": _ff_module, "pl": pl, "datetime": _datetime}

    expressions: list[Expr] = []
    for formula, output_name in zip(flowfile_formulas, output_column_names, strict=False):
        try:
            generated = to_flowframe_code(formula)
        except Exception:
            logger.debug("to_flowframe_code raised for formula %r; falling back", formula, exc_info=True)
            return None
        if not generated:
            logger.debug("to_flowframe_code returned empty output for formula %r; falling back", formula)
            return None
        try:
            expr = eval(generated, {"__builtins__": {}}, eval_namespace)  # noqa: S307
        except Exception:
            logger.debug(
                "eval failed for generated code %r (formula %r); falling back",
                generated, formula, exc_info=True,
            )
            return None
        if not isinstance(expr, Expr):
            logger.debug(
                "Generated code %r produced non-Expr %r (formula %r); falling back",
                generated, type(expr), formula,
            )
            return None
        expressions.append(expr.alias(output_name))
    return expressions


def get_method_name_from_code(code: str) -> str | None:
    split_code = code.split("input_df.")
    if len(split_code) > 1:
        return split_code[1].split("(")[0]


def _to_string_val(v) -> str:
    if isinstance(v, str):
        return f"'{v}'"
    else:
        return v


def _extract_expr_parts(expr_obj: Expr | Any) -> tuple[str, str]:
    """
    Extract the pure expression string and any raw definitions (including function sources) from an Expr object.

    Parameters
    ----------
    expr_obj : Expr
        The expression object to extract parts from

    Returns
    -------
    tuple[str, str]
        A tuple of (pure_expr_str, raw_definitions_str)
    """
    if not isinstance(expr_obj, Expr):
        return str(expr_obj), ""

    pure_expr_str = expr_obj._repr_str

    raw_definitions = []

    if hasattr(expr_obj, "_function_sources") and expr_obj._function_sources:
        unique_sources = []
        seen = set()
        for source in expr_obj._function_sources:
            if source not in seen:
                seen.add(source)
                unique_sources.append(source)

        if unique_sources:
            raw_definitions.extend(unique_sources)

    raw_defs_str = "\n\n".join(raw_definitions) if raw_definitions else ""

    return pure_expr_str, raw_defs_str


def _check_ok_for_serialization(
    method_name: str = None, polars_expr: pl.Expr | None = None, group_expr: pl.Expr | None = None
) -> None:
    if method_name is None:
        raise NotImplementedError("Cannot create a polars lambda expression without the method")
    if polars_expr is None:
        raise NotImplementedError("Cannot create polars expressions with lambda function")
    method_ref = getattr(pl.LazyFrame, method_name)
    if method_ref is None:
        raise ModuleNotFoundError(f"Could not find the method {method_name} in polars lazyframe")
    if method_name == "group_by":
        if group_expr is None:
            raise NotImplementedError("Cannot create a polars lambda expression without the groupby expression")
        if not all(isinstance(ge, pl.Expr) for ge in group_expr):
            raise NotImplementedError("Cannot create a polars lambda expression without the groupby expression")


@add_lazyframe_methods
class FlowFrame:
    """Main class that wraps FlowDataEngine and maintains the ETL graph."""

    flow_graph: FlowGraph
    data: pl.LazyFrame

    @staticmethod
    def create_from_any_type(
        data: FrameInitTypes = None,
        schema: SchemaDefinition | None = None,
        *,
        schema_overrides: SchemaDict | None = None,
        strict: bool = True,
        orient: Orientation | None = None,
        infer_schema_length: int | None = 100,
        nan_to_null: bool = False,
        flow_graph=None,
        node_id=None,
        parent_node_id=None,
    ):
        """
        Simple naive implementation of creating the frame from any type. It converts the data to a polars frame,
        next it implements it from a manual_input

        Parameters
        ----------
        data : FrameInitTypes
            Data to initialize the frame with
        schema : SchemaDefinition, optional
            Schema definition for the data
        schema_overrides : pl.SchemaDict, optional
            Schema overrides for specific columns
        strict : bool, default True
            Whether to enforce the schema strictly
        orient : pl.Orientation, optional
            Orientation of the data
        infer_schema_length : int, default 100
            Number of rows to use for schema inference
        nan_to_null : bool, default False
            Whether to convert NaN values to null
        flow_graph : FlowGraph, optional
            Existing ETL graph to add nodes to
        node_id : int, optional
            ID for the new node
        parent_node_id : int, optional
            ID of the parent node

        Returns
        -------
        FlowFrame
            A new FlowFrame with the data loaded as a manual input node
        """
        node_id = node_id or generate_node_id()
        description = "Data imported from Python object"
        if flow_graph is None:
            flow_graph = create_flow_graph()

        flow_id = flow_graph.flow_id
        if isinstance(data, pl.LazyFrame):
            flow_graph.add_dependency_on_polars_lazy_frame(data.lazy(), node_id)
        else:
            try:
                pl_df = pl.DataFrame(
                    data,
                    schema=schema,
                    schema_overrides=schema_overrides,
                    strict=strict,
                    orient=orient,
                    infer_schema_length=infer_schema_length,
                    nan_to_null=nan_to_null,
                )
                pl_data = pl_df.lazy()
            except Exception as e:
                raise ValueError(f"Could not dconvert data to a polars DataFrame: {e}") from e
            flow_table = FlowDataEngine(raw_data=pl_data)
            raw_data_format = input_schema.RawData(
                data=list(flow_table.to_dict().values()),
                columns=[c.get_minimal_field_info() for c in flow_table.schema],
            )
            input_node = input_schema.NodeManualInput(
                flow_id=flow_id,
                node_id=node_id,
                raw_data_format=raw_data_format,
                pos_x=100,
                pos_y=100,
                is_setup=True,
                description=description,
            )
            flow_graph.add_manual_input(input_node)
        return FlowFrame(
            data=materialise(flow_graph.get_node(node_id)).data_frame,
            flow_graph=flow_graph,
            node_id=node_id,
            parent_node_id=parent_node_id,
        )

    def __new__(
        cls,
        data: pl.LazyFrame | FrameInitTypes = None,
        schema: SchemaDefinition | None = None,
        *,
        schema_overrides: SchemaDict | None = None,
        strict: bool = True,
        orient: Orientation | None = None,
        infer_schema_length: int | None = 100,
        nan_to_null: bool = False,
        flow_graph: FlowGraph | None = None,
        node_id: int | None = None,
        parent_node_id: int | None = None,
        output_handle: str = "output-0",
        deferred: bool = False,
        **kwargs,  # Accept and ignore any other kwargs for API compatibility
    ) -> FlowFrame:
        """
        Unified constructor for FlowFrame.

        - If `flow_graph` and `node_id` are provided, it creates a lightweight Python
          wrapper around an existing node in the graph.
        - Otherwise, it creates a new source node in a new or existing graph
          from the provided data.

        ``output_handle`` identifies which source handle of the wrapped node
        downstream operations should read from. Defaults to ``"output-0"``; set
        to ``"output-1"`` for the second output of a multi-output node (e.g.
        the ``fail`` branch of ``filter_split``).

        ``deferred`` (wrapping only) marks a frame whose node, or an ancestor, only runs
        with the flow: ``data`` is a typed zero-row placeholder, operations on it build
        nodes without executing the deferred ones, and :meth:`collect` runs the graph.
        Frames created from data are never deferred.
        """
        if flow_graph is not None and node_id is not None:
            instance = super().__new__(cls)
            instance.data = data
            instance.flow_graph = flow_graph
            instance.node_id = node_id
            instance.parent_node_id = parent_node_id
            instance.output_handle = output_handle
            instance._deferred = deferred
            return instance
        elif flow_graph is not None and not isinstance(data, pl.LazyFrame):
            # create_from_any_type creates a new source node which always emits
            # output-0; no need to forward output_handle.
            instance = cls.create_from_any_type(
                data=data,
                schema=schema,
                schema_overrides=schema_overrides,
                strict=strict,
                orient=orient,
                infer_schema_length=infer_schema_length,
                nan_to_null=nan_to_null,
                flow_graph=flow_graph,
                node_id=node_id,
                parent_node_id=parent_node_id,
            )
            return instance

        source_graph = create_flow_graph()
        source_node_id = generate_node_id()

        if data is None:
            data = pl.LazyFrame()
        if not isinstance(data, pl.LazyFrame):
            description = "Data imported from Python object"
            try:
                pl_df = pl.DataFrame(
                    data,
                    schema=schema,
                    schema_overrides=schema_overrides,
                    strict=strict,
                    orient=orient,
                    infer_schema_length=infer_schema_length,
                    nan_to_null=nan_to_null,
                )
                pl_data = pl_df.lazy()
            except Exception as e:
                raise ValueError(f"Could not convert data to a Polars DataFrame: {e}") from e

            flow_table = FlowDataEngine(raw_data=pl_data)
            raw_data_format = input_schema.RawData(
                data=list(flow_table.to_dict().values()),
                columns=[c.get_minimal_field_info() for c in flow_table.schema],
            )
            input_node = input_schema.NodeManualInput(
                flow_id=source_graph.flow_id,
                node_id=source_node_id,
                raw_data_format=raw_data_format,
                pos_x=100,
                pos_y=100,
                is_setup=True,
                description=description,
            )
            source_graph.add_manual_input(input_node)
        else:
            source_graph.add_dependency_on_polars_lazy_frame(data, source_node_id)

        final_data = materialise(source_graph.get_node(source_node_id)).data_frame
        return cls(data=final_data, flow_graph=source_graph, node_id=source_node_id, parent_node_id=parent_node_id)

    def __init__(self, *args, **kwargs):
        """
        The __init__ method is intentionally left empty.
        All initialization logic is handled in the `__new__` method to support
        the flexible factory pattern and prevent state from being overwritten.
        Python automatically calls __init__ after __new__, so this empty
        method catches that call and safely does nothing.
        """
        pass

    def __repr__(self):
        return str(self.data)

    def _add_connection(
        self,
        from_id,
        to_id,
        input_type: input_schema.InputType = "main",
        output_handle: str = "output-0",
    ):
        """Helper method to add a connection between nodes"""
        connection = input_schema.NodeConnection.create_from_simple_input(
            from_id=from_id,
            to_id=to_id,
            input_type=input_type,
            output_handle=output_handle,
        )
        add_connection_checked(self.flow_graph, connection)

    def _create_child_frame(self, new_node_id, *, precomputed_result=None, deferred: bool | None = None):
        """Helper method to create a new FlowFrame that's a child of this one.

        ``deferred`` overrides the inherited flag for nodes with more inputs than this frame.
        On a deferred frame a side-effect node (writer, output, model) or a deferred node type
        is seeded from its own predicted schema instead of executed: building must never write
        or train on the zero-row placeholder.
        """
        deferred = self._deferred if deferred is None else deferred
        self._add_connection(self.node_id, new_node_id, output_handle=getattr(self, "output_handle", "output-0"))
        # If a precomputed result was provided (e.g. serialization fallback),
        # inject it into the node AFTER the connection is added (which resets the node).
        if precomputed_result is not None:
            from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine

            node = self.flow_graph.get_node(new_node_id)
            if node is not None:
                node.results.resulting_data = FlowDataEngine(precomputed_result)
        node = self.flow_graph.get_node(new_node_id)
        if (
            deferred
            and node is not None
            and (node.node_type in DEFERRED_NODE_TYPES or is_side_effect_node_type(node.node_type))
        ):
            seed_from_predicted_schema(node)
            return FlowFrame(
                data=node.results.resulting_data.data_frame,
                flow_graph=self.flow_graph,
                node_id=new_node_id,
                parent_node_id=self.node_id,
                deferred=True,
            )
        try:
            return FlowFrame(
                data=materialise(self.flow_graph.get_node(new_node_id)).data_frame,
                flow_graph=self.flow_graph,
                node_id=new_node_id,
                parent_node_id=self.node_id,
                deferred=deferred,
            )
        except AttributeError:
            raise ValueError("Could not execute the function") from None
        except DeferredNodeError as exc:
            raise lost_placeholder_error(node) from exc

    @staticmethod
    def _generate_sort_polars_code(
        pure_sort_expr_strs: list[str],
        descending_values: list[bool],
        nulls_last_values: list[bool],
        multithreaded: bool,
        maintain_order: bool,
    ) -> str:
        """
        Generates the `input_df.sort(...)` Polars code string using pure expression strings.
        """
        kwargs_for_code: dict[str, Any] = {}
        if any(descending_values):
            kwargs_for_code["descending"] = descending_values[0] if len(descending_values) == 1 else descending_values
        if any(nulls_last_values):
            kwargs_for_code["nulls_last"] = nulls_last_values[0] if len(nulls_last_values) == 1 else nulls_last_values
        if not multithreaded:
            kwargs_for_code["multithreaded"] = multithreaded
        if maintain_order:
            kwargs_for_code["maintain_order"] = maintain_order

        kwargs_str_for_code = ", ".join(f"{k}={repr(v)}" for k, v in kwargs_for_code.items())

        by_arg_for_code = (
            pure_sort_expr_strs[0] if len(pure_sort_expr_strs) == 1 else f"[{', '.join(pure_sort_expr_strs)}]"
        )
        return f"input_df.sort({by_arg_for_code}{', ' + kwargs_str_for_code if kwargs_str_for_code else ''})"

    def sort(
        self,
        by: list[Expr | str] | Expr | str,
        *more_by: Expr | str,
        descending: bool | list[bool] = False,
        nulls_last: bool | list[bool] = False,
        multithreaded: bool = True,
        maintain_order: bool = False,
        description: str | None = None,
    ) -> FlowFrame:
        """
        Sort the dataframe by the given columns.
        """
        refuse_parameter_as_column((by, more_by), "sort")
        initial_by_args = list(_parse_inputs_as_iterable((by,)))
        new_node_id = generate_node_id()

        sort_expressions_input: list = initial_by_args
        if more_by:
            sort_expressions_input.extend(list(_parse_inputs_as_iterable(more_by)))

        all_processed_expr_objects: list[Expr] = []
        pure_polars_expr_strings_for_sort: list[str] = []
        collected_raw_definitions: list[str] = []
        column_names_for_native_node: list[str] = []

        use_polars_code_path = False

        if maintain_order or not multithreaded:
            use_polars_code_path = True

        is_nulls_last_list = isinstance(nulls_last, list | tuple)
        if is_nulls_last_list and any(val for val in nulls_last if val is not False):
            use_polars_code_path = True
        elif not is_nulls_last_list and nulls_last is not False:
            use_polars_code_path = True

        for expr_input in sort_expressions_input:
            current_expr_obj: Expr
            is_simple_col_for_native = False

            if isinstance(expr_input, str):
                current_expr_obj = col(expr_input)
                column_names_for_native_node.append(expr_input)
                is_simple_col_for_native = True
            elif isinstance(expr_input, Column):
                current_expr_obj = expr_input
                # Type ignore below due to simplified Column stub
                if not expr_input._select_input.is_altered:  # type: ignore
                    column_names_for_native_node.append(expr_input.column_name)  # type: ignore
                    is_simple_col_for_native = True
                else:
                    use_polars_code_path = True  # Altered Column implies complex expression
            elif isinstance(expr_input, Expr):
                current_expr_obj = expr_input
                use_polars_code_path = True  # General Expr implies complex expression
            else:  # Convert other types to lit
                current_expr_obj = lit(expr_input)
                use_polars_code_path = True  # Literal might be part of a complex sort for Polars code

            all_processed_expr_objects.append(current_expr_obj)

            pure_expr_str, raw_defs_str = _extract_expr_parts(current_expr_obj)
            pure_polars_expr_strings_for_sort.append(pure_expr_str)

            if raw_defs_str:
                if raw_defs_str not in collected_raw_definitions:
                    collected_raw_definitions.append(raw_defs_str)
                use_polars_code_path = True

            if not is_simple_col_for_native:  # If it wasn't a simple string or unaltered Column
                use_polars_code_path = True

        desc_values = (
            list(descending) if isinstance(descending, list) else [descending] * len(all_processed_expr_objects)
        )
        null_last_values = (
            list(nulls_last) if isinstance(nulls_last, list) else [nulls_last] * len(all_processed_expr_objects)
        )

        if len(desc_values) != len(all_processed_expr_objects):
            raise ValueError("Length of 'descending' does not match the number of sort expressions.")
        if len(null_last_values) != len(all_processed_expr_objects):
            raise ValueError("Length of 'nulls_last' does not match the number of sort expressions.")

        if use_polars_code_path:
            polars_operation_code = self._generate_sort_polars_code(
                pure_polars_expr_strings_for_sort, desc_values, null_last_values, multithreaded, maintain_order
            )

            final_code_for_node: str
            if collected_raw_definitions:
                unique_raw_definitions = list(dict.fromkeys(collected_raw_definitions))  # Order-preserving unique
                definitions_section = "\n\n".join(unique_raw_definitions)
                final_code_for_node = (
                    definitions_section + "\n#─────SPLIT─────\n\n" + f"output_df = {polars_operation_code}"
                )
            else:
                final_code_for_node = polars_operation_code

            pl_expressions_for_fallback = [
                e.expr for e in all_processed_expr_objects if hasattr(e, "expr") and e.expr is not None
            ]
            kwargs_for_fallback = {
                "descending": desc_values[0] if len(desc_values) == 1 else desc_values,
                "nulls_last": null_last_values[0] if len(null_last_values) == 1 else null_last_values,
                "multithreaded": multithreaded,
                "maintain_order": maintain_order,
            }

            precomputed = self._add_polars_code(
                new_node_id,
                final_code_for_node,
                description,
                method_name="sort",
                convertable_to_code=_check_if_convertible_to_code(all_processed_expr_objects),
                polars_expr=pl_expressions_for_fallback,
                kwargs_expr=kwargs_for_fallback,
            )
        else:
            precomputed = None
            sort_inputs_for_node = []
            for i, col_name_for_native in enumerate(column_names_for_native_node):
                sort_inputs_for_node.append(
                    transform_schema.SortByInput(column=col_name_for_native, how="desc" if desc_values[i] else "asc")
                    # type: ignore
                )
            sort_settings = input_schema.NodeSort(
                flow_id=self.flow_graph.flow_id,
                node_id=new_node_id,
                sort_input=sort_inputs_for_node,  # type: ignore
                pos_x=200,
                pos_y=150,
                is_setup=True,
                depending_on_id=self.node_id,
                description=description or f"Sort by {', '.join(column_names_for_native_node)}",
            )
            self.flow_graph.add_sort(sort_settings)

        return self._create_child_frame(new_node_id, precomputed_result=precomputed)

    def _add_polars_code(
        self,
        new_node_id: int,
        code: str,
        description: str = None,
        depending_on_ids: list[str] | None = None,
        convertable_to_code: bool = True,
        method_name: str = None,
        polars_expr: Expr | list[Expr] | None = None,
        group_expr: Expr | list[Expr] | None = None,
        kwargs_expr: dict | None = None,
        group_kwargs: dict | None = None,
    ) -> Any | None:
        """Returns a precomputed result if serialization fell back, otherwise None."""
        polars_code_for_node: str
        precomputed = None
        if not convertable_to_code or _contains_lambda_pattern(code):
            if self._deferred:
                raise NativeNodeError(
                    "This operation has no code form (e.g. a lambda without retrievable source), so it would be "
                    "evaluated now and baked into the node; a deferred frame only holds placeholder rows until "
                    "the flow runs. Define the function in a module or collect the frame first."
                )
            effective_method_name = (
                get_method_name_from_code(code) if method_name is None and "input_df." in code else method_name
            )

            pl_expr_list = ensure_inputs_as_iterable(polars_expr) if polars_expr is not None else []
            group_expr_list = ensure_inputs_as_iterable(group_expr) if group_expr is not None else []

            _check_ok_for_serialization(
                polars_expr=pl_expr_list, method_name=effective_method_name, group_expr=group_expr_list
            )

            current_kwargs_expr = kwargs_expr if kwargs_expr is not None else {}
            result_lazyframe_or_expr: Any

            if effective_method_name == "group_by":
                group_kwargs = {} if group_kwargs is None else group_kwargs
                if not group_expr_list:
                    raise ValueError("group_expr is required for group_by method in serialization fallback.")
                target_obj = getattr(self.data, effective_method_name)(*group_expr_list, **group_kwargs)
                if not pl_expr_list:
                    raise ValueError(
                        "Aggregation expressions (polars_expr) are required for "
                        "group_by().agg() in serialization fallback."
                    )
                result_lazyframe_or_expr = target_obj.agg(*pl_expr_list, **current_kwargs_expr)
            elif effective_method_name:
                result_lazyframe_or_expr = getattr(self.data, effective_method_name)(
                    *pl_expr_list, **current_kwargs_expr
                )
            else:
                raise ValueError(
                    "Cannot execute Polars operation: method_name is missing and "
                    "could not be inferred for serialization fallback."
                )
            try:
                if isinstance(result_lazyframe_or_expr, pl.LazyFrame):
                    import base64

                    serialized_bytes = result_lazyframe_or_expr.serialize()
                    b64_str = base64.b64encode(serialized_bytes).decode("ascii")
                    polars_code_for_node = "\n".join(
                        [
                            f"serialized_value = base64.b64decode('{b64_str}')",
                            "buffer = BytesIO(serialized_value)",
                            "output_df = pl.LazyFrame.deserialize(buffer)",
                        ]
                    )
                    logger.warning(
                        f"Transformation '{effective_method_name}' uses non-serializable elements. "
                        "Falling back to serializing the resulting Polars LazyFrame object. "
                        "This will result in a breaking graph when using the the ui."
                    )
                else:
                    logger.error(
                        f"Fallback for non-convertible code for method '{effective_method_name}' "
                        f"resulted in a '{type(result_lazyframe_or_expr).__name__}' instead of a Polars LazyFrame. "
                        "This type cannot be persisted as a LazyFrame node via this fallback."
                    )
                    polars_code_for_node = "output_df = input_df"
                    precomputed = result_lazyframe_or_expr
            except Exception as e:
                logger.warning(
                    f"Critical error: Could not serialize the result of operation '{effective_method_name}' "
                    f"during fallback for non-convertible code. Error: {e}. "
                    "When using a lambda function, consider defining the function first"
                )
                polars_code_for_node = "output_df = input_df"
                precomputed = result_lazyframe_or_expr
        else:
            polars_code_for_node = code
        if description is None:
            description = _describe_polars_code(method_name, group_expr if group_expr is not None else polars_expr)
        polars_code_settings = input_schema.NodePolarsCode(
            flow_id=self.flow_graph.flow_id,
            node_id=new_node_id,
            polars_code_input=transform_schema.PolarsCodeInput(polars_code=polars_code_for_node),
            is_setup=True,
            depending_on_ids=depending_on_ids if depending_on_ids is not None else [self.node_id],
            description=description,
        )
        self.flow_graph.add_polars_code(polars_code_settings)
        return precomputed

    def join(
        self,
        other,
        on: list[str | Column] | str | Column = None,
        how: str = "inner",
        left_on: list[str | Column] | str | Column = None,
        right_on: list[str | Column] | str | Column = None,
        suffix: str = "_right",
        validate: str = None,
        nulls_equal: bool = False,
        coalesce: bool = None,
        maintain_order: Literal[None, "left", "right", "left_right", "right_left"] = None,
        description: str = None,
    ) -> FlowFrame:
        """
        Add a join operation to the Logical Plan.

        Parameters
        ----------
        other : FlowFrame
            Other DataFrame.
        on : str or list of str, optional
            Name(s) of the join columns in both DataFrames.
        how : {'inner', 'left', 'outer', 'semi', 'anti', 'cross'}, default 'inner'
            Join strategy.
        left_on : str or list of str, optional
            Name(s) of the left join column(s).
        right_on : str or list of str, optional
            Name(s) of the right join column(s).
        suffix : str, default "_right"
            Suffix to add to columns with a duplicate name.
        validate : {"1:1", "1:m", "m:1", "m:m"}, optional
            Validate join relationship.
        nulls_equal:
            Join on null values. By default, null values will never produce matches.
        coalesce:
            None: -> join specific.
            True: -> Always coalesce join columns.
            False: -> Never coalesce join columns.
        maintain_order:
            Which DataFrame row order to preserve, if any. Do not rely on any observed ordering without explicitly
            setting this parameter, as your code may break in a future release.
            Not specifying any ordering can improve performance Supported for inner, left, right and full joins
            None: No specific ordering is desired. The ordering might differ across Polars versions or even between
            different runs.
            left: Preserves the order of the left DataFrame.
            right: Preserves the order of the right DataFrame.
            left_right: First preserves the order of the left DataFrame, then the right.
            right_left: First preserves the order of the right DataFrame, then the left.
        description : str, optional
            Description of the join operation for the ETL graph.

        Returns
        -------
        FlowFrame
            New FlowFrame with join operation applied.
        """
        use_polars_code = self._should_use_polars_code_for_join(maintain_order, coalesce, nulls_equal, validate, suffix)
        self._ensure_same_graph(other)

        new_node_id = generate_node_id()

        left_columns, right_columns = self._parse_join_columns(on, left_on, right_on, how)
        if how != "cross" and left_columns is not None and right_columns is not None:
            if len(left_columns) != len(right_columns):
                raise ValueError(
                    f"Length mismatch: left columns ({len(left_columns)}) != right columns ({len(right_columns)})"
                )

        join_mappings = None
        if not use_polars_code and how != "cross":
            join_mappings, use_polars_code = _create_join_mappings(left_columns or [], right_columns or [])

        if use_polars_code or suffix != "_right":
            return self._execute_polars_code_join(
                other,
                new_node_id,
                on,
                left_on,
                right_on,
                left_columns,
                right_columns,
                how,
                suffix,
                validate,
                nulls_equal,
                coalesce,
                maintain_order,
                description,
            )
        elif join_mappings or how == "cross":
            return self._execute_native_join(other, new_node_id, join_mappings, how, description)
        else:
            raise ValueError("Could not execute join")

    def _should_use_polars_code_for_join(self, maintain_order, coalesce, nulls_equal, validate, suffix) -> bool:
        """Determine if we should use Polars code instead of native join."""
        return not (
            maintain_order is None
            and coalesce is None
            and nulls_equal is False
            and validate is None
            and suffix == "_right"
        )

    def _ensure_same_graph(self, other: FlowFrame) -> None:
        """Ensure both FlowFrames are in the same graph, combining if necessary."""
        merge_frames([self, other])

    def _parse_join_columns(
        self,
        on: list[str | Column] | str | Column,
        left_on: list[str | Column] | str | Column,
        right_on: list[str | Column] | str | Column,
        how: str,
    ) -> tuple[list[str] | None, list[str] | None]:
        """Parse and validate join column specifications."""
        if on is not None:
            left_columns = right_columns = _normalize_columns_to_list(on)
        elif left_on is not None and right_on is not None:
            left_columns = _normalize_columns_to_list(left_on)
            right_columns = _normalize_columns_to_list(right_on)
        elif how == "cross" and left_on is None and right_on is None and on is None:
            left_columns = None
            right_columns = None
        else:
            raise ValueError("Must specify either 'on' or both 'left_on' and 'right_on'")

        return left_columns, right_columns

    def _execute_polars_code_join(
        self,
        other: FlowFrame,
        new_node_id: int,
        on: list[str | Column] | str | Column,
        left_on: list[str | Column] | str | Column,
        right_on: list[str | Column] | str | Column,
        left_columns: list[str] | None,
        right_columns: list[str] | None,
        how: str,
        suffix: str,
        validate: str,
        nulls_equal: bool,
        coalesce: bool,
        maintain_order: Literal[None, "left", "right", "left_right", "right_left"],
        description: str,
    ) -> FlowFrame:
        """Execute join using Polars code approach."""
        code_kwargs = self._build_polars_join_kwargs(
            on,
            left_on,
            right_on,
            left_columns,
            right_columns,
            how,
            suffix,
            validate,
            nulls_equal,
            coalesce,
            maintain_order,
        )

        kwargs_str = ", ".join(f"{k}={v}" for k, v in code_kwargs.items() if v is not None)
        code = f"input_df_1.join({kwargs_str})"

        self._add_polars_code(new_node_id, code, description, depending_on_ids=[self.node_id, other.node_id])

        self._add_connection(self.node_id, new_node_id, "main", output_handle=self.output_handle)
        other._add_connection(other.node_id, new_node_id, "main", output_handle=other.output_handle)

        return FlowFrame(
            data=materialise(self.flow_graph.get_node(new_node_id)).data_frame,
            flow_graph=self.flow_graph,
            node_id=new_node_id,
            parent_node_id=self.node_id,
            deferred=self._deferred or other._deferred,
        )

    def _build_polars_join_kwargs(
        self,
        on: list[str | Column] | str | Column,
        left_on: list[str | Column] | str | Column,
        right_on: list[str | Column] | str | Column,
        left_columns: list[str] | None,
        right_columns: list[str] | None,
        how: str,
        suffix: str,
        validate: str,
        nulls_equal: bool,
        coalesce: bool,
        maintain_order: Literal[None, "left", "right", "left_right", "right_left"],
    ) -> dict:
        """Build kwargs dictionary for Polars join code."""

        def format_column_list(cols):
            if cols is None:
                return None
            return (
                "["
                + ", ".join(f"'{v}'" if isinstance(v, str) else str(v) for v in _normalize_columns_to_list(cols))
                + "]"
            )

        return {
            "other": "input_df_2",
            "how": _to_string_val(how),
            "on": format_column_list(on) if on else None,
            "left_on": format_column_list(left_columns) if left_on else None,
            "right_on": format_column_list(right_columns) if right_on else None,
            "suffix": _to_string_val(suffix),
            "validate": _to_string_val(validate),
            "nulls_equal": nulls_equal,
            "coalesce": coalesce,
            "maintain_order": _to_string_val(maintain_order),
        }

    def _execute_native_join(
        self,
        other: FlowFrame,
        new_node_id: int,
        join_mappings: list | None,
        how: str,
        description: str,
    ) -> FlowFrame:
        """Execute join using native FlowFile join nodes."""
        left_select = transform_schema.SelectInputs.create_from_pl_df(self.data)
        right_select = transform_schema.SelectInputs.create_from_pl_df(other.data)
        if how == "cross":
            join_input = transform_schema.CrossJoinInput(
                left_select=transform_schema.JoinInputs(renames=left_select.renames),
                right_select=right_select.renames,
            )
            join_input_manager = transform_schema.CrossJoinInputManager(join_input)

        else:
            join_input = transform_schema.JoinInput(
                join_mapping=join_mappings,
                left_select=transform_schema.JoinInputs(renames=left_select.renames),
                right_select=right_select.renames,
                how=how,
            )
            join_input_manager = transform_schema.JoinInputManager(join_input)

        for right_column in join_input_manager.right_select.renames:
            if right_column.join_key:
                right_column.keep = False

        if how == "cross":
            self._add_cross_join_node(new_node_id, join_input_manager.to_cross_join_input(), description, other)
        else:
            self._add_regular_join_node(new_node_id, join_input_manager.to_join_input(), description, other)

        self._add_connection(self.node_id, new_node_id, "main", output_handle=self.output_handle)
        other._add_connection(other.node_id, new_node_id, "right", output_handle=other.output_handle)
        return FlowFrame(
            data=materialise(self.flow_graph.get_node(new_node_id)).data_frame,
            flow_graph=self.flow_graph,
            node_id=new_node_id,
            parent_node_id=self.node_id,
            deferred=self._deferred or other._deferred,
        )

    def _add_cross_join_node(
        self,
        new_node_id: int,
        join_input: transform_schema.CrossJoinInput,
        description: str,
        other: FlowFrame,
    ) -> None:
        """Add a cross join node to the graph."""
        cross_join_settings = input_schema.NodeCrossJoin(
            flow_id=self.flow_graph.flow_id,
            node_id=new_node_id,
            cross_join_input=join_input,
            is_setup=True,
            depending_on_ids=[self.node_id, other.node_id],
            description=description or "Join with cross strategy",
            auto_generate_selection=True,
            verify_integrity=True,
        )
        self.flow_graph.add_cross_join(cross_join_settings)

    def _add_regular_join_node(
        self,
        new_node_id: int,
        join_input: transform_schema.JoinInput,
        description: str,
        other: FlowFrame,
    ) -> None:
        """Add a regular join node to the graph."""
        join_settings = input_schema.NodeJoin(
            flow_id=self.flow_graph.flow_id,
            node_id=new_node_id,
            join_input=join_input,
            auto_generate_selection=True,
            verify_integrity=True,
            pos_x=200,
            pos_y=150,
            is_setup=True,
            depending_on_ids=[self.node_id, other.node_id],
            description=description or f"Join with {join_input.how} strategy",
        )
        self.flow_graph.add_join(join_settings)

    def _add_number_of_records(self, new_node_id: int, description: str = None) -> FlowFrame:
        node_number_of_records = input_schema.NodeRecordCount(
            flow_id=self.flow_graph.flow_id,
            node_id=new_node_id,
            pos_x=200,
            pos_y=100,
            is_setup=True,
            depending_on_id=self.node_id,
            description=description,
        )
        self.flow_graph.add_record_count(node_number_of_records)
        return self._create_child_frame(new_node_id)

    def rename(self, mapping: Mapping[str, str], *, strict: bool = True, description: str = None) -> FlowFrame:
        """Rename columns based on a mapping or function."""
        refuse_parameter_as_column(mapping, "rename")
        return self.select(
            [col(old_name).alias(new_name) for old_name, new_name in mapping.items()],
            description=description,
            _keep_missing=True,
        )

    def select(
        self, *columns: str | Expr | Selector, description: str | None = None, _keep_missing: bool = False
    ) -> FlowFrame:
        """
        Select columns from the frame.
        """
        refuse_parameter_as_column(columns, "select")
        columns_iterable = list(_parse_inputs_as_iterable(columns))
        new_node_id = generate_node_id()
        if (
            len(columns_iterable) == 1
            and isinstance(columns_iterable[0], Expr)
            and str(columns_iterable[0])
            in ("pl.len().alias('number_of_records')", "pl.Expr(len()).alias('number_of_records')")
        ):
            return self._add_number_of_records(new_node_id, description)

        all_input_expr_objects: list[Expr] = []
        pure_polars_expr_strings_for_select: list[str] = []
        collected_raw_definitions: list[str] = []
        selected_col_names_for_native: list[transform_schema.SelectInput] = []  # For native node

        can_use_native_node = True
        if len(columns_iterable) == 1 and isinstance(columns_iterable[0], str) and columns_iterable[0] == "*":
            effective_columns_iterable = [col(c_name) for c_name in self.columns]
        else:
            effective_columns_iterable = columns_iterable
        for expr_input in effective_columns_iterable:
            current_expr_obj = expr_input
            is_simple_col_for_native = False
            if isinstance(expr_input, str):
                current_expr_obj = col(expr_input)
                selected_col_names_for_native.append(transform_schema.SelectInput(old_name=expr_input))
                is_simple_col_for_native = True
            elif isinstance(expr_input, Column):
                selected_col_names_for_native.append(expr_input.to_select_input())
                is_simple_col_for_native = True
            elif isinstance(expr_input, Selector):
                can_use_native_node = False
            elif not isinstance(expr_input, Expr):
                current_expr_obj = lit(expr_input)

            all_input_expr_objects.append(current_expr_obj)  # type: ignore

            pure_expr_str, raw_defs_str = _extract_expr_parts(current_expr_obj)

            pure_polars_expr_strings_for_select.append(pure_expr_str)
            if raw_defs_str and raw_defs_str not in collected_raw_definitions:
                collected_raw_definitions.append(raw_defs_str)

            if not is_simple_col_for_native and not isinstance(expr_input, Selector):
                can_use_native_node = False
        if collected_raw_definitions:  # Has to use Polars code if there are definitions
            can_use_native_node = False
        if can_use_native_node:
            existing_cols = self.columns
            selected_col_names = {select_col.old_name for select_col in selected_col_names_for_native}
            not_selected_columns = [
                transform_schema.SelectInput(c, keep=_keep_missing)
                for c in existing_cols
                if c not in selected_col_names
            ]
            selected_col_names_for_native.extend(not_selected_columns)
            if _keep_missing:
                lookup_selection = {_col.old_name: _col for _col in selected_col_names_for_native}
                selected_col_names_for_native = [
                    lookup_selection.get(_col) for _col in existing_cols if _col in lookup_selection
                ]
            precomputed = None
            select_settings = input_schema.NodeSelect(
                flow_id=self.flow_graph.flow_id,
                node_id=new_node_id,
                select_input=selected_col_names_for_native,
                keep_missing=_keep_missing,
                pos_x=200,
                pos_y=100,
                is_setup=True,
                depending_on_id=self.node_id,
                description=description,
            )
            self.flow_graph.add_select(select_settings)
        else:
            polars_operation_code = f"input_df.select([{', '.join(pure_polars_expr_strings_for_select)}])"
            final_code_for_node: str
            if collected_raw_definitions:
                unique_raw_definitions = list(dict.fromkeys(collected_raw_definitions))
                definitions_section = "\n\n".join(unique_raw_definitions)
                final_code_for_node = (
                    definitions_section + "\n#─────SPLIT─────\n\n" + f"output_df = {polars_operation_code}"
                )
            else:
                final_code_for_node = polars_operation_code

            pl_expressions_for_fallback = [
                e.expr
                for e in all_input_expr_objects
                if isinstance(e, Expr) and hasattr(e, "expr") and e.expr is not None
            ]
            precomputed = self._add_polars_code(
                new_node_id,
                final_code_for_node,
                description,
                method_name="select",
                convertable_to_code=_check_if_convertible_to_code(all_input_expr_objects),
                polars_expr=pl_expressions_for_fallback,
            )

        return self._create_child_frame(new_node_id, precomputed_result=precomputed)

    def drop(
        self,
        *columns: str | Column | Selector | Iterable[str | Column | Selector],
        strict: bool = True,
        description: str | None = None,
    ) -> FlowFrame:
        """Remove columns from the frame.

        Plain column names lower onto a native Select node with those columns
        unchecked (``keep=False``) and every other column kept, so the drop is
        editable on the canvas. Selectors, or a column missing under
        ``strict=True``, take the Polars-code path — the latter raises
        ``ColumnNotFoundError`` when the schema resolves, as Polars does.
        """
        refuse_parameter_as_column(columns, "drop")
        names: list[str] = []
        can_use_native = True
        for item in _parse_inputs_as_iterable(columns):
            if isinstance(item, str):
                names.append(item)
            elif isinstance(item, Column) and not item._select_input.is_altered:
                names.append(item.column_name)
            else:
                can_use_native = False
        existing_columns = self.columns
        if strict and any(name not in existing_columns for name in names):
            can_use_native = False

        new_node_id = generate_node_id()
        if can_use_native:
            dropped = set(names)
            select_settings = input_schema.NodeSelect(
                flow_id=self.flow_graph.flow_id,
                node_id=new_node_id,
                select_input=[transform_schema.SelectInput(c, keep=c not in dropped) for c in existing_columns],
                keep_missing=True,
                pos_x=200,
                pos_y=100,
                is_setup=True,
                depending_on_id=self.node_id,
                description=description,
            )
            self.flow_graph.add_select(select_settings)
        else:
            processed = process_callable_args(columns, {} if strict else {"strict": strict})
            self._add_polars_code(
                new_node_id, f"output_df = input_df.drop({processed.params_repr})", description or "Drop operation"
            )
        return self._create_child_frame(new_node_id)

    def filter(
        self,
        *predicates: Expr | Any,
        flowfile_formula: str | None = None,
        description: str | None = None,
        **constraints: Any,
    ) -> FlowFrame:
        """Filter rows based on a predicate.

        Predicates with a flowfile-formula form (comparisons, ``&``/``|``/``~``,
        ``is_in``, ``is_null``, and the string/date helpers with a formula
        equivalent) lower onto a native Filter node whose advanced expression is
        that formula, so the condition is readable and editable on the canvas.
        Anything else — lambdas, methods without a formula mapping — emits a
        Polars-code node.
        """
        if (len(predicates) > 0 or len(constraints) > 0) and flowfile_formula:
            raise ValueError("You can only use one of the following: predicates, constraints or flowfile_formula")
        new_node_id = generate_node_id()
        if len(predicates) > 0 or len(constraints) > 0:
            all_input_expr_objects = self._collect_filter_exprs(predicates, constraints)
            formula = _filter_exprs_to_formula(all_input_expr_objects, self._param_values())
            if formula is not None:
                self._add_native_filter(new_node_id, formula, description)
                return self._create_child_frame(new_node_id)

            pure_polars_expr_strings: list[str] = []
            collected_raw_definitions: list[str] = []
            for current_expr_obj in all_input_expr_objects:
                pure_expr_str, raw_defs_str = _extract_expr_parts(current_expr_obj)
                pure_polars_expr_strings.append(f"({pure_expr_str})")
                if raw_defs_str and raw_defs_str not in collected_raw_definitions:
                    collected_raw_definitions.append(raw_defs_str)

            filter_conditions_str = " & ".join(pure_polars_expr_strings) if pure_polars_expr_strings else "pl.lit(True)"
            polars_operation_code = f"input_df.filter({filter_conditions_str})"

            final_code_for_node: str
            if collected_raw_definitions:
                unique_raw_definitions = list(dict.fromkeys(collected_raw_definitions))  # Order-preserving unique
                definitions_section = "\n\n".join(unique_raw_definitions)
                final_code_for_node = (
                    definitions_section + "\n#─────SPLIT─────\n\n" + f"output_df = {polars_operation_code}"
                )
            else:
                final_code_for_node = polars_operation_code

            convertable_to_code = _check_if_convertible_to_code(all_input_expr_objects)
            pl_expressions_for_fallback = [
                e.expr
                for e in all_input_expr_objects
                if isinstance(e, Expr) and hasattr(e, "expr") and e.expr is not None
            ]
            precomputed = self._add_polars_code(
                new_node_id,
                final_code_for_node,
                description,
                method_name="filter",
                convertable_to_code=convertable_to_code,
                polars_expr=pl_expressions_for_fallback,
            )
        elif flowfile_formula:
            precomputed = None
            self._add_native_filter(new_node_id, flowfile_formula, description)
        else:
            logger.info("Filter called with no arguments; creating a pass-through Polars code node.")
            precomputed = self._add_polars_code(
                new_node_id, "output_df = input_df", description or "No-op filter", method_name=None
            )

        return self._create_child_frame(new_node_id, precomputed_result=precomputed)

    def _param_values(self) -> dict[str, ParamValue]:
        """The flow's current parameter values, typed the way a node's ``_params_getter`` serves them."""
        return {p.name: p.typed_default() for p in self.flow_graph.flow_settings.parameters or []}

    def _collect_filter_exprs(self, predicates: tuple, constraints: dict) -> list[Expr]:
        """Normalise filter arguments (nested iterables, column-name strings, literals, kwargs) to Exprs."""
        refuse_parameter_as_column(list(constraints), "filter keyword constraints")
        available_columns = self.columns
        processed_predicates = []
        for pred_item in predicates:
            if isinstance(pred_item, tuple | list | Iterator):
                processed_predicates.extend(list(pred_item))
            else:
                processed_predicates.append(pred_item)

        exprs: list[Expr] = []
        for pred_input in processed_predicates:
            if isinstance(pred_input, Expr):
                exprs.append(pred_input)
            elif isinstance(pred_input, str) and pred_input in available_columns:
                exprs.append(col(pred_input))
            else:
                exprs.append(lit(pred_input))
        for k, v_val in constraints.items():
            exprs.append(col(k) == lit(v_val))
        return exprs

    def _add_native_filter(
        self, new_node_id: int, advanced_filter: str, description: str | None, *, split_mode: bool = False
    ) -> None:
        refuse_parameter_column_in_formula(advanced_filter, "filter formula")
        filter_settings = input_schema.NodeFilter(
            flow_id=self.flow_graph.flow_id,
            node_id=new_node_id,
            filter_input=transform_schema.FilterInput(advanced_filter=advanced_filter, filter_type="advanced"),
            pos_x=200,
            pos_y=150,
            is_setup=True,
            depending_on_id=self.node_id,
            description=description,
            split_mode=split_mode,
        )
        self.flow_graph.add_filter(filter_settings)

    def _build_filter_expression_string(self, predicates: tuple, constraints: dict) -> str:
        """Formula text for a split filter's ``advanced_filter``; raises when a predicate has none.

        The split filter is one two-output node with no Polars-code fallback, and the
        engine turns an expression it cannot parse into "keep nothing" with only a
        warning, so an untranslatable predicate must fail here instead of silently
        emptying the pass output.
        """
        exprs = self._collect_filter_exprs(predicates, constraints)
        formula = _filter_exprs_to_formula(exprs, self._param_values())
        if formula is None:
            raise ValueError(
                "filter_split predicates must have a flowfile-formula form (comparisons, and/or/not, is_in, "
                "is_null, ...); lambdas and unmapped methods are not supported. Pass flowfile_formula=... "
                "or use filter() twice instead."
            )
        return formula

    def filter_split(
        self,
        *predicates: Expr | Any,
        flowfile_formula: str | None = None,
        description: str | None = None,
        **constraints: Any,
    ) -> tuple[FlowFrame, FlowFrame]:
        """Split the frame by a predicate into (pass, fail) frames.

        Rows matching the predicate are routed to the first (``pass``) frame
        and the rest to the second (``fail``) frame. Rows where the predicate
        evaluates to null are dropped from both (Polars ``filter`` semantics).

        Args mirror :meth:`filter` — accept either positional polars
        expressions, a ``flowfile_formula`` string, or keyword constraints.
        Combinations of predicates and constraints are AND-ed together.
        Predicates must have a flowfile-formula form (see :meth:`filter`);
        others raise ``ValueError`` because the split node has no Polars-code
        fallback.
        """
        if (len(predicates) > 0 or len(constraints) > 0) and flowfile_formula:
            raise ValueError("You can only use one of the following: predicates, constraints or flowfile_formula")

        if flowfile_formula:
            advanced_expr = flowfile_formula
        elif len(predicates) > 0 or len(constraints) > 0:
            advanced_expr = self._build_filter_expression_string(predicates, constraints)
        else:
            raise ValueError("filter_split requires at least one predicate, constraint, or flowfile_formula")

        new_node_id = generate_node_id()
        self._add_native_filter(new_node_id, advanced_expr, description, split_mode=True)
        self._add_connection(
            self.node_id,
            new_node_id,
            output_handle=getattr(self, "output_handle", "output-0"),
        )

        filter_node = self.flow_graph.get_node(new_node_id)
        pass_engine = materialise(filter_node, "output-0")
        fail_engine = materialise(filter_node, "output-1")

        pass_frame = FlowFrame(
            data=pass_engine.data_frame if pass_engine is not None else None,
            flow_graph=self.flow_graph,
            node_id=new_node_id,
            parent_node_id=self.node_id,
            output_handle="output-0",
            deferred=self._deferred,
        )
        fail_frame = FlowFrame(
            data=fail_engine.data_frame if fail_engine is not None else None,
            flow_graph=self.flow_graph,
            node_id=new_node_id,
            parent_node_id=self.node_id,
            output_handle="output-1",
            deferred=self._deferred,
        )
        return pass_frame, fail_frame

    def random_split(
        self,
        splits: Mapping[str, float] | list[input_schema.RandomSplitGroup],
        seed: int | None = None,
        description: str | None = None,
    ) -> tuple[FlowFrame, ...]:
        """Randomly partition rows into N labeled FlowFrames.

        ``splits`` is either an ordered mapping of split name to percentage
        (e.g. ``{"train": 80, "test": 20}``) or a list of
        :class:`flowfile_core.schemas.input_schema.RandomSplitGroup` for the
        fully-typed form. Percentages must sum to 100; order determines output
        handles. ``seed`` is the optional shuffle seed.

        Returns a tuple of FlowFrames in the same order as ``splits``.
        """
        if isinstance(splits, Mapping):
            split_groups = [input_schema.RandomSplitGroup(name=n, percentage=p) for n, p in splits.items()]
        else:
            split_groups = list(splits)
        new_node_id = generate_node_id()
        settings = input_schema.NodeRandomSplit(
            flow_id=self.flow_graph.flow_id,
            node_id=new_node_id,
            splits=split_groups,
            seed=seed,
            pos_x=200,
            pos_y=150,
            is_setup=True,
            depending_on_id=self.node_id,
            description=description,
        )
        self.flow_graph.add_random_split(settings)
        self._add_connection(
            self.node_id,
            new_node_id,
            output_handle=getattr(self, "output_handle", "output-0"),
        )
        node = self.flow_graph.get_node(new_node_id)
        frames: list[FlowFrame] = []
        for i in range(len(settings.splits)):
            engine = materialise(node, f"output-{i}") if node is not None else None
            frames.append(
                FlowFrame(
                    data=engine.data_frame if engine is not None else None,
                    flow_graph=self.flow_graph,
                    node_id=new_node_id,
                    parent_node_id=self.node_id,
                    output_handle=f"output-{i}",
                    deferred=self._deferred,
                )
            )
        return tuple(frames)

    def train_model(
        self,
        target: str,
        features: list[str] | None = None,
        model_type: str = "linear_regression",
        params: dict | None = None,
        publish_to_catalog: bool = False,
        model_name: str = "",
        namespace_id: int | None = None,
        catalog_description: str | None = None,
        catalog_tags: list[str] | None = None,
        description: str | None = None,
        *,
        schema: SchemaReference | None = None,
    ) -> FlowFrame:
        """
        Fit an ML model (regression or classification) and optionally publish it to the catalog.

        Compute runs on the worker. Catalog publishing requires the flow to be
        registered (so the artifact has a ``source_registration_id``); see
        :class:`flowfile_core.flowfile.flow_graph.FlowGraph.add_train_model`.

        Parameters
        ----------
        model_name:
            Catalog name for the trained model. Re-running with the same name
            auto-bumps the version (v2, v3, ...).
        target:
            Column to predict.
        features:
            Feature columns to fit on. If ``None``, uses every column except *target*.
        model_type:
            One of ``"linear_regression"``, ``"ridge_regression"``, ``"lasso_regression"``,
            ``"logistic_regression"``, ``"knn_classifier"``. See ``GET /ml/algorithms``
            for the live list and per-algorithm hyperparameter specs.
        params:
            Algorithm-specific hyperparameters (e.g. ``{"l2_reg": 0.1}`` for ridge).
        schema:
            Target :class:`SchemaReference` for the catalog artifact. Preferred
            over ``namespace_id``.
        namespace_id:
            Legacy. Raw namespace id; mutually exclusive with ``schema``.
        catalog_description / catalog_tags:
            Optional metadata stored alongside the artifact.
        description:
            Optional node description shown in the visual designer.

        Returns
        -------
        FlowFrame
            A new FlowFrame whose data is the input pass-through. The model
            is recorded in the catalog as a side effect.
        """
        from flowfile_frame.catalog_reference import _resolve_namespace_id

        if features is None:
            features = [c for c in self.columns if c != target]
        if not features:
            raise ValueError("train_model: no feature columns inferred. Pass `features=[...]` explicitly.")
        if publish_to_catalog and not model_name:
            raise ValueError("train_model: 'model_name' is required when 'publish_to_catalog=True'.")

        resolved_namespace_id = _resolve_namespace_id(schema, namespace_id)

        new_node_id = generate_node_id()
        train_settings = input_schema.NodeTrainModel(
            flow_id=self.flow_graph.flow_id,
            node_id=new_node_id,
            train_input=input_schema.TrainModelSettings(
                target_column=target,
                feature_columns=list(features),
                model_type=model_type,
                params=params or {},
                publish_to_catalog=publish_to_catalog,
                model_name=model_name,
                namespace_id=resolved_namespace_id,
                catalog_description=catalog_description,
                catalog_tags=list(catalog_tags or []),
            ),
            pos_x=200,
            pos_y=150,
            is_setup=True,
            depending_on_id=self.node_id,
            description=description or (f"Train {model_type} '{model_name}'" if model_name else f"Train {model_type}"),
        )
        self.flow_graph.add_train_model(train_settings)
        return self._create_child_frame(new_node_id)

    def wait_for(
        self,
        dependency: FlowFrame,
        *,
        description: str | None = None,
    ) -> FlowFrame:
        """
        Pass this frame through unchanged, but force execution to wait until
        *dependency* has finished running.

        Useful when a downstream node depends on a side-effect of another node —
        e.g. Apply Model needs Train Model to have stored its artifact first.
        The dependency frame's data is discarded; only its completion gates
        this node.

        Parameters
        ----------
        dependency:
            A :class:`FlowFrame` whose backing node must finish before this
            node runs. Wired to the *right* input handle of the Wait For node.
        description:
            Optional node description for the visual designer.
        """
        from flowfile_core.schemas import input_schema as _is

        self._ensure_same_graph(dependency)
        new_node_id = generate_node_id()
        wait_settings = input_schema.NodeWaitFor(
            flow_id=self.flow_graph.flow_id,
            node_id=new_node_id,
            depending_on_ids=[self.node_id, dependency.node_id],
            pos_x=200,
            pos_y=150,
            is_setup=True,
            description=description or "Wait for dependency",
        )
        self.flow_graph.add_wait_for(wait_settings)
        right_conn = _is.NodeConnection.create_from_simple_input(
            dependency.node_id, new_node_id, input_type="right", output_handle=dependency.output_handle
        )
        add_connection_checked(self.flow_graph, right_conn)
        return self._create_child_frame(new_node_id, deferred=self._deferred or dependency._deferred)

    def apply_model(
        self,
        upstream: FlowFrame | None = None,
        *,
        model_name: str = "",
        output_column: str = "prediction",
        version: int | None = None,
        schema: SchemaReference | None = None,
        namespace_id: int | None = None,
        description: str | None = None,
    ) -> FlowFrame:
        """
        Score the data using a trained model.

        Two model sources are supported:

        - Pass an *upstream* :class:`FlowFrame` whose backing node is a
          ``train_model`` — the trained model is read from the flow's local
          cache, no catalog round-trip required. This is the natural way to
          chain Train Model → Apply Model in the same flow.
        - Or pass *model_name* (and optionally *version* / *schema*) to
          look the model up from the catalog.

        Parameters
        ----------
        upstream:
            FlowFrame returned by :meth:`train_model`. When provided, the apply
            node reads from that train node's flow-scoped output.
        model_name:
            Catalog name of the trained model. Used only when *upstream* is None.
        output_column:
            Name of the new prediction column added to the output.
        version:
            Specific catalog version to apply. Defaults to the latest active version.
        schema:
            Catalog :class:`SchemaReference` to look the model up in. Preferred
            over ``namespace_id``.
        namespace_id:
            Legacy. Raw namespace id; mutually exclusive with ``schema``.
        description:
            Optional node description shown in the visual designer.

        Returns
        -------
        FlowFrame
            A new FlowFrame with all input columns plus *output_column* (Float64).
        """
        from flowfile_frame.catalog_reference import _resolve_namespace_id

        if upstream is None and not model_name:
            raise ValueError("apply_model: pass either *upstream* (FlowFrame from train_model) or *model_name*.")
        if upstream is not None and model_name:
            raise ValueError("apply_model: pass either *upstream* or *model_name*, not both.")

        resolved_namespace_id = _resolve_namespace_id(schema, namespace_id)

        new_node_id = generate_node_id()
        if upstream is not None:
            apply_input = input_schema.ApplyModelSettings(
                source="upstream",
                upstream_node_id=upstream.node_id,
                output_column=output_column,
            )
            default_desc = f"Apply (upstream node {upstream.node_id}) -> {output_column}"
        else:
            apply_input = input_schema.ApplyModelSettings(
                source="catalog",
                model_name=model_name,
                model_version=version,
                namespace_id=resolved_namespace_id,
                output_column=output_column,
            )
            default_desc = f"Apply '{model_name}' -> {output_column}"

        apply_settings = input_schema.NodeApplyModel(
            flow_id=self.flow_graph.flow_id,
            node_id=new_node_id,
            apply_input=apply_input,
            pos_x=200,
            pos_y=150,
            is_setup=True,
            depending_on_id=self.node_id,
            description=description or default_desc,
        )
        self.flow_graph.add_apply_model(apply_settings)
        return self._create_child_frame(
            new_node_id, deferred=self._deferred or (upstream is not None and upstream._deferred)
        )

    def evaluate_model(
        self,
        actual_column: str,
        *,
        predicted_column: str = "prediction",
        task_type: Literal["auto", "regression", "classification"] = "auto",
        upstream: FlowFrame | None = None,
        description: str | None = None,
    ) -> FlowFrame:
        """
        Compute model-quality metrics by comparing an actual column with a prediction column.

        Returns a long-form ``(metric, value)`` frame. Reusable on training,
        test, or hold-out splits — there's no built-in coupling to a specific
        Train/Apply pair. Pass *upstream* (a FlowFrame returned by
        :meth:`train_model`) so ``task_type="auto"`` can read the trainer's
        task type; otherwise pass *task_type* explicitly. When neither is set,
        regression metrics are computed.

        Parameters
        ----------
        actual_column:
            Column on the input frame holding the true values.
        predicted_column:
            Column on the input frame holding the predicted values. Defaults
            to ``"prediction"``, matching :meth:`apply_model`'s default
            *output_column*.
        task_type:
            Metric set to compute. ``"auto"`` resolves the task type from
            *upstream*'s trainer when provided; otherwise falls back to
            ``"regression"``.
        upstream:
            Optional :class:`FlowFrame` returned by :meth:`train_model`. When
            given, ``task_type="auto"`` reads the trainer's task type from
            this node.
        description:
            Optional node description shown in the visual designer.

        Returns
        -------
        FlowFrame
            A new FlowFrame with two columns: ``metric`` (String) and
            ``value`` (Float64).
        """
        if actual_column not in self.columns:
            raise ValueError(f"evaluate_model: actual_column '{actual_column}' not in input columns {self.columns}.")
        if predicted_column not in self.columns:
            raise ValueError(
                f"evaluate_model: predicted_column '{predicted_column}' not in input columns {self.columns}."
            )
        if upstream is not None and upstream.flow_graph is not self.flow_graph:
            raise ValueError("evaluate_model: 'upstream' must belong to the same flow as this frame.")

        new_node_id = generate_node_id()
        evaluate_settings = input_schema.NodeEvaluateModel(
            flow_id=self.flow_graph.flow_id,
            node_id=new_node_id,
            evaluate_input=input_schema.EvaluateModelSettings(
                actual_column=actual_column,
                predicted_column=predicted_column,
                task_type=task_type,
                upstream_train_node_id=upstream.node_id if upstream is not None else None,
            ),
            pos_x=200,
            pos_y=150,
            is_setup=True,
            depending_on_id=self.node_id,
            description=description or f"Evaluate {predicted_column} vs {actual_column}",
        )
        self.flow_graph.add_evaluate_model(evaluate_settings)
        return self._create_child_frame(
            new_node_id, deferred=self._deferred or (upstream is not None and upstream._deferred)
        )

    def sink_csv(self, file: str, *args, separator: str = ",", encoding: str = "utf-8", description: str = None):
        """
        Write the data to a CSV file.

        Args:
            path: Path or filename for the CSV file
            separator: Field delimiter to use, defaults to ','
            encoding: File encoding, defaults to 'utf-8'
            description: Description of this operation for the ETL graph

        Returns:
            Self for method chaining
        """
        return self.write_csv(file, *args, separator=separator, encoding=encoding, description=description)

    def _refuse_polars_code_writer_if_deferred(self, method_name: str) -> None:
        """Refuse the Polars-code writer fallback on a deferred frame.

        That node writes when it is built, which on a deferred frame means writing the zero-row
        placeholder; only the native Output node waits for the flow run.
        """
        if self._deferred:
            raise NativeNodeError(
                f"{method_name} with extra writer options builds a Polars-code node that writes immediately, "
                "but this frame only holds placeholder rows until the flow runs. Drop the extra options to use "
                "a native Output node, or collect the frame first."
            )

    def write_parquet(
        self,
        path: str | os.PathLike,
        *,
        compression: str | None = None,
        description: str = None,
        convert_to_absolute_path: bool = True,
        **kwargs: Any,
    ) -> FlowFrame:
        """
        Write the data to a Parquet file. Creates a standard Output node if only
        'path' and standard options are provided. Falls back to a Polars Code node
        if other keyword arguments are used.

        Args:
            path: Path (string or pathlib.Path) or filename for the Parquet file.
                  Note: Writable file-like objects are not supported when using advanced options
                  that trigger the Polars Code node fallback.
            description: Description of this operation for the ETL graph.
            convert_to_absolute_path: If the path needs to be set to a fixed location.
            **kwargs: Additional keyword arguments for polars.DataFrame.sink_parquet/write_parquet.
                      If any kwargs other than 'description' or 'convert_to_absolute_path' are provided,
                      a Polars Code node will be created instead of a standard Output node.
                      Complex objects like IO streams or credential provider functions are NOT
                      supported via this method's Polars Code fallback.

        Returns:
            Self for method chaining (new FlowFrame pointing to the output node).
        """
        new_node_id = generate_node_id()

        is_path_input = isinstance(path, str | os.PathLike)
        if isinstance(path, os.PathLike):
            file_str = str(path)
        elif isinstance(path, str):
            file_str = path
        else:
            file_str = path
            is_path_input = False
        if "~" in file_str:
            file_str = os.path.expanduser(file_str)
        file_name = file_str.split(os.sep)[-1]
        use_polars_code = bool(kwargs.items()) or not is_path_input

        parquet_table = (
            input_schema.OutputParquetTable(compression=compression)
            if compression
            else input_schema.OutputParquetTable()
        )
        output_settings = input_schema.OutputSettings(
            file_type="parquet",
            name=file_name,
            directory=file_str if is_path_input else str(file_str),
            table_settings=parquet_table,
        )

        if is_path_input:
            try:
                output_settings.set_absolute_filepath()
                if convert_to_absolute_path:
                    output_settings.directory = output_settings.abs_file_path
            except Exception as e:
                logger.warning(f"Could not determine absolute path for {file_str}: {e}")

        if not use_polars_code:
            node_output = input_schema.NodeOutput(
                flow_id=self.flow_graph.flow_id,
                node_id=new_node_id,
                output_settings=output_settings,
                depending_on_id=self.node_id,
                description=description,
            )
            self.flow_graph.add_output(node_output)
        else:
            self._refuse_polars_code_writer_if_deferred("write_parquet")
            if not is_path_input:
                raise TypeError(
                    f"Input 'path' must be a string or Path-like object when using advanced "
                    f"write_parquet options (kwargs={kwargs.items()}), got {type(path)}."
                    " File-like objects are not supported with the Polars Code fallback."
                )

            fallback_kwargs = dict(kwargs)
            if compression is not None:
                fallback_kwargs.setdefault("compression", compression)
            path_arg_repr = repr(output_settings.directory)
            kwargs_repr = ", ".join(f"{k}={repr(v)}" for k, v in fallback_kwargs.items())
            args_str = f"path={path_arg_repr}"
            if kwargs_repr:
                args_str += f", {kwargs_repr}"

            code = f"input_df.sink_parquet({args_str})"
            logger.debug(f"Generated Polars Code: {code}")
            self._add_polars_code(new_node_id, code, description)

        return self._create_child_frame(new_node_id)

    def _write_simple_file(
        self,
        path: str | os.PathLike,
        file_type: str,
        table_settings: Any,
        fallback_code_template: str,
        *,
        compression: str | None = None,
        description: str = None,
        convert_to_absolute_path: bool = True,
        **kwargs: Any,
    ) -> FlowFrame:
        """Shared implementation for option-light file writers (ipc/ndjson/avro).

        Creates a standard Output node when only a path (and optional compression)
        is given; falls back to a Polars Code node when extra kwargs are supplied.
        ``compression`` rides on ``table_settings`` for the standard path and is
        merged into the generated call for the fallback path. ``fallback_code_template``
        is formatted with ``args_str``.
        """
        new_node_id = generate_node_id()

        is_path_input = isinstance(path, str | os.PathLike)
        file_str = str(path)
        if "~" in file_str:
            file_str = os.path.expanduser(file_str)
        file_name = file_str.split(os.sep)[-1]
        use_polars_code = bool(kwargs.items()) or not is_path_input

        output_settings = input_schema.OutputSettings(
            file_type=file_type,
            name=file_name,
            directory=file_str,
            table_settings=table_settings,
        )

        if is_path_input:
            try:
                output_settings.set_absolute_filepath()
                if convert_to_absolute_path:
                    output_settings.directory = output_settings.abs_file_path
            except Exception as e:
                logger.warning(f"Could not determine absolute path for {file_str}: {e}")

        if not use_polars_code:
            node_output = input_schema.NodeOutput(
                flow_id=self.flow_graph.flow_id,
                node_id=new_node_id,
                output_settings=output_settings,
                depending_on_id=self.node_id,
                description=description,
            )
            self.flow_graph.add_output(node_output)
        else:
            self._refuse_polars_code_writer_if_deferred(f"write_{file_type}")
            if not is_path_input:
                raise TypeError(
                    f"Input 'path' must be a string or Path-like object when using advanced "
                    f"write_{file_type} options (kwargs={kwargs.items()}), got {type(path)}."
                    " File-like objects are not supported with the Polars Code fallback."
                )
            fallback_kwargs = dict(kwargs)
            if compression is not None:
                fallback_kwargs.setdefault("compression", compression)
            path_arg_repr = repr(output_settings.directory)
            kwargs_repr = ", ".join(f"{k}={repr(v)}" for k, v in fallback_kwargs.items())
            args_str = f"path={path_arg_repr}"
            if kwargs_repr:
                args_str += f", {kwargs_repr}"
            code = fallback_code_template.format(args_str=args_str)
            logger.debug(f"Generated Polars Code: {code}")
            self._add_polars_code(new_node_id, code, description)

        return self._create_child_frame(new_node_id)

    def write_ipc(
        self,
        path: str | os.PathLike,
        *,
        compression: str | None = None,
        description: str = None,
        convert_to_absolute_path: bool = True,
        **kwargs: Any,
    ) -> FlowFrame:
        """Write the data to an Arrow IPC/Feather file.

        Creates a standard Output node if only 'path' (and optional compression)
        is provided; falls back to a Polars Code node (``sink_ipc``) when other
        keyword arguments are used.

        Args:
            path: Path or filename for the IPC/Arrow file.
            compression: One of 'uncompressed', 'lz4', 'zstd' (default 'uncompressed').
            description: Description of this operation for the ETL graph.
            convert_to_absolute_path: If the path needs to be set to a fixed location.
            **kwargs: Additional keyword arguments for polars.LazyFrame.sink_ipc.

        Returns:
            Self for method chaining (new FlowFrame pointing to the output node).
        """
        table_settings = (
            input_schema.OutputIpcTable(compression=compression) if compression else input_schema.OutputIpcTable()
        )
        return self._write_simple_file(
            path,
            "ipc",
            table_settings,
            "input_df.sink_ipc({args_str})",
            compression=compression,
            description=description,
            convert_to_absolute_path=convert_to_absolute_path,
            **kwargs,
        )

    def sink_ipc(
        self, path: str | os.PathLike, *, compression: str | None = None, description: str = None, **kwargs: Any
    ) -> FlowFrame:
        """Alias for :meth:`write_ipc`."""
        return self.write_ipc(path, compression=compression, description=description, **kwargs)

    def write_ndjson(
        self,
        path: str | os.PathLike,
        *,
        compression: str | None = None,
        description: str = None,
        convert_to_absolute_path: bool = True,
        **kwargs: Any,
    ) -> FlowFrame:
        """Write the data to a newline-delimited JSON file.

        Creates a standard Output node if only 'path' (and optional compression)
        is provided; falls back to a Polars Code node (``sink_ndjson``) when other
        keyword arguments are used.

        Args:
            path: Path or filename for the NDJSON file.
            compression: One of 'uncompressed', 'gzip', 'zstd' (default 'uncompressed').
            description: Description of this operation for the ETL graph.
            convert_to_absolute_path: If the path needs to be set to a fixed location.
            **kwargs: Additional keyword arguments for polars.LazyFrame.sink_ndjson.

        Returns:
            Self for method chaining (new FlowFrame pointing to the output node).
        """
        table_settings = (
            input_schema.OutputNdjsonTable(compression=compression)
            if compression
            else input_schema.OutputNdjsonTable()
        )
        return self._write_simple_file(
            path,
            "ndjson",
            table_settings,
            "input_df.sink_ndjson({args_str})",
            compression=compression,
            description=description,
            convert_to_absolute_path=convert_to_absolute_path,
            **kwargs,
        )

    def sink_ndjson(
        self, path: str | os.PathLike, *, compression: str | None = None, description: str = None, **kwargs: Any
    ) -> FlowFrame:
        """Alias for :meth:`write_ndjson`."""
        return self.write_ndjson(path, compression=compression, description=description, **kwargs)

    def write_avro(
        self,
        path: str | os.PathLike,
        *,
        compression: str | None = None,
        description: str = None,
        convert_to_absolute_path: bool = True,
        **kwargs: Any,
    ) -> FlowFrame:
        """Write the data to an Avro file.

        Creates a standard Output node if only 'path' (and optional compression)
        is provided; falls back to a Polars Code node (``collect().write_avro``)
        when other keyword arguments are used. Avro has no lazy sink, so writing
        materialises in the worker.

        Args:
            path: Path or filename for the Avro file.
            compression: One of 'uncompressed', 'snappy', 'deflate' (default 'uncompressed').
            description: Description of this operation for the ETL graph.
            convert_to_absolute_path: If the path needs to be set to a fixed location.
            **kwargs: Additional keyword arguments for polars.DataFrame.write_avro.

        Returns:
            Self for method chaining (new FlowFrame pointing to the output node).
        """
        table_settings = (
            input_schema.OutputAvroTable(compression=compression) if compression else input_schema.OutputAvroTable()
        )
        return self._write_simple_file(
            path,
            "avro",
            table_settings,
            "input_df.collect().write_avro({args_str})",
            compression=compression,
            description=description,
            convert_to_absolute_path=convert_to_absolute_path,
            **kwargs,
        )

    def write_csv(
        self,
        file: str | os.PathLike,
        *,
        separator: str = ",",
        encoding: str = "utf-8",
        description: str = None,
        convert_to_absolute_path: bool = True,
        **kwargs: Any,
    ) -> FlowFrame:
        new_node_id = generate_node_id()
        is_path_input = isinstance(file, str | os.PathLike)
        if isinstance(file, os.PathLike):
            file_str = str(file)
        elif isinstance(file, str):
            file_str = file
        else:
            file_str = file
            is_path_input = False
        if "~" in file_str:
            file_str = os.path.expanduser(file_str)
        file_name = file_str.split(os.sep)[-1] if is_path_input else "output.csv"

        use_polars_code = bool(kwargs) or not is_path_input
        output_settings = input_schema.OutputSettings(
            file_type="csv",
            name=file_name,
            directory=file_str if is_path_input else str(file_str),
            table_settings=input_schema.OutputCsvTable(delimiter=separator, encoding=encoding),
        )
        if is_path_input:
            try:
                output_settings.set_absolute_filepath()
                if convert_to_absolute_path:
                    output_settings.directory = output_settings.abs_file_path
            except Exception as e:
                logger.warning(f"Could not determine absolute path for {file_str}: {e}")

        if not use_polars_code:
            node_output = input_schema.NodeOutput(
                flow_id=self.flow_graph.flow_id,
                node_id=new_node_id,
                output_settings=output_settings,
                depending_on_id=self.node_id,
                description=description,
            )
            self.flow_graph.add_output(node_output)
        else:
            self._refuse_polars_code_writer_if_deferred("write_csv")
            if not is_path_input:
                raise TypeError(
                    f"Input 'file' must be a string or Path-like object when using advanced "
                    f"write_csv options (kwargs={kwargs}), got {type(file)}."
                    " File-like objects are not supported with the Polars Code fallback."
                )

            path_arg_repr = repr(output_settings.directory)

            all_kwargs_for_code = {
                "separator": separator,
                "encoding": encoding,
                **kwargs,
            }
            kwargs_repr = ", ".join(f"{k}={repr(v)}" for k, v in all_kwargs_for_code.items())

            args_str = f"file={path_arg_repr}"
            if kwargs_repr:
                args_str += f", {kwargs_repr}"

            code = f"input_df.collect().write_csv({args_str})"
            logger.debug(f"Generated Polars Code: {code}")
            self._add_polars_code(new_node_id, code, description)

        return self._create_child_frame(new_node_id)

    def write_excel(
        self,
        path: str | os.PathLike,
        *,
        worksheet: str = "Sheet1",
        write_mode: Literal["overwrite", "update", "create"] = "overwrite",
        description: str = None,
        convert_to_absolute_path: bool = True,
        **kwargs: Any,
    ) -> FlowFrame:
        """
        Write the data to an Excel file.

        Args:
            path: Path or filename for the Excel file.
            worksheet: Name of the worksheet, defaults to 'Sheet1'.
            write_mode: How to treat an existing file. 'overwrite' replaces the whole workbook,
                'update' replaces only the target worksheet and keeps every other sheet,
                'create' refuses to write when the file already exists. Defaults to 'overwrite'.
            description: Description of this operation for the ETL graph.
            convert_to_absolute_path: If the path needs to be set to a fixed location.
            **kwargs: Additional keyword arguments for polars.DataFrame.write_excel.
                      If any extra kwargs are provided, a Polars Code node will be created
                      instead of a standard Output node.

        Returns:
            Self for method chaining (new FlowFrame pointing to the output node).
        """
        new_node_id = generate_node_id()
        is_path_input = isinstance(path, str | os.PathLike)
        if isinstance(path, os.PathLike):
            file_str = str(path)
        elif isinstance(path, str):
            file_str = path
        else:
            file_str = path
            is_path_input = False
        if "~" in file_str:
            file_str = os.path.expanduser(file_str)
        file_name = file_str.split(os.sep)[-1] if is_path_input else "output.xlsx"

        use_polars_code = bool(kwargs) or not is_path_input
        output_settings = input_schema.OutputSettings(
            file_type="excel",
            name=file_name,
            directory=file_str if is_path_input else str(file_str),
            write_mode=write_mode,
            table_settings=input_schema.OutputExcelTable(sheet_name=worksheet),
        )
        if is_path_input:
            try:
                output_settings.set_absolute_filepath()
                if convert_to_absolute_path:
                    output_settings.directory = output_settings.abs_file_path
            except Exception as e:
                logger.warning(f"Could not determine absolute path for {file_str}: {e}")

        if not use_polars_code:
            node_output = input_schema.NodeOutput(
                flow_id=self.flow_graph.flow_id,
                node_id=new_node_id,
                output_settings=output_settings,
                depending_on_id=self.node_id,
                description=description,
            )
            self.flow_graph.add_output(node_output)
        else:
            self._refuse_polars_code_writer_if_deferred("write_excel")
            if not is_path_input:
                raise TypeError(
                    f"Input 'path' must be a string or Path-like object when using advanced "
                    f"write_excel options (kwargs={kwargs}), got {type(path)}."
                    " File-like objects are not supported with the Polars Code fallback."
                )
            if write_mode != "overwrite":
                raise TypeError(
                    f"write_mode={write_mode!r} cannot be combined with advanced write_excel options "
                    f"(kwargs={kwargs}): polars.DataFrame.write_excel always overwrites the whole workbook."
                )

            path_arg_repr = repr(output_settings.directory)

            all_kwargs_for_code = {
                "worksheet": worksheet,
                **kwargs,
            }
            kwargs_repr = ", ".join(f"{k}={repr(v)}" for k, v in all_kwargs_for_code.items())

            args_str = f"{path_arg_repr}"
            if kwargs_repr:
                args_str += f", {kwargs_repr}"

            code = f"input_df.collect().write_excel({args_str})"
            logger.debug(f"Generated Polars Code: {code}")
            self._add_polars_code(new_node_id, code, description)

        return self._create_child_frame(new_node_id)

    def write_parquet_to_cloud_storage(
        self,
        path: str,
        connection_name: str | None = None,
        compression: Literal["snappy", "gzip", "brotli", "lz4", "zstd"] = "snappy",
        description: str | None = None,
    ) -> FlowFrame:
        """
        Write the data frame to cloud storage in Parquet format.

        Args:
            path (str): The destination path in cloud storage where the Parquet file will be written.
            connection_name (Optional[str], optional): The name of the storage connection
                that a user can create. If None, uses the default connection. Defaults to None.
            compression (Literal["snappy", "gzip", "brotli", "lz4", "zstd"], optional):
                The compression algorithm to use for the Parquet file. Defaults to "snappy".
            description (Optional[str], optional): Description of this operation for the ETL graph.

        Returns:
            FlowFrame: A new child data frame representing the written data.
        """

        new_node_id = add_write_ff_to_cloud_storage(
            path,
            flow_graph=self.flow_graph,
            connection_name=connection_name,
            depends_on_node_id=self.node_id,
            parquet_compression=compression,
            file_format="parquet",
            description=description,
        )
        return self._create_child_frame(new_node_id)

    def write_csv_to_cloud_storage(
        self,
        path: str,
        connection_name: str | None = None,
        delimiter: str = ";",
        encoding: CsvEncoding = "utf8",
        description: str | None = None,
    ) -> FlowFrame:
        """
        Write the data frame to cloud storage in CSV format.

        Args:
            path (str): The destination path in cloud storage where the CSV file will be written.
            connection_name (Optional[str], optional): The name of the storage connection
                that a user can create. If None, uses the default connection. Defaults to None.
            delimiter (str, optional): The character used to separate fields in the CSV file.
                Defaults to ";".
            encoding (CsvEncoding, optional): The character encoding to use for the CSV file.
                Defaults to "utf8".
            description (Optional[str], optional): Description of this operation for the ETL graph.

        Returns:
            FlowFrame: A new child data frame representing the written data.
        """
        new_node_id = add_write_ff_to_cloud_storage(
            path,
            flow_graph=self.flow_graph,
            connection_name=connection_name,
            depends_on_node_id=self.node_id,
            csv_delimiter=delimiter,
            csv_encoding=encoding,
            file_format="csv",
            description=description,
        )
        return self._create_child_frame(new_node_id)

    def write_delta(
        self,
        path: str,
        connection_name: str | None = None,
        write_mode: Literal["overwrite", "append", "error", "upsert", "update", "delete"] = "overwrite",
        partition_by: list[str] | None = None,
        merge_keys: list[str] | None = None,
        track_changes: bool = False,
        description: str | None = None,
    ) -> FlowFrame:
        """
        Write the data frame to cloud storage in Delta Lake format.

        Args:
            path (str): The destination path in cloud storage where the Delta table will be written.
            connection_name (Optional[str], optional): The name of the storage connection
                that a user can create. If None, uses the default connection. Defaults to None.
            write_mode (optional): How to handle existing data. ``"overwrite"`` replaces it,
                ``"append"`` adds to it, ``"error"`` fails if the table already exists, and
                ``"upsert"`` / ``"update"`` / ``"delete"`` merge on ``merge_keys``.
                Defaults to ``"overwrite"``.
            partition_by (Optional[List[str]], optional): Delta partition columns (applied at table
                creation; writes to an existing table must match its partitioning).
            merge_keys (Optional[List[str]], optional): Column names for merge operations
                (required for upsert/update/delete).
            track_changes (bool, optional): Turn the table's change feed on so readers can ask for
                changes only (see ``changes_since`` on :func:`flowfile_frame.scan_delta`).
                Enable-only: ``False`` never turns tracking off. Not allowed with
                ``write_mode="overwrite"``.
            description (Optional[str], optional): Description of this operation for the ETL graph.
        Returns:
            FlowFrame: A new child data frame representing the written data.
        """
        new_node_id = add_write_ff_to_cloud_storage(
            path,
            flow_graph=self.flow_graph,
            connection_name=connection_name,
            depends_on_node_id=self.node_id,
            write_mode=write_mode,
            file_format="delta",
            partition_by=partition_by,
            merge_keys=merge_keys,
            track_changes=track_changes,
            description=description,
        )
        return self._create_child_frame(new_node_id)

    def write_json_to_cloud_storage(
        self,
        path: str,
        connection_name: str | None = None,
        description: str | None = None,
    ) -> FlowFrame:
        """
        Write the data frame to cloud storage in JSON format.

        Args:
            path (str): The destination path in cloud storage where the JSON file will be written.
            connection_name (Optional[str], optional): The name of the storage connection
                that a user can create. If None, uses the default connection. Defaults to None.
            description (Optional[str], optional): Description of this operation for the ETL graph.
        Returns:
            FlowFrame: A new child data frame representing the written data.
        """
        new_node_id = add_write_ff_to_cloud_storage(
            path,
            flow_graph=self.flow_graph,
            connection_name=connection_name,
            depends_on_node_id=self.node_id,
            file_format="json",
            description=description,
        )
        return self._create_child_frame(new_node_id)

    def write_catalog_table(
        self,
        table_name: str,
        *,
        schema: SchemaReference | None = None,
        namespace_id: int | None = None,
        namespace_full_name: str | None = None,
        write_mode: Literal[
            "overwrite", "error", "append", "upsert", "update", "delete", "scd2", "virtual"
        ] = "overwrite",
        merge_keys: list[str] | None = None,
        partition_by: list[str] | None = None,
        scd2_compare_columns: list[str] | None = None,
        scd2_full_snapshot: bool = False,
        scd2_surrogate_key_column: str = "sk",
        scd2_valid_from_column: str = "valid_from",
        scd2_valid_to_column: str = "valid_to",
        scd2_is_current_column: str = "is_current",
        scd2_partition_on_current: bool = True,
        scd2_output_mode: Literal["input", "changed", "current"] = "input",
        track_changes: bool = False,
        description: str | None = None,
    ) -> FlowFrame:
        """Write the data frame to the Flowfile catalog.

        Args:
            table_name: Name of the catalog table to write to.
            schema: Target :class:`SchemaReference`. Preferred over ``namespace_id``.
            namespace_id: Legacy. Raw namespace id; mutually exclusive with ``schema``.
            namespace_full_name: Portable ``"catalog.schema"`` name, resolved at run time.
            write_mode: How to handle existing data. ``"scd2"`` tracks history: changed
                rows are end-dated and re-inserted as a new version (see the ``scd2_*``
                arguments). ``"virtual"`` registers the result as a virtual catalog table
                backed by this flow (requires the flow to be registered with the catalog
                first; see :func:`flowfile_frame.register_flow_with_catalog`).
            merge_keys: Column names for merge operations (required for upsert/update/delete/scd2;
                also the SCD2 business key).
            partition_by: Delta partition columns (applied at table creation; appends
                must match the existing partitioning).
            scd2_compare_columns: Columns compared for change detection when ``write_mode="scd2"``.
                Empty means every non-key, non-system column.
            scd2_full_snapshot: When ``write_mode="scd2"``, end-date current rows whose business
                key is absent from this run's input.
            scd2_surrogate_key_column: Name of the generated surrogate-key column
                (``write_mode="scd2"``).
            scd2_valid_from_column: Name of the generated valid-from column (``write_mode="scd2"``).
            scd2_valid_to_column: Name of the generated valid-to column (``write_mode="scd2"``).
            scd2_is_current_column: Name of the generated is-current column (``write_mode="scd2"``).
            scd2_partition_on_current: Partition new SCD2 tables by the is-current column.
            scd2_output_mode: What an ``write_mode="scd2"`` write passes downstream:
                ``"input"`` (the default) returns the input rows, in order, with the four
                generated columns looked up from the table — new rows carry the surrogate key
                minted by this write, unchanged rows their existing one; ``"changed"`` returns
                only the row versions this write inserted or closed; ``"current"`` returns the
                table's whole current slice, including keys absent from this input.
            track_changes: Turn the table's change feed on so readers can ask for changes only
                (see ``changes_since`` on :func:`flowfile_frame.read_catalog_table`).
                Enable-only: ``False`` never turns tracking off. Not allowed with
                ``write_mode`` ``"overwrite"``, ``"virtual"`` or ``"scd2"``.
            description: Optional description for this operation.

        Returns:
            FlowFrame: A new child data frame representing the written data. For
            ``write_mode="scd2"`` it carries the input's columns followed by the four generated
            columns (surrogate key, valid-from, valid-to, is-current), with the rows selected by
            ``scd2_output_mode``. Every other write mode passes the input through unchanged.

        Raises:
            ValueError: If both ``schema`` and ``namespace_id`` are provided.
        """
        from flowfile_frame.catalog import add_write_to_catalog

        new_node_id = add_write_to_catalog(
            self.flow_graph,
            depends_on_node_id=self.node_id,
            table_name=table_name,
            schema=schema,
            namespace_id=namespace_id,
            namespace_full_name=namespace_full_name,
            write_mode=write_mode,
            merge_keys=merge_keys,
            partition_by=partition_by,
            scd2_compare_columns=scd2_compare_columns,
            scd2_full_snapshot=scd2_full_snapshot,
            scd2_surrogate_key_column=scd2_surrogate_key_column,
            scd2_valid_from_column=scd2_valid_from_column,
            scd2_valid_to_column=scd2_valid_to_column,
            scd2_is_current_column=scd2_is_current_column,
            scd2_partition_on_current=scd2_partition_on_current,
            scd2_output_mode=scd2_output_mode,
            track_changes=track_changes,
            description=description,
        )
        return self._create_child_frame(new_node_id)

    def write_database(
        self,
        connection_name: str,
        table_name: str,
        *,
        schema_name: str | None = None,
        if_exists: Literal["append", "replace", "fail"] = "append",
        description: str | None = None,
    ) -> FlowFrame:
        """Write the data frame to a database using a stored connection.

        Args:
            connection_name: Name of the stored database connection to use.
            table_name: Name of the table to write to.
            schema_name: Database schema name (e.g., 'public' for PostgreSQL).
            if_exists: What to do if the table already exists.
            description: Optional description for this operation.

        Returns:
            FlowFrame: A new child data frame representing the written data.
        """
        from flowfile_frame.database.frame_helpers import add_write_to_database

        new_node_id = add_write_to_database(
            self.flow_graph,
            depends_on_node_id=self.node_id,
            connection_name=connection_name,
            table_name=table_name,
            schema_name=schema_name,
            if_exists=if_exists,
            description=description,
        )
        return self._create_child_frame(new_node_id)

    def group_by(self, *by, description: str = None, maintain_order=False, **named_by) -> GroupByFrame:
        """
        Start a group by operation.

        Parameters:
            *by: Column names or expressions to group by
            description: add optional description to this step for the frontend
            maintain_order: Keep groups in the order they appear in the data
            **named_by: Additional columns to group by with custom names

        Returns:
            GroupByFrame object for aggregations
        """
        refuse_parameter_as_column((by, named_by), "group_by")
        new_node_id = generate_node_id()
        by_cols = []
        for col_expr in by:
            if isinstance(col_expr, str):
                by_cols.append(col_expr)
            elif isinstance(col_expr, Expr):
                by_cols.append(col_expr)
            elif isinstance(col_expr, Selector):
                by_cols.append(col_expr)
            elif isinstance(col_expr, list | tuple):
                by_cols.extend(col_expr)

        for new_name, col_expr in named_by.items():
            if isinstance(col_expr, str):
                by_cols.append(col(col_expr).alias(new_name))
            elif isinstance(col_expr, Expr):
                by_cols.append(col_expr.alias(new_name))
        return GroupByFrame(
            node_id=new_node_id,
            parent_frame=self,
            by_cols=by_cols,
            maintain_order=maintain_order,
            description=description,
        )

    @contextmanager
    def group(self, name: str, *, color: GroupColor | None = None) -> Iterator[FlowFrame]:
        """Group every node created inside this block into a labeled visual container.

        Organizational only: groups affect the canvas overview/layout, never execution
        or results. This is unrelated to :meth:`group_by` (which performs aggregation).

        Example:
            >>> with df.group("Clean customer data"):
            ...     df = df.filter(col("age") > 18).select(["id", "name"])
        """
        before = set(self.flow_graph._node_db.keys())
        try:
            yield self
        finally:
            # Group only nodes that are new in this block AND not already grouped
            # (so an inner `with df.group(...)` keeps its own membership).
            new_node_ids = [
                node_id
                for node_id, node in self.flow_graph._node_db.items()
                if node_id not in before and getattr(node.setting_input, "group_id", None) is None
            ]
            if new_node_ids:
                self.flow_graph.create_group(name, new_node_ids, color=color)

    def set_group(self, name: str, *, color: GroupColor | None = None) -> FlowFrame:
        """Assign this frame's current node to a (new or existing) named visual group.

        Returns ``self`` for chaining. Organizational only; see :meth:`group` for the
        block form. Unrelated to :meth:`group_by` (aggregation).

        Example:
            >>> df = df.filter(col("age") > 18).set_group("Clean customer data")
        """
        self.flow_graph.assign_node_to_named_group(self.node_id, name, color=color)
        return self

    def to_graph(self):
        """Get the underlying ETL graph."""
        return self.flow_graph

    def save_graph(self, file_path: str, auto_arrange: bool = True):
        """Save the graph"""
        if auto_arrange:
            self.flow_graph.apply_layout()
        self.flow_graph.save_flow(file_path)

    def to_flow_output(self, name: str, *, description: str | None = None) -> FlowFrame:
        """Mark this frame as the flow output ``name`` (a ``flow_output`` node) and return it unchanged.

        A parent flow's ``RunFlow`` exposes it as ``run[name]``; outputs are ordered by the
        order they were declared in. The ``flow_output`` node has no output handle on the
        canvas, so the returned frame is this one, not the sink.
        """
        from flowfile_frame.run_flow import _to_flow_output

        return _to_flow_output(self, name, description)

    def collect(self, *args, **kwargs) -> pl.DataFrame:
        """Collect lazy data into memory.

        On a deferred frame this runs the whole flow first (see :meth:`_materialised_lazyframe`), and so
        does a frame below a gate, because only a run decides which gate exit is live.
        """
        if self._deferred or self._below_a_gate():
            return self._materialised_lazyframe().collect(*args, **kwargs)
        if hasattr(self.data, "collect"):
            return self.data.collect(*args, **kwargs)
        return self.data

    def _below_a_gate(self) -> bool:
        """Whether this node or any ancestor is a gate (a gate exit's frame points at the gate node)."""
        return any(n.node_type == "gate" for n in ancestors(self.flow_graph.get_node(self.node_id)).values())

    def _materialised_lazyframe(self) -> pl.LazyFrame:
        """The LazyFrame to materialise: ``self.data``, or a deferred frame's real output.

        A deferred frame runs ``flow_graph.run_graph()`` (every output and writer node of the
        graph runs with it) and is judged on this node and its ancestors only, so a failure on
        an unrelated branch does not fail it. An ancestor without a run result was skipped by the
        planner (not configured, or below a failed node) and fails it too; a deliberately skipped
        node keeps a result. When a gate routed this frame away (the node was deliberately
        skipped, or this is a gate's dead exit) the zero-row typed frame is returned. A frame
        below a gate takes the same path, because its build-time plan passes through both exits.
        """
        if not (self._deferred or self._below_a_gate()):
            return self.data
        run_info = self.flow_graph.run_graph()
        results = {result.node_id: result for result in run_info.node_step_result}
        node = self.flow_graph.get_node(self.node_id)
        lineage = ancestors(node)
        problems = [
            f"  node {node_id}: {results[node_id].error}"
            for node_id in lineage
            if node_id in results and results[node_id].success is False
        ]
        problems += [
            f"  node {node_id}: {n.results.errors or ('not configured' if not n.is_correct else 'did not run')}"
            for node_id, n in lineage.items()
            if node_id not in results
        ]
        if problems:
            errors = "\n".join(problems)
            raise NativeNodeError(f"Running the flow failed for node {self.node_id}:\n{errors}")
        own_result = results.get(self.node_id)
        closed_handles = self.flow_graph._last_closed_gate_handles.get(self.node_id, ())
        if (own_result is not None and own_result.skipped) or self.output_handle in closed_handles:
            # self.data holds real rows when the frame was built after an earlier run; the routing wins.
            return self.data.clear()
        frame = node.get_output(self.output_handle).data_frame
        return frame.lazy() if isinstance(frame, pl.DataFrame) else frame

    def _with_flowfile_formula(
        self,
        flowfile_formula: str,
        output_column_name: str,
        description: str = None,
        output_column_datatype: str = "Auto",
    ) -> FlowFrame:
        new_node_id = generate_node_id()
        function_settings = input_schema.NodeFormula(
            flow_id=self.flow_graph.flow_id,
            node_id=new_node_id,
            depending_on_id=self.node_id,
            function=transform_schema.FunctionInput(
                function=flowfile_formula,
                field=transform_schema.FieldInput(name=output_column_name, data_type=output_column_datatype),
            ),
            description=description,
        )
        self.flow_graph.add_formula(function_settings)
        return self._create_child_frame(new_node_id)

    def _with_flowfile_formulas(
        self,
        entries: list[tuple[str, str, str]],
        description: str = None,
    ) -> FlowFrame:
        """One Formula node holding every ``(output_column_name, formula, data_type)`` entry.

        The node evaluates its entries sequentially — entry N sees the outputs of entries
        1..N-1 — so callers that need Polars' parallel semantics must check independence
        first (:func:`entries_are_independent`).
        """
        new_node_id = generate_node_id()
        function_settings = input_schema.NodeFormula(
            flow_id=self.flow_graph.flow_id,
            node_id=new_node_id,
            depending_on_id=self.node_id,
            functions=[
                transform_schema.FunctionInput(
                    function=formula,
                    field=transform_schema.FieldInput(name=output_column_name, data_type=data_type),
                )
                for output_column_name, formula, data_type in entries
            ],
            description=description,
        )
        self.flow_graph.add_formula(function_settings)
        return self._create_child_frame(new_node_id)

    def head(self, n: int, description: str = None):
        new_node_id = generate_node_id()
        settings = input_schema.NodeSample(
            flow_id=self.flow_graph.flow_id,
            node_id=new_node_id,
            depending_on_id=self.node_id,
            sample_size=n,
            description=description,
        )
        self.flow_graph.add_sample(settings)
        return self._create_child_frame(new_node_id)

    def limit(self, n: int, description: str = None):
        return self.head(n, description)

    def sample(
        self,
        n: int | None = None,
        *,
        fraction: float | None = None,
        seed: int | None = None,
        description: str = None,
    ):
        """Take a uniform random sample of rows.

        Unlike ``polars``, which only offers ``sample`` on eager DataFrames,
        this stays lazy: it emits a sample node that filters on a shuffled row
        rank, so nothing is materialized until ``collect()``. Sampling more rows
        than the frame holds returns the whole frame, and the original row order
        is preserved. Sampling with replacement is not supported.

        Args:
            n: Number of rows to keep. Mutually exclusive with `fraction`.
            fraction: Share of rows to keep, between 0 and 1. Mutually exclusive with `n`.
            seed: Seed for a reproducible sample; None draws a fresh permutation
                on every execution.
            description: Optional node description shown in the Designer.

        Returns:
            A new FlowFrame over the sampled rows.
        """
        if (n is None) == (fraction is None):
            raise ValueError("Provide exactly one of n or fraction")
        new_node_id = generate_node_id()
        if fraction is not None:
            if not 0 < fraction <= 1:
                raise ValueError("fraction must be greater than 0 and at most 1")
            settings = input_schema.NodeSample(
                flow_id=self.flow_graph.flow_id,
                node_id=new_node_id,
                depending_on_id=self.node_id,
                sample_method="random_fraction",
                fraction=fraction * 100.0,
                seed=seed,
                description=description,
            )
        else:
            settings = input_schema.NodeSample(
                flow_id=self.flow_graph.flow_id,
                node_id=new_node_id,
                depending_on_id=self.node_id,
                sample_method="random",
                sample_size=n,
                seed=seed,
                description=description,
            )
        self.flow_graph.add_sample(settings)
        return self._create_child_frame(new_node_id)

    def solve_graph(
        self,
        col_from: str,
        col_to: str,
        output_column_name: str = "graph_group",
        *,
        description: str | None = None,
    ) -> FlowFrame:
        new_node_id = generate_node_id()
        graph_solver_settings = input_schema.NodeGraphSolver(
            flow_id=self.flow_graph.flow_id,
            node_id=new_node_id,
            depending_on_id=self.node_id,
            graph_solver_input=transform_schema.GraphSolverInput(
                col_from=col_from,
                col_to=col_to,
                output_column_name=output_column_name,
            ),
            description=description,
        )
        self.flow_graph.add_graph_solver(graph_solver_settings)
        return self._create_child_frame(new_node_id)

    def dynamic_rename(
        self,
        mode: Literal["prefix", "suffix", "formula", "first_row"] = "prefix",
        *,
        prefix: str = "",
        suffix: str = "",
        formula: str = "",
        columns: list[str] | None = None,
        data_type: Literal["Numeric", "String", "Date", "Other", "Boolean", "Binary", "Complex"] | None = None,
        description: str | None = None,
    ) -> FlowFrame:
        """
        Rename many columns at once via a single rule.

        One node, four modes — useful when you need a uniform transformation
        across columns (e.g. prefixing every numeric column with ``"num_"``)
        or want to promote the first row of a CSV to headers.

        Parameters
        ----------
        mode:
            How to compute new column names.

            - ``"prefix"`` — prepend *prefix* to each selected column's name.
            - ``"suffix"`` — append *suffix* to each selected column's name.
            - ``"formula"`` — evaluate a Flowfile formula with
              ``[column_name]`` bound to each column's current name
              (e.g. ``"uppercase([column_name])"``).
            - ``"first_row"`` — promote the first row of data to column
              headers and drop that row from the output.
        prefix:
            Required and only valid when ``mode="prefix"``.
        suffix:
            Required and only valid when ``mode="suffix"``.
        formula:
            Required and only valid when ``mode="formula"``.
        columns:
            When given, apply the rule only to these columns. Mutually
            exclusive with *data_type*.
        data_type:
            When given, apply the rule only to columns of this data-type
            group. Mutually exclusive with *columns*.
        description:
            Optional node description shown in the visual designer.

        Returns
        -------
        FlowFrame
            A new FlowFrame with renamed columns. In ``"first_row"`` mode,
            the first row is dropped from the output regardless of which
            columns were selected for renaming.
        """
        if mode == "prefix":
            if not prefix:
                raise ValueError("dynamic_rename: 'prefix' is required when mode='prefix'.")
            if suffix or formula:
                raise ValueError("dynamic_rename: only 'prefix' may be set when mode='prefix'.")
        elif mode == "suffix":
            if not suffix:
                raise ValueError("dynamic_rename: 'suffix' is required when mode='suffix'.")
            if prefix or formula:
                raise ValueError("dynamic_rename: only 'suffix' may be set when mode='suffix'.")
        elif mode == "formula":
            if not formula.strip():
                raise ValueError("dynamic_rename: 'formula' is required when mode='formula'.")
            if prefix or suffix:
                raise ValueError("dynamic_rename: only 'formula' may be set when mode='formula'.")
        elif mode == "first_row":
            if prefix or suffix or formula:
                raise ValueError(
                    "dynamic_rename: 'prefix', 'suffix' and 'formula' must be empty when mode='first_row'."
                )

        if columns is not None and data_type is not None:
            raise ValueError("dynamic_rename: pass at most one of 'columns' or 'data_type'.")

        if columns is not None:
            selection_mode = "list"
        elif data_type is not None:
            selection_mode = "data_type"
        else:
            selection_mode = "all"

        new_node_id = generate_node_id()
        rename_settings = input_schema.NodeDynamicRename(
            flow_id=self.flow_graph.flow_id,
            node_id=new_node_id,
            dynamic_rename_input=transform_schema.DynamicRenameInput(
                rename_mode=mode,
                prefix=prefix,
                suffix=suffix,
                formula=formula,
                selection_mode=selection_mode,
                selected_columns=list(columns or []),
                selected_data_type=data_type,
            ),
            pos_x=200,
            pos_y=150,
            is_setup=True,
            depending_on_id=self.node_id,
            description=description,
        )
        self.flow_graph.add_dynamic_rename(rename_settings)
        return self._create_child_frame(new_node_id)

    def multi_field_formula(
        self,
        formula: str,
        columns: list[str] | None = None,
        *,
        data_type: Literal["Numeric", "String", "Date", "Other", "Boolean", "Binary", "Complex"] | None = None,
        prefix: str = "",
        suffix: str = "",
        output_data_type: str | None = None,
        description: str | None = None,
    ) -> FlowFrame:
        """
        Apply one Flowfile formula to many columns at once.

        One node instead of a formula node per column — useful when the same
        calculation has to run over every numeric column, over a listed subset,
        or over the whole frame. Inside the formula three placeholders bind to
        the column currently being processed:

        - ``[_CurrentField_]`` — that column's value.
        - ``[_CurrentFieldName_]`` — its name, as a string literal.
        - ``[_CurrentFieldType_]`` — its Polars dtype, as a string literal
          (the base token, so ``Datetime(time_unit='us')`` binds as
          ``"Datetime"``).

        Every expression is evaluated against the *original* input values in a
        single step, so a formula may reference another selected column
        (``"[_CurrentField_] / [Total] * 100"`` reads the untouched ``Total``
        even when ``Total`` is itself being overwritten).

        Parameters
        ----------
        formula:
            The Flowfile formula to apply, carrying the placeholders above.
            Required and may not be blank.
        columns:
            When given, apply the formula only to these columns, in this order.
            Names that do not exist are silently skipped. Mutually exclusive
            with *data_type*.
        data_type:
            When given, apply the formula only to columns of this data-type
            group. Mutually exclusive with *columns*.
        prefix:
            When *prefix* or *suffix* is set, results are written to new
            ``f"{prefix}{name}{suffix}"`` columns appended after the existing
            ones instead of overwriting the source columns.
        suffix:
            See *prefix*.
        output_data_type:
            Cast every result to this type (e.g. ``"Float64"``). ``None``
            (default) keeps whatever type the formula produces.
        description:
            Optional node description shown in the visual designer.

        Returns
        -------
        FlowFrame
            A new FlowFrame with the formula applied. Selecting no column is a
            no-op: the frame passes through unchanged.

        Examples
        --------
        >>> df.multi_field_formula("[_CurrentField_] * 1.21", data_type="Numeric", suffix="_incl_vat")
        >>> df.multi_field_formula("trim([_CurrentField_])", columns=["name", "city"])
        """
        if not formula.strip():
            raise ValueError("multi_field_formula: 'formula' is required.")
        if columns is not None and data_type is not None:
            raise ValueError("multi_field_formula: pass at most one of 'columns' or 'data_type'.")

        if columns is not None:
            selection_mode = "list"
        elif data_type is not None:
            selection_mode = "data_type"
        else:
            selection_mode = "all"

        new_node_id = generate_node_id()
        formula_settings = input_schema.NodeMultiFieldFormula(
            flow_id=self.flow_graph.flow_id,
            node_id=new_node_id,
            multi_field_formula_input=transform_schema.MultiFieldFormulaInput(
                formula=formula,
                selection_mode=selection_mode,
                selected_columns=list(columns or []),
                selected_data_type=data_type,
                output_mode="new" if (prefix or suffix) else "replace",
                output_prefix=prefix,
                output_suffix=suffix,
                output_data_type=output_data_type or "Auto",
            ),
            pos_x=200,
            pos_y=150,
            is_setup=True,
            depending_on_id=self.node_id,
            description=description,
        )
        self.flow_graph.add_multi_field_formula(formula_settings)
        return self._create_child_frame(new_node_id)

    def cache(self) -> FlowFrame:
        setting_input = self.get_node_settings().setting_input
        setting_input.cache_results = True
        self.data.cache()
        return self

    def get_node_settings(self) -> FlowNode:
        return self.flow_graph.get_node(self.node_id)

    def pivot(
        self,
        on: str | list[str],
        *,
        index: str | list[str] | None = None,
        values: str | list[str] | None = None,
        aggregate_function: str | None = "first",
        maintain_order: bool = True,
        sort_columns: bool = False,
        separator: str = "_",
        description: str = None,
    ) -> FlowFrame:
        """
        Pivot a DataFrame from long to wide format.

        Parameters
        ----------
        on: str | list[str]
            Column values to use as column names in the pivoted DataFrame
        index: str | list[str] | None
            Column(s) to use as index/row identifiers in the pivoted DataFrame
        values: str | list[str] | None
            Column(s) that contain the values of the pivoted DataFrame
        aggregate_function: str | None
            Function to aggregate values if there are duplicate entries.
            Options: 'first', 'last', 'min', 'max', 'sum', 'mean', 'median', 'count'
        maintain_order: bool
            Whether to maintain the order of the columns/rows as they appear in the source
        sort_columns: bool
            Whether to sort the output columns
        separator: str
            Separator to use when joining column levels in the pivoted DataFrame
        description: str
            Description of this operation for the ETL graph

        Returns
        -------
        FlowFrame
            A new FlowFrame with pivoted data
        """
        refuse_parameter_as_column((on, index, values), "pivot")
        new_node_id = generate_node_id()

        on_value = on[0] if isinstance(on, list) and len(on) == 1 else on

        if index is None:
            index_columns = []
        elif isinstance(index, str):
            index_columns = [index]
        else:
            index_columns = list(index)

        if values is None:
            raise ValueError("Values parameter must be specified for pivot operation")

        value_col = values if isinstance(values, str) else values[0]

        valid_aggs = ["first", "last", "min", "max", "sum", "mean", "median", "count"]
        if aggregate_function not in valid_aggs:
            raise ValueError(
                f"Invalid aggregate_function: {aggregate_function}. " f"Must be one of: {', '.join(valid_aggs)}"
            )

        can_use_native = isinstance(on_value, str) and isinstance(value_col, str) and aggregate_function in valid_aggs

        if can_use_native:
            pivot_input = transform_schema.PivotInput(
                index_columns=index_columns,
                pivot_column=on_value,
                value_col=value_col,
                aggregations=[aggregate_function],
            )

            pivot_settings = input_schema.NodePivot(
                flow_id=self.flow_graph.flow_id,
                node_id=new_node_id,
                pivot_input=pivot_input,
                pos_x=200,
                pos_y=150,
                is_setup=True,
                depending_on_id=self.node_id,
                description=description or f"Pivot {value_col} by {on_value}",
            )

            self.flow_graph.add_pivot(pivot_settings)
        else:
            on_repr = repr(on)
            index_repr = repr(index)
            values_repr = repr(values)

            code = f"""
    # Perform pivot operation
    result = input_df.pivot(
        on={on_repr},
        index={index_repr},
        values={values_repr},
        aggregate_function='{aggregate_function}',
        maintain_order={maintain_order},
        sort_columns={sort_columns},
        separator="{separator}"
    )
    result
    """
            if description is None:
                on_str = on if isinstance(on, str) else ", ".join(on if isinstance(on, list) else [on])
                values_str = (
                    values if isinstance(values, str) else ", ".join(values if isinstance(values, list) else [values])
                )
                description = f"Pivot {values_str} by {on_str}"

            self._add_polars_code(new_node_id, code, description)

        return self._create_child_frame(new_node_id)

    def unpivot(
        self,
        on: list[str | Selector] | str | None | Selector = None,
        *,
        index: list[str] | str | None = None,
        variable_name: str = "variable",
        value_name: str = "value",
        description: str = None,
    ) -> FlowFrame:
        """
        Unpivot a DataFrame from wide to long format.

        Parameters
        ----------
        on : list[str | Selector] | str | None | Selector
            Column(s) to unpivot (become values in the value column)
            If None, all columns not in index will be used
        index : list[str] | str | None
            Column(s) to use as identifier variables (stay as columns)
        variable_name : str, optional
            Name to give to the variable column, by default "variable"
        value_name : str, optional
            Name to give to the value column, by default "value"
        description : str, optional
            Description of this operation for the ETL graph

        Returns
        -------
        FlowFrame
            A new FlowFrame with unpivoted data
        """
        refuse_parameter_as_column((on, index, variable_name, value_name), "unpivot")
        new_node_id = generate_node_id()

        if index is None:
            index_columns = []
        elif isinstance(index, str):
            index_columns = [index]
        else:
            index_columns = list(index)
        can_use_native = True
        if on is None:
            value_columns = []
        elif isinstance(on, str | Selector):
            if isinstance(on, Selector):
                can_use_native = False
            value_columns = [on]
        elif isinstance(on, Iterable):
            value_columns = list(on)
            if isinstance(value_columns[0], Iterable):
                can_use_native = False
        else:
            value_columns = [on]

        if can_use_native:
            can_use_native = variable_name == "variable" and value_name == "value"
        if can_use_native:
            unpivot_input = transform_schema.UnpivotInput(
                index_columns=index_columns,
                value_columns=value_columns,
                data_type_selector=None,
                data_type_selector_mode="column",
            )

            unpivot_settings = input_schema.NodeUnpivot(
                flow_id=self.flow_graph.flow_id,
                node_id=new_node_id,
                unpivot_input=unpivot_input,
                pos_x=200,
                pos_y=150,
                is_setup=True,
                depending_on_id=self.node_id,
                description=description or "Unpivot data from wide to long format",
            )

            self.flow_graph.add_unpivot(unpivot_settings)
        else:
            on_repr = repr(on)
            index_repr = repr(index)

            code = f"""
    # Perform unpivot operation
    output_df = input_df.unpivot(
        on={on_repr},
        index={index_repr},
        variable_name="{variable_name}",
        value_name="{value_name}"
    )
    output_df
    """
            if description is None:
                index_str = ", ".join(index_columns) if index_columns else "none"
                value_str = ", ".join(value_columns) if value_columns else "all non-index columns"
                description = f"Unpivot data with index: {index_str} and value cols: {value_str}"

            self._add_polars_code(new_node_id, code, description)

        return self._create_child_frame(new_node_id)

    def concat(
        self,
        other: FlowFrame | list[FlowFrame],
        how: str = "vertical",
        rechunk: bool = False,
        parallel: bool = True,
        description: str = None,
    ) -> FlowFrame:
        """
        Combine multiple FlowFrames into a single FlowFrame.

        This is equivalent to Polars' concat operation with various joining strategies.

        Parameters
        ----------
        other : FlowFrame or List[FlowFrame]
            One or more FlowFrames to concatenate with this one
        how : str, default 'vertical'
            How to combine the FlowFrames:
            - 'vertical': Stack frames on top of each other (equivalent to 'union all')
            - 'vertical_relaxed': Same as vertical but coerces columns to common supertypes
            - 'diagonal': Union of column schemas, filling missing values with null
            - 'diagonal_relaxed': Same as diagonal but coerces columns to common supertypes
            - 'horizontal': Stack horizontally (column-wise concat)
            - 'align', 'align_full', 'align_left', 'align_right': Auto-determine key columns
        rechunk : bool, default False
            Whether to ensure contiguous memory in result
        parallel : bool, default True
            Whether to use parallel processing for the operation
        description : str, optional
            Description of this operation for the ETL graph

        Returns
        -------
        FlowFrame
            A new FlowFrame with the concatenated data
        """
        if isinstance(other, FlowFrame):
            others = [other]
        else:
            others = other

        # Ensure all frames are in the same graph (combine all at once, not pairwise)
        all_frames = [self] + others
        merge_frames(all_frames)

        new_node_id = generate_node_id()

        # Build a stable, deduplicated view of upstream sources. The graph wires
        # each (source_node, input_slot) pair at most once (add_connection is
        # idempotent), so when the same FlowFrame appears in `all_frames` more
        # than once we must reuse the same input variable rather than fan out
        # to phantom input_df_N slots that will not be bound at execute-time.
        unique_node_ids: list[int] = []
        position_by_node_id: dict[int, int] = {}
        handle_by_node_id: dict[int, str] = {}
        for f in all_frames:
            if f.node_id not in position_by_node_id:
                position_by_node_id[f.node_id] = len(unique_node_ids)
                unique_node_ids.append(f.node_id)
                handle_by_node_id[f.node_id] = f.output_handle
            elif handle_by_node_id[f.node_id] != f.output_handle:
                raise NativeNodeError(
                    f"concat cannot read both {handle_by_node_id[f.node_id]} and {f.output_handle} of node "
                    f"{f.node_id}: one node reads a single output handle per source."
                )
        has_duplicate_sources = len(unique_node_ids) < len(all_frames)

        # NodeUnion can't express "include this input twice"; fall back to the
        # polars-code path (which can, positionally) when duplicates are present.
        use_native = how == "diagonal_relaxed" and parallel and not rechunk and not has_duplicate_sources
        if use_native:
            union_input = transform_schema.UnionInput(
                mode="relaxed"  # This maps to diagonal_relaxed in polars
            )

            union_settings = input_schema.NodeUnion(
                flow_id=self.flow_graph.flow_id,
                node_id=new_node_id,
                union_input=union_input,
                pos_x=200,
                pos_y=150,
                is_setup=True,
                depending_on_ids=list(unique_node_ids),
                description=description or "Concatenate dataframes",
            )

            self.flow_graph.add_union(union_settings)

            # Wire each unique upstream source exactly once.
            seen_sources: set[int] = set()
            for f in all_frames:
                if f.node_id in seen_sources:
                    continue
                seen_sources.add(f.node_id)
                f._add_connection(f.node_id, new_node_id, "main", output_handle=f.output_handle)
        else:
            # Match execute_polars_code's binding convention: a single unique
            # source binds as `input_df` (singular); two or more bind as
            # `input_df_1`, `input_df_2`, ... in upstream-discovery order.
            if len(unique_node_ids) == 1:
                input_vars = ["input_df"] * len(all_frames)
            else:
                input_vars = [f"input_df_{position_by_node_id[f.node_id] + 1}" for f in all_frames]

            frames_list = f"[{', '.join(input_vars)}]"
            code = f"""
            # Perform concat operation
            output_df = pl.concat(
                {frames_list},
                how='{how}',
                rechunk={rechunk},
                parallel={parallel}
            )
            """

            self._add_polars_code(new_node_id, code, description, depending_on_ids=list(unique_node_ids))
            # Wire each unique upstream source exactly once.
            seen_sources: set[int] = set()
            for f in all_frames:
                if f.node_id in seen_sources:
                    continue
                seen_sources.add(f.node_id)
                f._add_connection(f.node_id, new_node_id, "main", output_handle=f.output_handle)
        return FlowFrame(
            data=materialise(self.flow_graph.get_node(new_node_id)).data_frame,
            flow_graph=self.flow_graph,
            node_id=new_node_id,
            parent_node_id=self.node_id,
            deferred=any(f._deferred for f in all_frames),
        )

    def _detect_cum_count_record_id(
        self, expr: Any, new_node_id: int, description: str | None = None
    ) -> tuple[bool, FlowFrame | None]:
        """
        Detect if the expression is a cum_count operation and use record_id if possible.

        Parameters
        ----------
        expr : Any
            Expression to analyze
        new_node_id : int
            Node ID to use if creating a record_id node
        description : str, optional
            Description to use for the new node

        Returns
        -------
        Tuple[bool, Optional[FlowFrame]]
            A tuple containing:
            - bool: Whether a cum_count expression was detected and optimized
            - Optional[FlowFrame]: The new FlowFrame if detection was successful, otherwise None
        """
        if (
            not isinstance(expr, Expr)
            or not expr._repr_str
            or "cum_count" not in expr._repr_str
            or not hasattr(expr, "name")
        ):
            return False, None

        output_name = expr.column_name

        if ".over(" not in expr._repr_str:
            # Simple cumulative count can be implemented as a record ID with offset=1
            record_id_input = transform_schema.RecordIdInput(
                output_column_name=output_name,
                offset=1,
                group_by=False,
                group_by_columns=[],
            )

            record_id_settings = input_schema.NodeRecordId(
                flow_id=self.flow_graph.flow_id,
                node_id=new_node_id,
                record_id_input=record_id_input,
                pos_x=200,
                pos_y=150,
                is_setup=True,
                depending_on_id=self.node_id,
                description=description or f"Add cumulative count as '{output_name}'",
            )

            self.flow_graph.add_record_id(record_id_settings)
            return True, self._create_child_frame(new_node_id)

        elif ".over(" in expr._repr_str:
            partition_columns = []

            # Case 1: Simple string column - .over('column')
            simple_match = re.search(r'\.over\([\'"]([^\'"]+)[\'"]\)', expr._repr_str)
            if simple_match:
                partition_columns = [simple_match.group(1)]

            # Case 2: List of column strings - .over(['col1', 'col2'])
            list_match = re.search(r"\.over\(\[(.*?)\]", expr._repr_str)
            if list_match:
                items = list_match.group(1).split(",")
                for item in items:
                    # Extract string column names from quoted strings
                    col_match = re.search(r'[\'"]([^\'"]+)[\'"]', item.strip())
                    if col_match:
                        partition_columns.append(col_match.group(1))

            # Case 3: pl.col expressions - .over(pl.col('category'), pl.col('abc'))
            col_matches = re.finditer(r'pl\.col\([\'"]([^\'"]+)[\'"]\)', expr._repr_str)
            for match in col_matches:
                partition_columns.append(match.group(1))

            # If we found partition columns, create a grouped record ID
            if partition_columns:
                record_id_input = transform_schema.RecordIdInput(
                    output_column_name=output_name,
                    offset=1,
                    group_by=True,
                    group_by_columns=partition_columns,
                )

                record_id_settings = input_schema.NodeRecordId(
                    flow_id=self.flow_graph.flow_id,
                    node_id=new_node_id,
                    record_id_input=record_id_input,
                    pos_x=200,
                    pos_y=150,
                    is_setup=True,
                    depending_on_id=self.node_id,
                    description=description
                    or f"Add grouped cumulative count as '{output_name}' by {', '.join(partition_columns)}",
                )

                self.flow_graph.add_record_id(record_id_settings)
                return True, self._create_child_frame(new_node_id)

        return False, None

    def with_columns(
        self,
        *exprs: Expr | Iterable[Expr] | Any,  # Allow Any for implicit lit conversion
        flowfile_formulas: list[str] | None = None,
        output_column_names: list[str] | None = None,
        output_column_datatypes: list[str] | None = None,
        description: str | None = None,
        **named_exprs: Expr | Any,  # Allow Any for implicit lit conversion
    ) -> FlowFrame:
        """
        Add or replace columns in the DataFrame.

        Node emission follows Polars' parallel semantics: expressions read the frame as it was
        before the call, never each other's output. Expressions that all have a flowfile-formula
        form and are independent (no expression reads a column another writes) become ONE
        Formula node with one entry per expression, in argument order. Dependent expressions —
        ``(col("x") + 1).alias("x")`` alongside ``(col("x") * 2).alias("y")`` — cannot be a
        Formula node, whose entries evaluate sequentially, so they emit a Polars-code node
        instead and keep Polars' semantics.

        ``flowfile_formulas=`` is the opposite contract: its formulas evaluate SEQUENTIALLY, so
        a later formula may reference an earlier one's output column. They always become one
        Formula node holding every formula as an entry.
        """
        new_node_id = generate_node_id()

        all_input_expr_objects: list[Expr] = []
        pure_polars_expr_strings_for_wc: list[str] = []
        collected_raw_definitions: list[str] = []
        has_exprs_or_named_exprs = bool(exprs or named_exprs)
        if has_exprs_or_named_exprs:
            actual_exprs_to_process: list[Expr] = []
            temp_exprs_iterable = list(_parse_inputs_as_iterable(exprs))

            for item in temp_exprs_iterable:
                if isinstance(item, Expr):
                    actual_exprs_to_process.append(item)
                else:  # auto-lit for non-Expr positional args
                    actual_exprs_to_process.append(lit(item))

            for name, val_expr in named_exprs.items():
                if isinstance(val_expr, Expr):
                    actual_exprs_to_process.append(val_expr.alias(name))  # type: ignore # Assuming Expr has alias
                else:  # auto-lit for named args and then alias
                    actual_exprs_to_process.append(lit(val_expr).alias(name))  # type: ignore

            if len(actual_exprs_to_process) == 1 and isinstance(actual_exprs_to_process[0], Expr):
                pass

            # Try flowfile formula conversion (all-or-nothing)
            params = self._param_values()
            if all(
                isinstance(e, Expr)
                and e._ff_repr is not None
                and e.column_name is not None
                and _formula_parses(e._ff_repr, params)
                for e in actual_exprs_to_process
            ):
                formula_entries = [(e.column_name, e._ff_repr) for e in actual_exprs_to_process]
                if len(formula_entries) == 1:
                    return self._with_flowfile_formula(formula_entries[0][1], formula_entries[0][0], description)
                # A Formula node evaluates sequentially; only independent expressions mean the
                # same thing there as they do in a single parallel Polars with_columns call.
                if entries_are_independent(formula_entries):
                    return self._with_flowfile_formulas(
                        [(name, formula, "Auto") for name, formula in formula_entries], description
                    )

            window_frame = self._try_native_window_functions(actual_exprs_to_process, new_node_id, description)
            if window_frame is not None:
                return window_frame

            for current_expr_obj in actual_exprs_to_process:
                all_input_expr_objects.append(current_expr_obj)
                pure_expr_str, raw_defs_str = _extract_expr_parts(current_expr_obj)
                pure_polars_expr_strings_for_wc.append(pure_expr_str)  # with_columns takes individual expressions
                if raw_defs_str and raw_defs_str not in collected_raw_definitions:
                    collected_raw_definitions.append(raw_defs_str)

            polars_operation_code = f"input_df.with_columns([{', '.join(pure_polars_expr_strings_for_wc)}])"

            final_code_for_node: str
            if collected_raw_definitions:
                unique_raw_definitions = list(dict.fromkeys(collected_raw_definitions))
                definitions_section = "\n\n".join(unique_raw_definitions)
                final_code_for_node = (
                    definitions_section + "\n#─────SPLIT─────\n\n" + f"output_df = {polars_operation_code}"
                )
            else:
                final_code_for_node = polars_operation_code

            pl_expressions_for_fallback = [
                e.expr
                for e in all_input_expr_objects
                if isinstance(e, Expr) and hasattr(e, "expr") and e.expr is not None
            ]
            precomputed = self._add_polars_code(
                new_node_id,
                final_code_for_node,
                description,
                method_name="with_columns",
                convertable_to_code=_check_if_convertible_to_code(all_input_expr_objects),
                polars_expr=pl_expressions_for_fallback,
            )
            return self._create_child_frame(new_node_id, precomputed_result=precomputed)

        elif flowfile_formulas is not None and output_column_names is not None:
            refuse_parameter_as_column(output_column_names, "with_columns(output_column_names=)")
            for formula in flowfile_formulas:
                refuse_parameter_column_in_formula(formula, "with_columns(flowfile_formulas=)")
            if len(output_column_names) != len(flowfile_formulas):
                raise ValueError("Length of both the formulas and the output columns names must be identical")
            if output_column_datatypes is not None and len(output_column_datatypes) != len(flowfile_formulas):
                raise ValueError("Length of output_column_datatypes must match the number of formulas")

            # When the user did not request explicit output datatypes, try to
            # upgrade flowfile formulas to native polars/flowframe expressions
            # for a more efficient node type. Falls back transparently when
            # the upstream translator can't handle a given formula.
            # Only independent formulas may take the translated route: it re-enters
            # with_columns, whose expression path assumes parallel semantics.
            independent = entries_are_independent(
                list(zip(output_column_names, flowfile_formulas, strict=False))
            )
            if output_column_datatypes is None and independent:
                translated = _try_translate_flowfile_formulas(flowfile_formulas, output_column_names)
                if translated is not None:
                    return self.with_columns(*translated, description=description)

            datatypes = output_column_datatypes or ["Auto"] * len(flowfile_formulas)
            if len(flowfile_formulas) == 1:
                return self._with_flowfile_formula(
                    flowfile_formulas[0], output_column_names[0], description, output_column_datatype=datatypes[0]
                )
            return self._with_flowfile_formulas(
                list(zip(output_column_names, flowfile_formulas, datatypes, strict=False)), description
            )
        else:
            raise ValueError("Either exprs/named_exprs or flowfile_formulas with output_column_names must be provided")

    def _try_native_window_functions(
        self, exprs: list[Expr], new_node_id: int, description: str | None
    ) -> FlowFrame | None:
        """Emits one Window Functions node when every expression is a partition
        aggregate (``col(x).<agg>().over(g).alias(name)``) and all share the same
        partition columns. Returns ``None`` when the call does not fit, so the
        caller falls back to a Polars-code node.
        """
        specs = [getattr(e, "_window_spec", None) for e in exprs]
        if not specs or any(spec is None for spec in specs):
            return None
        partition_by = specs[0]["partition_by"]
        if any(spec["partition_by"] != partition_by for spec in specs):
            return None
        window_functions = []
        for expr_obj, spec in zip(exprs, specs, strict=True):
            if expr_obj.column_name is None or expr_obj.column_name == spec["column"]:
                return None
            window_functions.append(
                transform_schema.WindowFunctionInput(
                    column=spec["column"], function=spec["function"], new_column_name=expr_obj.column_name
                )
            )
        settings = input_schema.NodeWindowFunctions(
            flow_id=self.flow_graph.flow_id,
            node_id=new_node_id,
            depending_on_id=self.node_id,
            window_input=transform_schema.WindowFunctionsInput(
                partition_by=partition_by, window_functions=window_functions
            ),
            description=description,
        )
        self.flow_graph.add_window_functions(settings)
        return self._create_child_frame(new_node_id)

    def with_row_index(self, name: str = "index", offset: int = 0, description: str = None) -> FlowFrame:
        """
        Add a row index as the first column in the DataFrame.

        Parameters
        ----------
        name : str, default "index"
            Name of the index column.
        offset : int, default 0
            Start the index at this offset. Cannot be negative.
        description : str, optional
            Description of this operation for the ETL graph

        Returns
        -------
        FlowFrame
            A new FlowFrame with the row index column added
        """
        refuse_parameter_as_column(name, "with_row_index(name=)")
        new_node_id = generate_node_id()

        if name == "record_id" or (offset == 1 and name != "index"):
            record_id_input = transform_schema.RecordIdInput(
                output_column_name=name,
                offset=offset,
                group_by=False,
                group_by_columns=[],
            )

            record_id_settings = input_schema.NodeRecordId(
                flow_id=self.flow_graph.flow_id,
                node_id=new_node_id,
                record_id_input=record_id_input,
                pos_x=200,
                pos_y=150,
                is_setup=True,
                depending_on_id=self.node_id,
                description=description or f"Add row index column '{name}'",
            )

            self.flow_graph.add_record_id(record_id_settings)
        else:
            code = f"input_df.with_row_index(name='{name}', offset={offset})"
            self._add_polars_code(new_node_id, code, description or f"Add row index column '{name}'")

        return self._create_child_frame(new_node_id)

    def explode(
        self,
        columns: str | Column | Iterable[str | Column],
        *more_columns: str | Column,
        description: str = None,
    ) -> FlowFrame:
        """
        Explode the dataframe to long format by exploding the given columns.

        The underlying columns being exploded must be of the List or Array data type.

        Parameters
        ----------
        columns : str, Column, or Sequence[str, Column]
            Column names, expressions, or a sequence of them to explode
        *more_columns : str or Column
            Additional columns to explode, specified as positional arguments
        description : str, optional
            Description of this operation for the ETL graph

        Returns
        -------
        FlowFrame
            A new FlowFrame with exploded rows
        """
        new_node_id = generate_node_id()

        all_columns = []

        if isinstance(columns, list | tuple):
            all_columns.extend([col.column_name if isinstance(col, Column) else col for col in columns])
        else:
            all_columns.append(columns.column_name if isinstance(columns, Column) else columns)

        if more_columns:
            for col in more_columns:
                all_columns.append(col.column_name if isinstance(col, Column) else col)

        if len(all_columns) == 1:
            columns_str = stringify_values(all_columns[0])
        else:
            columns_str = "[" + ", ".join([stringify_values(col) for col in all_columns]) + "]"

        code = f"""
        # Explode columns into multiple rows
        output_df = input_df.explode({columns_str})
        """

        cols_desc = ", ".join(str(s) for s in all_columns)
        desc = description or f"Explode column(s): {cols_desc}"

        self._add_polars_code(new_node_id, code, desc)

        return self._create_child_frame(new_node_id)

    def fuzzy_join(
        self,
        other: FlowFrame,
        fuzzy_mappings: list[FuzzyMapping],
        description: str = None,
    ) -> FlowFrame:
        self._ensure_same_graph(other)

        new_node_id = generate_node_id()
        node_fuzzy_match = input_schema.NodeFuzzyMatch(
            flow_id=self.flow_graph.flow_id,
            node_id=new_node_id,
            join_input=transform_schema.FuzzyMatchInput(
                join_mapping=fuzzy_mappings, left_select=self.columns, right_select=other.columns
            ),
            description=description or "Fuzzy match between two FlowFrames",
            depending_on_ids=[self.node_id, other.node_id],
        )
        self.flow_graph.add_fuzzy_match(node_fuzzy_match)
        self._add_connection(self.node_id, new_node_id, "main", output_handle=self.output_handle)
        other._add_connection(other.node_id, new_node_id, "right", output_handle=other.output_handle)
        return FlowFrame(
            data=materialise(self.flow_graph.get_node(new_node_id)).data_frame,
            flow_graph=self.flow_graph,
            node_id=new_node_id,
            parent_node_id=self.node_id,
            deferred=self._deferred or other._deferred,
        )

    def text_to_rows(
        self,
        column: str | Column,
        output_column: str = None,
        delimiter: str = None,
        split_by_column: str = None,
        description: str = None,
    ) -> FlowFrame:
        """
        Split text in a column into multiple rows.

        This is equivalent to the explode operation after string splitting in Polars.

        Parameters
        ----------
        column : str or Column
            Column containing text to split
        output_column : str, optional
            Column name for the split values (defaults to input column name)
        delimiter : str, default ','
            String delimiter to split text on when using a fixed value
        split_by_column : str, optional
            Alternative: column name containing the delimiter for each row
            If provided, this overrides the delimiter parameter
        description : str, optional
            Description of this operation for the ETL graph

        Returns
        -------
        FlowFrame
            A new FlowFrame with text split into multiple rows
        """
        refuse_parameter_as_column((column, output_column, split_by_column), "text_to_rows")
        new_node_id = generate_node_id()

        if isinstance(column, Column):
            column_name = column.column_name
        else:
            column_name = column

        output_column = output_column or column_name

        text_to_rows_input = transform_schema.TextToRowsInput(
            column_to_split=column_name,
            output_column_name=output_column,
            split_by_fixed_value=split_by_column is None,
            split_fixed_value=delimiter,
            split_by_column=split_by_column,
        )

        text_to_rows_settings = input_schema.NodeTextToRows(
            flow_id=self.flow_graph.flow_id,
            node_id=new_node_id,
            text_to_rows_input=text_to_rows_input,
            pos_x=200,
            pos_y=150,
            is_setup=True,
            depending_on_id=self.node_id,
            description=description or f"Split text in '{column_name}' to rows",
        )

        self.flow_graph.add_text_to_rows(text_to_rows_settings)

        return self._create_child_frame(new_node_id)

    def unique(
        self,
        subset: str | Expr | list[str | Expr] = None,
        *,
        keep: Literal["first", "last", "any", "none"] = "any",
        maintain_order: bool = False,
        description: str = None,
    ) -> FlowFrame:
        """
        Drop duplicate rows from this dataframe.

        Parameters
        ----------
        subset : str, Expr, list of str or Expr, optional
            Column name(s) or selector(s), to consider when identifying duplicate rows.
            If set to None (default), use all columns.
        keep : {'first', 'last', 'any', 'none'}, default 'any'
            Which of the duplicate rows to keep.
            * 'any': Does not give any guarantee of which row is kept.
              This allows more optimizations.
            * 'none': Don't keep duplicate rows.
            * 'first': Keep first unique row.
            * 'last': Keep last unique row.
        maintain_order : bool, default False
            Keep the same order as the original DataFrame. This is more expensive
            to compute. Settings this to True blocks the possibility to run on
            the streaming engine.
        description : str, optional
            Description of this operation for the ETL graph.

        Returns
        -------
        FlowFrame
            DataFrame with unique rows.
        """
        refuse_parameter_as_column(subset, "unique(subset=)")
        new_node_id = generate_node_id()
        processed_subset = None
        can_use_native = True
        if subset is not None:
            if not isinstance(subset, list | tuple):
                subset = [subset]

            processed_subset = []
            for col_expr in subset:
                if isinstance(col_expr, str):
                    processed_subset.append(col_expr)
                elif isinstance(col_expr, Column):
                    if col_expr._select_input.is_altered:
                        can_use_native = False
                        break
                    processed_subset.append(col_expr.column_name)
                else:
                    can_use_native = False
                    break

        can_use_native = can_use_native and keep in ["any", "first", "last", "none"] and not maintain_order

        if can_use_native:
            if not processed_subset:  # Ensure the subset is selecting all columns
                processed_subset = self.columns
            unique_input = transform_schema.UniqueInput(columns=processed_subset, strategy=keep)

            unique_settings = input_schema.NodeUnique(
                flow_id=self.flow_graph.flow_id,
                node_id=new_node_id,
                unique_input=unique_input,
                pos_x=200,
                pos_y=150,
                is_setup=True,
                depending_on_id=self.node_id,
                description=description or f"Get unique rows (strategy: {keep})",
            )

            self.flow_graph.add_unique(unique_settings)
        else:
            if subset is None:
                subset_str = "None"
            elif isinstance(subset, list | tuple):
                items = []
                for item in subset:
                    if isinstance(item, str):
                        items.append(f'"{item}"')
                    else:
                        items.append(str(item))
                subset_str = f"[{', '.join(items)}]"
            else:
                subset_str = str(subset)

            code = f"""
            # Remove duplicate rows
            output_df = input_df.unique(
                subset={subset_str},
                keep='{keep}',
                maintain_order={maintain_order}
            )
            """

            subset_desc = "all columns" if subset is None else f"columns: {subset_str}"
            desc = description or f"Get unique rows using {subset_desc}, keeping {keep}"

            self._add_polars_code(new_node_id, code, desc)

        return self._create_child_frame(new_node_id)

    def data_cleansing(
        self,
        columns: list[str] | None = None,
        *,
        remove_null_rows: bool = False,
        remove_null_columns: bool = False,
        replace_nulls_with_blank: bool = True,
        replace_nulls_with_zero: bool = True,
        trim_whitespace: bool = True,
        normalize_whitespace: bool = False,
        remove_all_whitespace: bool = False,
        remove_letters: bool = False,
        remove_numbers: bool = False,
        remove_punctuation: bool = False,
        case_mode: Literal["none", "uppercase", "lowercase", "titlecase"] = "none",
        description: str | None = None,
    ) -> FlowFrame:
        """
        Fix common data quality issues in one node.

        Bundles the usual "clean this up before I can work with it" rules: dropping
        entirely-null rows and columns, replacing nulls with a neutral value, stripping
        unwanted characters and whitespace, and normalising casing.

        Two rules are frame-wide and deliberately ignore *columns*: ``remove_null_rows``
        drops a row only when *every* column in it is null, and ``remove_null_columns``
        drops a column only when it is null in *every* row. An empty string is not a
        null, so it keeps its row/column alive.

        Every other rule applies only to the selected columns, and only to columns of
        the matching data-type group: the null-to-blank and character/case rules touch
        String columns, ``replace_nulls_with_zero`` touches Numeric columns, and every
        other dtype passes through untouched even when selected. Selected columns that
        do not exist are silently ignored.

        Within one string column the rules run in a fixed order: fill nulls, remove
        letters, then numbers, then punctuation, then clean up whitespace (so gaps left
        by character removal are collapsed too), then apply the casing rule. Titlecasing
        follows polars, which capitalises the letter after any non-letter — so
        ``"3rd street"`` becomes ``"3Rd Street"``.

        Unlike every other keyword, ``remove_null_columns=True`` is resolved eagerly:
        the drop list comes from a bounded null-count aggregation (worker-first) taken
        when this method is called, not when the frame is collected.

        Parameters
        ----------
        columns : list of str, optional
            Columns to cleanse. ``None`` (default) cleanses every column.
        remove_null_rows : bool, default False
            Drop rows where every column is null.
        remove_null_columns : bool, default False
            Drop columns that are null in every row. Data-dependent: the column is
            decided when the node runs, not when the graph is built.
        replace_nulls_with_blank : bool, default True
            Replace nulls with ``""`` in selected String columns.
        replace_nulls_with_zero : bool, default True
            Replace nulls with ``0`` in selected Numeric columns.
        trim_whitespace : bool, default True
            Strip leading and trailing whitespace from selected String columns.
        normalize_whitespace : bool, default False
            Collapse every run of whitespace (tabs and line breaks included) into a
            single space.
        remove_all_whitespace : bool, default False
            Remove whitespace entirely. Supersedes *trim_whitespace* and
            *normalize_whitespace*.
        remove_letters : bool, default False
            Remove letters, Unicode included (``À``, ``é`` and ``ö`` all go).
        remove_numbers : bool, default False
            Remove digits.
        remove_punctuation : bool, default False
            Remove ASCII punctuation (Python's ``string.punctuation`` set).
        case_mode : {'none', 'uppercase', 'lowercase', 'titlecase'}, default 'none'
            Casing applied to selected String columns after every other rule.
        description : str, optional
            Description of this operation for the ETL graph. Defaults to a summary of
            the enabled rules.

        Returns
        -------
        FlowFrame
            A new FlowFrame with the cleansing rules applied. Dtypes, column order and
            column names are unchanged; only ``remove_null_columns`` removes columns.
        """
        if isinstance(columns, str):
            columns = [columns]

        new_node_id = generate_node_id()

        cleansing_input = transform_schema.DataCleansingInput(
            remove_null_rows=remove_null_rows,
            remove_null_columns=remove_null_columns,
            selection_mode="all" if columns is None else "list",
            selected_columns=list(columns or []),
            replace_nulls_with_blank=replace_nulls_with_blank,
            replace_nulls_with_zero=replace_nulls_with_zero,
            trim_whitespace=trim_whitespace,
            normalize_whitespace=normalize_whitespace,
            remove_all_whitespace=remove_all_whitespace,
            remove_letters=remove_letters,
            remove_numbers=remove_numbers,
            remove_punctuation=remove_punctuation,
            case_mode=case_mode,
        )

        cleansing_settings = input_schema.NodeDataCleansing(
            flow_id=self.flow_graph.flow_id,
            node_id=new_node_id,
            cleansing_input=cleansing_input,
            pos_x=200,
            pos_y=150,
            is_setup=True,
            depending_on_id=self.node_id,
            description=description,
        )

        self.flow_graph.add_data_cleansing(cleansing_settings)

        return self._create_child_frame(new_node_id)

    @property
    def columns(self) -> list[str]:
        """Get the column names."""
        return self.data.collect_schema().names()

    @property
    def dtypes(self) -> list[pl.DataType]:
        """Get the column data types."""
        return self.data.dtypes

    @property
    def schema(self) -> pl.schema.Schema:
        """Get an ordered mapping of column names to their data type."""
        return self.data.schema

    @property
    def width(self) -> int:
        """Get the number of columns."""
        return self.data.width

    def __contains__(self, key):
        """This special method enables the 'in' operator to work with FlowFrame objects."""
        return key in self.data

    def __bool__(self):
        """This special method determines how the object behaves in boolean contexts.
        Returns True if the FlowFrame contains any data, False otherwise."""
        return bool(self.data)

    @staticmethod
    def _comparison_error(operator: str) -> pl.lazyframe.frame.NoReturn:
        msg = f'"{operator!r}" comparison not supported for LazyFrame objects'
        raise TypeError(msg)

    def __eq__(self, other: object) -> pl.lazyframe.frame.NoReturn:
        self._comparison_error("==")

    def __ne__(self, other: object) -> pl.lazyframe.frame.NoReturn:
        self._comparison_error("!=")

    def __gt__(self, other: Any) -> pl.lazyframe.frame.NoReturn:
        self._comparison_error(">")

    def __lt__(self, other: Any) -> pl.lazyframe.frame.NoReturn:
        self._comparison_error("<")

    def __ge__(self, other: Any) -> pl.lazyframe.frame.NoReturn:
        self._comparison_error(">=")

    def __le__(self, other: Any) -> pl.lazyframe.frame.NoReturn:
        self._comparison_error("<=")
