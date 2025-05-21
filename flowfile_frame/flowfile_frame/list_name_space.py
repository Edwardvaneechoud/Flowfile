from __future__ import annotations

from typing import Any, Optional, Union, TYPE_CHECKING, List, TypeVar, Callable, Sequence, Literal

import polars as pl


# --- TYPE CHECKING IMPORTS ---
if TYPE_CHECKING:
    from flowfile_frame.expr import Expr, _get_expr_and_repr, col, lit
    from polars._typing import IntoExprColumn, NullBehavior, ListToStructWidthStrategy
    from datetime import date, datetime, time


class ExprListNameSpace:
    """Namespace for list related expressions."""

    def __init__(self, parent_expr: 'Expr', parent_repr_str: str):
        self.parent = parent_expr
        self.expr = parent_expr.expr.list if parent_expr.expr is not None else None
        self.parent_repr_str = parent_repr_str

    def _create_next_expr(self, *args, method_name: str, result_expr: Optional[pl.Expr], is_complex: bool = True, **kwargs) -> 'Expr':
        from flowfile_frame.expr import Expr
        """Creates a new Expr instance, appending method call to repr string."""
        args_repr = ", ".join(repr(a) for a in args)
        kwargs_repr = ", ".join(f"{k}={repr(v)}" for k, v in kwargs.items())

        if args_repr and kwargs_repr:
            args_str = f"{args_repr}, {kwargs_repr}"
        elif args_repr:
            args_str = args_repr
        elif kwargs_repr:
            args_str = kwargs_repr
        else:
            args_str = ""

        new_repr = f"{self.parent_repr_str}.list.{method_name}({args_str})"

        # Create new instance, inheriting current agg_func status by default
        new_expr_instance = Expr(
            result_expr,
            self.parent.column_name,
            repr_str=new_repr,
            initial_column_name=self.parent._initial_column_name,
            selector=None,
            agg_func=self.parent.agg_func,
            is_complex=is_complex,
            convertable_to_code=self.parent.convertable_to_code
        )
        return new_expr_instance

    def all(self) -> Expr:
        res_expr = self.expr.all() if self.expr is not None else None
        return self._create_next_expr(method_name="all", result_expr=res_expr)

    def any(self) -> Expr:
        res_expr = self.expr.any() if self.expr is not None else None
        return self._create_next_expr(method_name="any", result_expr=res_expr)

    def len(self) -> Expr:
        res_expr = self.expr.len() if self.expr is not None else None
        return self._create_next_expr(method_name="len", result_expr=res_expr)

    def drop_nulls(self) -> Expr:
        res_expr = self.expr.drop_nulls() if self.expr is not None else None
        return self._create_next_expr(method_name="drop_nulls", result_expr=res_expr)

    def sample(
            self,
            n: int | IntoExprColumn | None = None,
            *,
            fraction: float | IntoExprColumn | None = None,
            with_replacement: bool = False,
            shuffle: bool = False,
            seed: int | None = None,
    ) -> Expr:
        if n is not None and fraction is not None:
            raise ValueError("cannot specify both `n` and `fraction`")

        res_expr = None
        if self.expr is not None:
            try:
                if fraction is not None:
                    expr_fraction = fraction.expr if hasattr(fraction, 'expr') else fraction
                    res_expr = self.expr.sample(n=None, fraction=expr_fraction,
                                                with_replacement=with_replacement,
                                                shuffle=shuffle, seed=seed)
                else:
                    expr_n = n.expr if hasattr(n, 'expr') else (1 if n is None else n)
                    res_expr = self.expr.sample(n=expr_n, fraction=None,
                                                with_replacement=with_replacement,
                                                shuffle=shuffle, seed=seed)
            except Exception as e:
                print(f"Warning: Could not create polars expression for list.sample(): {e}")

        return self._create_next_expr(
            n if n is not None else None,
            method_name="sample",
            result_expr=res_expr,
            fraction=fraction,
            with_replacement=with_replacement,
            shuffle=shuffle,
            seed=seed
        )

    def sum(self) -> Expr:
        res_expr = self.expr.sum() if self.expr is not None else None
        return self._create_next_expr(method_name="sum", result_expr=res_expr)

    def max(self) -> Expr:
        res_expr = self.expr.max() if self.expr is not None else None
        return self._create_next_expr(method_name="max", result_expr=res_expr)

    def min(self) -> Expr:
        res_expr = self.expr.min() if self.expr is not None else None
        return self._create_next_expr(method_name="min", result_expr=res_expr)

    def mean(self) -> Expr:
        res_expr = self.expr.mean() if self.expr is not None else None
        return self._create_next_expr(method_name="mean", result_expr=res_expr)

    def median(self) -> Expr:
        res_expr = self.expr.median() if self.expr is not None else None
        return self._create_next_expr(method_name="median", result_expr=res_expr)

    def std(self, ddof: int = 1) -> Expr:
        res_expr = self.expr.std(ddof=ddof) if self.expr is not None else None
        return self._create_next_expr(method_name="std", result_expr=res_expr, ddof=ddof)

    def var(self, ddof: int = 1) -> Expr:
        res_expr = self.expr.var(ddof=ddof) if self.expr is not None else None
        return self._create_next_expr(method_name="var", result_expr=res_expr, ddof=ddof)

    def sort(self, *, descending: bool = False, nulls_last: bool = False) -> Expr:
        res_expr = self.expr.sort(descending=descending, nulls_last=nulls_last) if self.expr is not None else None
        return self._create_next_expr(method_name="sort", result_expr=res_expr, descending=descending, nulls_last=nulls_last)

    def reverse(self) -> Expr:
        res_expr = self.expr.reverse() if self.expr is not None else None
        return self._create_next_expr(method_name="reverse", result_expr=res_expr)

    def unique(self, *, maintain_order: bool = False) -> Expr:
        res_expr = self.expr.unique(maintain_order=maintain_order) if self.expr is not None else None
        return self._create_next_expr(method_name="unique", result_expr=res_expr, maintain_order=maintain_order)

    def n_unique(self) -> Expr:
        res_expr = self.expr.n_unique() if self.expr is not None else None
        return self._create_next_expr(method_name="n_unique", result_expr=res_expr)

    def concat(self, other: list[Expr | str] | Expr | str | pl.Series | list[Any]) -> Expr:
        res_expr = None
        other_expr = None

        # Handle different types of 'other'
        if isinstance(other, (Expr, str)):
            if isinstance(other, Expr):
                other_expr = other.expr
            else:
                other_expr = pl.col(other)
        elif isinstance(other, pl.Series):
            other_expr = pl.lit(other)
        elif isinstance(other, list):
            if len(other) > 0 and isinstance(other[0], (Expr, str, pl.Series)):
                # List of expressions
                other_expr = [o.expr if hasattr(o, 'expr') else (pl.col(o) if isinstance(o, str) else o) for o in other]
            else:
                # List of values
                other_expr = pl.lit(other)

        # Create the polars expression if possible
        if self.expr is not None and other_expr is not None:
            try:
                if isinstance(other_expr, list):
                    # Insert self.expr at the beginning
                    all_exprs = [self.parent.expr] + other_expr
                    res_expr = pl.concat_list(all_exprs)
                else:
                    res_expr = self.expr.concat(other_expr)
            except Exception as e:
                print(f"Warning: Could not create polars expression for list.concat(): {e}")

        return self._create_next_expr(other, method_name="concat", result_expr=res_expr)

    def get(self, index: int | Expr | str, *, null_on_oob: bool = False) -> Expr:
        index_expr = index.expr if hasattr(index, 'expr') else index
        res_expr = self.expr.get(index_expr, null_on_oob=null_on_oob) if self.expr is not None else None
        return self._create_next_expr(index, method_name="get", result_expr=res_expr, null_on_oob=null_on_oob)

    def gather(self, indices: Expr | pl.Series | list[int] | list[list[int]], *, null_on_oob: bool = False) -> Expr:
        indices_expr = indices
        if isinstance(indices, list):
            indices_expr = pl.Series(indices)
        elif hasattr(indices, 'expr'):
            indices_expr = indices.expr

        res_expr = self.expr.gather(indices_expr, null_on_oob=null_on_oob) if self.expr is not None else None
        return self._create_next_expr(indices, method_name="gather", result_expr=res_expr, null_on_oob=null_on_oob)

    def gather_every(self, n: int | IntoExprColumn, offset: int | IntoExprColumn = 0) -> Expr:
        n_expr = n.expr if hasattr(n, 'expr') else n
        offset_expr = offset.expr if hasattr(offset, 'expr') else offset

        res_expr = self.expr.gather_every(n_expr, offset_expr) if self.expr is not None else None
        return self._create_next_expr(n, method_name="gather_every", result_expr=res_expr, offset=offset)

    def first(self) -> Expr:
        res_expr = self.expr.first() if self.expr is not None else None
        return self._create_next_expr(method_name="first", result_expr=res_expr)

    def last(self) -> Expr:
        res_expr = self.expr.last() if self.expr is not None else None
        return self._create_next_expr(method_name="last", result_expr=res_expr)

    def contains(self, item: float | str | bool | int | date | datetime | time | IntoExprColumn) -> Expr:
        item_expr = item.expr if hasattr(item, 'expr') else item
        res_expr = self.expr.contains(item_expr) if self.expr is not None else None
        return self._create_next_expr(item, method_name="contains", result_expr=res_expr)

    def join(self, separator: IntoExprColumn, *, ignore_nulls: bool = True) -> Expr:
        separator_expr = separator.expr if hasattr(separator, 'expr') else separator
        res_expr = self.expr.join(separator_expr, ignore_nulls=ignore_nulls) if self.expr is not None else None
        return self._create_next_expr(separator, method_name="join", result_expr=res_expr, ignore_nulls=ignore_nulls)

    def arg_min(self) -> Expr:
        res_expr = self.expr.arg_min() if self.expr is not None else None
        return self._create_next_expr(method_name="arg_min", result_expr=res_expr)

    def arg_max(self) -> Expr:
        res_expr = self.expr.arg_max() if self.expr is not None else None
        return self._create_next_expr(method_name="arg_max", result_expr=res_expr)

    def diff(self, n: int = 1, null_behavior: NullBehavior = "ignore") -> Expr:
        res_expr = self.expr.diff(n, null_behavior) if self.expr is not None else None
        return self._create_next_expr(method_name="diff", result_expr=res_expr, n=n, null_behavior=null_behavior)

    def shift(self, n: int | IntoExprColumn = 1) -> Expr:
        n_expr = n.expr if hasattr(n, 'expr') else n
        res_expr = self.expr.shift(n_expr) if self.expr is not None else None
        return self._create_next_expr(n, method_name="shift", result_expr=res_expr)

    def slice(self, offset: int | str | Expr, length: int | str | Expr | None = None) -> Expr:
        offset_expr = offset.expr if hasattr(offset, 'expr') else offset
        length_expr = length.expr if hasattr(length, 'expr') and length is not None else length

        res_expr = self.expr.slice(offset_expr, length_expr) if self.expr is not None else None
        return self._create_next_expr(offset, length, method_name="slice", result_expr=res_expr)

    def head(self, n: int | str | Expr = 5) -> Expr:
        n_expr = n.expr if hasattr(n, 'expr') else n
        res_expr = self.expr.head(n_expr) if self.expr is not None else None
        return self._create_next_expr(n, method_name="head", result_expr=res_expr)

    def tail(self, n: int | str | Expr = 5) -> Expr:
        n_expr = n.expr if hasattr(n, 'expr') else n
        res_expr = self.expr.tail(n_expr) if self.expr is not None else None
        return self._create_next_expr(n, method_name="tail", result_expr=res_expr)

    def explode(self) -> Expr:
        res_expr = self.expr.explode() if self.expr is not None else None
        return self._create_next_expr(method_name="explode", result_expr=res_expr)

    def count_matches(self, element: Any) -> Expr:
        element_expr = element.expr if hasattr(element, 'expr') else element
        res_expr = self.expr.count_matches(element_expr) if self.expr is not None else None
        return self._create_next_expr(element, method_name="count_matches", result_expr=res_expr)

    def to_array(self, width: int) -> Expr:
        res_expr = self.expr.to_array(width) if self.expr is not None else None
        return self._create_next_expr(width, method_name="to_array", result_expr=res_expr)

    def to_struct(
            self,
            n_field_strategy: ListToStructWidthStrategy = "first_non_null",
            fields: Sequence[str] | Callable[[int], str] | None = None,
            upper_bound: int = 0,
    ) -> Expr:
        res_expr = None

        if self.expr is not None:
            try:
                if isinstance(fields, Sequence):
                    res_expr = self.expr.to_struct(fields=fields)
                else:
                    res_expr = self.expr.to_struct(
                        n_field_strategy=n_field_strategy,
                        fields=fields,
                        upper_bound=upper_bound
                    )
            except Exception as e:
                print(f"Warning: Could not create polars expression for list.to_struct(): {e}")

        return self._create_next_expr(
            method_name="to_struct",
            result_expr=res_expr,
            n_field_strategy=n_field_strategy,
            fields=fields,
            upper_bound=upper_bound,
        )

    def eval(self, expr: Expr, *, parallel: bool = False) -> Expr:
        expr_inner = expr.expr if hasattr(expr, 'expr') else expr
        res_expr = self.expr.eval(expr_inner, parallel=parallel) if self.expr is not None else None
        return self._create_next_expr(expr, method_name="eval", result_expr=res_expr, parallel=parallel)

    def set_union(self, other: Any) -> Expr:
        other_expr = other.expr if hasattr(other, 'expr') else other
        res_expr = self.expr.set_union(other_expr) if self.expr is not None else None
        return self._create_next_expr(other, method_name="set_union", result_expr=res_expr)

    def set_difference(self, other: Any) -> Expr:
        other_expr = other.expr if hasattr(other, 'expr') else other
        res_expr = self.expr.set_difference(other_expr) if self.expr is not None else None
        return self._create_next_expr(other, method_name="set_difference", result_expr=res_expr)

    def set_intersection(self, other: Any) -> Expr:
        other_expr = other.expr if hasattr(other, 'expr') else other
        res_expr = self.expr.set_intersection(other_expr) if self.expr is not None else None
        return self._create_next_expr(other, method_name="set_intersection", result_expr=res_expr)

    def set_symmetric_difference(self, other: Any) -> Expr:
        other_expr = other.expr if hasattr(other, 'expr') else other
        res_expr = self.expr.set_symmetric_difference(other_expr) if self.expr is not None else None
        return self._create_next_expr(other, method_name="set_symmetric_difference", result_expr=res_expr)