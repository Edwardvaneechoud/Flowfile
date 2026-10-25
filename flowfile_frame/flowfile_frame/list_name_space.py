from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING, Any

import polars as pl

from flowfile_frame.config import logger

# --- TYPE CHECKING IMPORTS ---
if TYPE_CHECKING:
    from datetime import date, datetime, time

    from polars._typing import IntoExprColumn, ListToStructWidthStrategy, NullBehavior

    from flowfile_frame.expr import Expr


def _polars(value: Any) -> Any:
    """``value`` with its frame expressions replaced by Polars expressions (``expr._polars_argument``)."""
    from flowfile_frame.expr import _polars_argument

    return _polars_argument(value)


class ExprListNameSpace:
    """Namespace for list related expressions."""

    def __init__(self, parent_expr: Expr, parent_repr_str: str):
        self.parent = parent_expr
        self.expr = parent_expr.expr.list if parent_expr.expr is not None else None
        self.parent_repr_str = parent_repr_str

    def _create_next_expr(
        self, *args, method_name: str, result_expr: pl.Expr | None, is_complex: bool = True, **kwargs
    ) -> Expr:
        """Creates a new Expr instance, appending method call to repr string."""
        from flowfile_frame.expr import _frame_value, _namespace_expr, _repr_args

        args, kwargs = _frame_value(args), _frame_value(kwargs)
        new_repr = f"{self.parent_repr_str}.list.{method_name}({_repr_args(*args, **kwargs)})"
        return _namespace_expr(self.parent, new_repr, (*args, *kwargs.values()), result_expr, is_complex=is_complex)

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
                    expr_fraction = _polars(fraction)
                    res_expr = self.expr.sample(
                        n=None, fraction=expr_fraction, with_replacement=with_replacement, shuffle=shuffle, seed=seed
                    )
                else:
                    expr_n = _polars(1 if n is None else n)
                    res_expr = self.expr.sample(
                        n=expr_n, fraction=None, with_replacement=with_replacement, shuffle=shuffle, seed=seed
                    )
            except Exception as e:
                print(f"Warning: Could not create polars expression for list.sample(): {e}")

        return self._create_next_expr(
            n if n is not None else None,
            method_name="sample",
            result_expr=res_expr,
            fraction=fraction,
            with_replacement=with_replacement,
            shuffle=shuffle,
            seed=seed,
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
        return self._create_next_expr(
            method_name="sort", result_expr=res_expr, descending=descending, nulls_last=nulls_last
        )

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
        from flowfile_frame.expr import Expr, _frame_value

        other = _frame_value(other)
        res_expr = None
        other_expr = None

        if isinstance(other, Expr | str):
            if isinstance(other, Expr):
                other_expr = other.expr
            else:
                other_expr = pl.col(other)
        elif isinstance(other, pl.Series):
            other_expr = pl.lit(other)
        elif isinstance(other, list):
            if len(other) > 0 and isinstance(other[0], Expr | str | pl.Series):
                other_expr = [pl.col(o) if isinstance(o, str) else _polars(o) for o in other]
            else:
                other_expr = pl.lit(other)

        if self.expr is not None and other_expr is not None:
            try:
                if isinstance(other_expr, list):
                    all_exprs = [self.parent.expr] + other_expr
                    res_expr = pl.concat_list(all_exprs)
                else:
                    res_expr = self.expr.concat(other_expr)
            except Exception as e:
                print(f"Warning: Could not create polars expression for list.concat(): {e}")

        return self._create_next_expr(other, method_name="concat", result_expr=res_expr)

    def get(self, index: int | Expr | str, *, null_on_oob: bool = False) -> Expr:
        index_expr = _polars(index)
        res_expr = self.expr.get(index_expr, null_on_oob=null_on_oob) if self.expr is not None else None
        return self._create_next_expr(index, method_name="get", result_expr=res_expr, null_on_oob=null_on_oob)

    def gather(self, indices: Expr | pl.Series | list[int] | list[list[int]], *, null_on_oob: bool = False) -> Expr:
        indices_expr = pl.Series(indices) if isinstance(indices, list) else _polars(indices)

        res_expr = self.expr.gather(indices_expr, null_on_oob=null_on_oob) if self.expr is not None else None
        return self._create_next_expr(indices, method_name="gather", result_expr=res_expr, null_on_oob=null_on_oob)

    def gather_every(self, n: int | IntoExprColumn, offset: int | IntoExprColumn = 0) -> Expr:
        n_expr = _polars(n)
        offset_expr = _polars(offset)

        res_expr = self.expr.gather_every(n_expr, offset_expr) if self.expr is not None else None
        return self._create_next_expr(n, method_name="gather_every", result_expr=res_expr, offset=offset)

    def first(self) -> Expr:
        res_expr = self.expr.first() if self.expr is not None else None
        return self._create_next_expr(method_name="first", result_expr=res_expr)

    def last(self) -> Expr:
        res_expr = self.expr.last() if self.expr is not None else None
        return self._create_next_expr(method_name="last", result_expr=res_expr)

    def contains(self, item: float | str | bool | int | date | datetime | time | IntoExprColumn) -> Expr:
        item_expr = _polars(item)
        res_expr = self.expr.contains(item_expr) if self.expr is not None else None
        return self._create_next_expr(item, method_name="contains", result_expr=res_expr)

    def join(self, separator: IntoExprColumn, *, ignore_nulls: bool = True) -> Expr:
        separator_expr = _polars(separator)
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
        n_expr = _polars(n)
        res_expr = self.expr.shift(n_expr) if self.expr is not None else None
        return self._create_next_expr(n, method_name="shift", result_expr=res_expr)

    def slice(self, offset: int | str | Expr, length: int | str | Expr | None = None) -> Expr:
        offset_expr = _polars(offset)
        length_expr = _polars(length)

        res_expr = self.expr.slice(offset_expr, length_expr) if self.expr is not None else None
        return self._create_next_expr(offset, length, method_name="slice", result_expr=res_expr)

    def head(self, n: int | str | Expr = 5) -> Expr:
        n_expr = _polars(n)
        res_expr = self.expr.head(n_expr) if self.expr is not None else None
        return self._create_next_expr(n, method_name="head", result_expr=res_expr)

    def tail(self, n: int | str | Expr = 5) -> Expr:
        n_expr = _polars(n)
        res_expr = self.expr.tail(n_expr) if self.expr is not None else None
        return self._create_next_expr(n, method_name="tail", result_expr=res_expr)

    def explode(self) -> Expr:
        res_expr = self.expr.explode() if self.expr is not None else None
        return self._create_next_expr(method_name="explode", result_expr=res_expr)

    def count_matches(self, element: Any) -> Expr:
        element_expr = _polars(element)
        res_expr = self.expr.count_matches(element_expr) if self.expr is not None else None
        return self._create_next_expr(element, method_name="count_matches", result_expr=res_expr)

    def to_array(self, width: int) -> Expr:
        res_expr = self.expr.to_array(width) if self.expr is not None else None
        return self._create_next_expr(width, method_name="to_array", result_expr=res_expr)

    def to_struct(
        self,
        n_field_strategy: ListToStructWidthStrategy | None = None,
        fields: Sequence[str] | Callable[[int], str] | None = None,
        upper_bound: int | None = None,
    ) -> Expr:
        given = {
            name: value
            for name, value in (
                ("n_field_strategy", n_field_strategy),
                ("fields", fields),
                ("upper_bound", upper_bound),
            )
            if value is not None
        }
        res_expr = None
        if self.expr is not None:
            try:
                res_expr = self.expr.to_struct(**given)
            except Exception as e:
                logger.warning(f"Could not create polars expression for list.to_struct(): {e}")

        # Only the arguments given are written, as Polars reads the rest; a naming function has no code form.
        result = self._create_next_expr(method_name="to_struct", result_expr=res_expr, **given)
        if callable(fields):
            result.convertable_to_code = False
        return result

    def eval(self, expr: Expr, *, parallel: bool = False) -> Expr:
        expr_inner = _polars(expr)
        res_expr = self.expr.eval(expr_inner, parallel=parallel) if self.expr is not None else None
        return self._create_next_expr(expr, method_name="eval", result_expr=res_expr, parallel=parallel)

    def set_union(self, other: Any) -> Expr:
        other_expr = _polars(other)
        res_expr = self.expr.set_union(other_expr) if self.expr is not None else None
        return self._create_next_expr(other, method_name="set_union", result_expr=res_expr)

    def set_difference(self, other: Any) -> Expr:
        other_expr = _polars(other)
        res_expr = self.expr.set_difference(other_expr) if self.expr is not None else None
        return self._create_next_expr(other, method_name="set_difference", result_expr=res_expr)

    def set_intersection(self, other: Any) -> Expr:
        other_expr = _polars(other)
        res_expr = self.expr.set_intersection(other_expr) if self.expr is not None else None
        return self._create_next_expr(other, method_name="set_intersection", result_expr=res_expr)

    def set_symmetric_difference(self, other: Any) -> Expr:
        other_expr = _polars(other)
        res_expr = self.expr.set_symmetric_difference(other_expr) if self.expr is not None else None
        return self._create_next_expr(other, method_name="set_symmetric_difference", result_expr=res_expr)
