from dataclasses import dataclass
from typing import List

import polars as pl
from polars.expr import Expr


@dataclass
class AggFunc:
    __slots__ = ['func_name', 'func_expr']
    func_name: str
    func_expr: Expr


AggFuncs = List[AggFunc]

pl.Expr.sum

agg_funcs = ['sum', 'max', 'min', 'count', 'first', 'last', 'std', 'var', 'n_unique', 'list', 'list_agg']


