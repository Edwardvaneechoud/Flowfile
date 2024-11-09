import polars as pl
from polars import Expr
from schemas.transform_schema import PivotInput
from flowfile.flowfile_table.flowFilePolars import FlowFileTable


data = {
    "ix": [1, 2, 2, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5, 5, 6, 6, 6, 6, 6, 6, 7, 7, 7, 7, 7, 7, 7],
    "groups": ["A", "B", "A", "B", "A", "B", "A", "B", "A", "B", "A", "B", "A", "B", "A", "B", "A", "B", "A", "B", "A", "B", "A", "B", "A", "B", "A", "B"],
    "col1": [10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120, 130, 140, 150, 160, 170, 180, 190, 200, 210, 220, 230, 240, 250, 260, 270, 280],
    "col2": [5, 15, 25, 35, 45, 55, 65, 75, 85, 95, 105, 115, 125, 135, 145, 155, 165, 175, 185, 195, 205, 215, 225, 235, 245, 255, 265, 275]
}

# Create Polars DataFrame

ff = FlowFileTable(pl.DataFrame(data))
ff.lazy = True

pivot_input = PivotInput(index_columns=['ix'], pivot_column='groups', value_col='col1', aggregations=['sum', 'mean'])

r = ff.do_pivot(pivot_input)



def count_non_nulls(col: Expr) -> pl.Expr:
    return col.null_count()


def count_non_null() -> pl.Expr:
    return pl.element().is_not_null().sum()


