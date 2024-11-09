import polars as pl
from polars_expr_transformer import simple_function_to_expr
from flowfile_core.configs import logger
from dataclasses import dataclass
from typing import Optional


@dataclass
class RealTimeResult:
    result_df: pl.DataFrame
    data_type: pl.DataType
    readable_result: str
    success: Optional[bool] = None

    def __init__(self, result_value: pl.DataFrame, data_type: pl.DataType):
        self.result_df = result_value
        self.data_type = data_type
        if len(result_value)>0:
            self.readable_result = str(result_value.item(0,0))
            self.success = True
        else:
            self.readable_result = ''
            self.success = None

    def is_filterable_result(self):
        """
        This function is used to check if the result of the function can be used as a filter
        """
        if self.data_type == pl.Boolean:
            return True
        else:
            try:
                self.result_df.select(pl.col(self.result_df.columns[0]).cast(pl.Boolean))
                return True
            except:
                return False


def get_realtime_func_results(df: pl.DataFrame | pl.LazyFrame, func_string: str, sample: int = 1) -> RealTimeResult:
    """
    This function is used to get the first result of a function applied to a dataframe. This is useful for debugging the users write
    example:
    df = pl.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6], 'c': [1, 2, 3], 'names': ['ham', 'spam', 'eggs']})
    print(get_first_result_of_function('year(today())', df))
    """
    if isinstance(df, pl.LazyFrame):
        logger.warning('Performance in this case can be '
                       'improved by using polars.DataFrame to ensure it returns instantly')
        df = df.head(sample).collect()
    result = df.head(1).select(simple_function_to_expr(func_string))
    return RealTimeResult(result, result.dtypes[0])
