"""Core-only Excel schema helpers; the readers themselves live in ``shared.excel_reader``."""

from flowfile_core.flowfile.flow_data_engine.flow_file_column.main import FlowfileColumn
from flowfile_core.flowfile.flow_data_engine.flow_file_column.utils import dtype_to_pl_str
from flowfile_core.flowfile.flow_data_engine.utils import get_data_type
from shared.excel_reader import df_from_calamine_xlsx, raw_data_openpyxl


def get_calamine_xlsx_data_types(file_path: str, sheet_name: str, start_row: int = 0, end_row: int = 0):
    df = df_from_calamine_xlsx(file_path, sheet_name, start_row, end_row)
    return [
        FlowfileColumn.from_input(n, str(dt), col_index=i)
        for i, (n, dt) in enumerate(zip(df.columns, df.dtypes, strict=False))
    ]


def get_open_xlsx_datatypes(
    file_path: str,
    sheet_name: str = None,
    min_row: int = None,
    max_row: int = None,
    min_col: int = None,
    max_col: int = None,
    has_headers: bool = True,
) -> list[FlowfileColumn]:
    data_iterator = raw_data_openpyxl(
        file_path=file_path, sheet_name=sheet_name, min_row=min_row, max_row=max_row, min_col=min_col, max_col=max_col
    )
    raw_data = data_iterator
    if has_headers:
        columns = (f"_unnamed_column_{i}" if col is None else col for i, col in enumerate(next(raw_data)))
        data_types = (dtype_to_pl_str.get(get_data_type(vals), "String") for vals in zip(*raw_data, strict=False))
        schema = [
            FlowfileColumn.from_input(n, d, col_index=i)
            for i, (n, d) in enumerate(zip(columns, data_types, strict=False))
        ]
    else:
        columns = (f"column_{i}" for i in range(len(next(raw_data))))
        data_types = (dtype_to_pl_str.get(get_data_type(vals), "String") for vals in zip(*raw_data, strict=False))
        schema = [
            FlowfileColumn.from_input(n, d, col_index=i)
            for i, (n, d) in enumerate(zip(columns, data_types, strict=False))
        ]
    return schema
