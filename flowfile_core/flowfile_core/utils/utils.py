import re
from itertools import chain

from shared.dtype_utils import convert_to_string, standardize_col_dtype

__all__ = ["convert_to_string", "standardize_col_dtype"]


def camel_case_to_snake_case(text: str) -> str:
    transformed_text = re.sub(r"(?<!^)(?=[A-Z])", "_", text).lower()
    return transformed_text


def ensure_similarity_dicts(datas: list[dict], respect_order: bool = True):
    all_cols = (data.keys() for data in datas)
    if not respect_order:
        unique_cols = set(chain(*all_cols))
    else:
        col_store = set()
        unique_cols = list()
        for row in all_cols:
            for col in row:
                if col not in col_store:
                    unique_cols.append(col)
                    col_store.update((col,))
    output = []
    for data in datas:
        new_record = dict()
        for col in unique_cols:
            val = data.get(col)
            new_record[col] = val
        output.append(new_record)
    return output
