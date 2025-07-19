import re
from itertools import chain
from typing import List, Dict

def camel_case_to_snake_case(text: str) -> str:
    # Use a regular expression to find capital letters and replace them with _ followed by the lowercase letter
    transformed_text = re.sub(r'(?<!^)(?=[A-Z])', '_', text).lower()
    return transformed_text


def ensure_similarity_dicts(datas: List[Dict], respect_order: bool = True):
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


def convert_to_string(v):
    try:
        return str(v)
    except:
        return None


def standardize_col_dtype(vals):
    types = set(type(val) for val in vals)
    if len(types) == 1:
        return vals
    elif int in types and float in types:
        return vals
    else:
        return [convert_to_string(v) for v in vals]