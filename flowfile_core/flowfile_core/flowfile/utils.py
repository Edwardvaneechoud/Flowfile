import os
import hashlib
import json
import polars as pl
import shutil

from datetime import datetime, date, time
from typing import List
from decimal import Decimal

from flowfile_core.flowfile.flow_data_engine.utils import standardize_col_dtype
from flowfile_core.schemas import input_schema


def generate_sha256_hash(data: bytes):
    sha256 = hashlib.sha256()
    sha256.update(data)
    return sha256.hexdigest()


def create_directory_if_not_exists(directory: str):
    if not os.path.exists(directory):
        os.mkdir(directory)


def snake_case_to_camel_case(text: str) -> str:
    # Split the text by underscores, capitalize each piece, and join them together
    transformed_text = ''.join(word.capitalize() for word in text.split('_'))
    return transformed_text


def json_default(val):
    if isinstance(val, datetime):
        return val.isoformat(timespec='microseconds')
    elif isinstance(val, date):
        return val.isoformat()
    elif isinstance(val, time):
        return val.isoformat()
    elif hasattr(val, '__dict__'):
        return val.__dict__
    elif isinstance(val, Decimal):
        if val.as_integer_ratio()[1] == 1:
            return int(val)
        return float(val)
    else:
        raise Exception('Value is not serializable')


def json_dumps(thing) -> str:
    return json.dumps(
        thing,
        default=json_default,
        ensure_ascii=False,
        sort_keys=True,
        indent=None,
        separators=(',', ':'),
    )


def get_hash(val):
    if hasattr(val, 'overridden_hash') and val.overridden_hash():
        val = hash(val)
    elif hasattr(val, '__dict__'):
        val = {k: v for k, v in val.__dict__.items() if k not in {'pos_x', 'pos_y'}}
    elif hasattr(val, 'json'):
        pass
    return generate_sha256_hash(json_dumps(val).encode('utf-8'))


def cleanup(start_location: str = 'temp_storage'):
    def get_all_files_and_folders(_start_location) -> List[str]:
        inspect_items = [_start_location]
        output = []
        while len(inspect_items) > 0:
            attributes = []
            for inspect_item in inspect_items:
                output.append(inspect_item)
                if os.path.isdir(inspect_item):
                    dir_attributes = [os.path.join(inspect_item, _item) for _item in os.listdir(inspect_item)]
                    if len(dir_attributes) > 0:
                        attributes += dir_attributes
            inspect_items = attributes
        return output

    output = get_all_files_and_folders(start_location)

    # get level of dept of folder and sort based on that
    actions = [(_path.count(os.sep), _path) for _path in output]
    actions = [action for action in actions if action[0] > 0]
    files_to_delete = {action[0]: [] for action in actions}
    directories_to_delete = {action[0]: [] for action in actions}
    for action in actions:
        if os.path.isfile(action[1]):
            files_to_delete[action[0]].append(action[1])
        else:
            directories_to_delete[action[0]].append(action[1])

    files_to_delete = list(files_to_delete.items())
    directories_to_delete = list(directories_to_delete.items())
    files_to_delete.sort(key=lambda x: x[0], reverse=True)
    directories_to_delete.sort(key=lambda x: x[0], reverse=True)
    for action in files_to_delete:
        for _f in action[1]:
            os.remove(_f)
    for action in directories_to_delete:
        for _f in action[1]:
            shutil.rmtree(_f)


def batch_generator(input_list: List, batch_size: int = 10000):
    run: bool = True
    while run:
        if len(input_list) > batch_size:
            yield input_list[:batch_size]
            input_list = input_list[batch_size:]
        else:
            yield input_list
            input_list = []
            run = False


def _handle_raw_data(node_manual_input: input_schema.NodeManualInput):
    """Ensure compatibility with the new typed raw data and the old dict form data type"""
    if (not (hasattr(node_manual_input, "raw_data_format") and node_manual_input.raw_data_format)
            and (hasattr(node_manual_input, 'raw_data') and node_manual_input.raw_data)):
        values = [standardize_col_dtype([vv for vv in c]) for c in zip(*(r.values()
                                                                         for r in node_manual_input.raw_data))]
        data_types = (pl.DataType.from_python(type(next((v for v in column_values), None))) for column_values in values)
        _columns = [input_schema.MinimalFieldInfo(name=c, data_type=str(next(data_types))) for c in
                    node_manual_input.raw_data[0].keys()]

        node_manual_input.raw_data_format = input_schema.RawData(columns=_columns, data=values)
    elif ((hasattr(node_manual_input, "raw_data_format") and node_manual_input.raw_data_format)
          and not (hasattr(node_manual_input, 'raw_data') and node_manual_input.raw_data)):
        node_manual_input.raw_data = [{c.name: node_manual_input.raw_data_format.data[ci][ri] for ci, c in
                                       enumerate(node_manual_input.raw_data_format.columns)}
                                      for ri in range(len(node_manual_input.raw_data_format.data[0]))]
