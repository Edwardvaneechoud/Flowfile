
from flowfile_core.schemas import input_schema, transform_schema
from typing import Callable, Iterable
from functools import wraps
from flowfile_core.schemas.output_model import NodeData
from flowfile_core.flowfile.setting_generator.setting_generator import SettingGenerator, SettingUpdator

setting_generator = SettingGenerator()
setting_updator = SettingUpdator()


def setting_generator_method(f: callable) -> Callable:
    @wraps(f)
    def inner(node_data: NodeData) -> NodeData:
        if node_data.setting_input is None or isinstance(node_data.setting_input, input_schema.NodePromise):
            f(node_data)
        return node_data
    setting_generator.add_setting_generator_func(inner)
    return inner


def setting_updator_method(f: callable) -> Callable:
    @wraps(f)
    def inner(node_data: NodeData) -> NodeData:
        if node_data.setting_input is not None and not isinstance(node_data.setting_input, input_schema.NodePromise):
            f(node_data)
        return node_data

    setting_updator.add_setting_updator_func(inner)
    return inner


@setting_generator_method
def join(node_data: "NodeData") -> NodeData:
    if node_data.right_input and node_data.main_input:
        overlapping_cols = list(set(node_data.main_input.columns) & set(node_data.right_input.columns))
        if len(overlapping_cols) > 0:
            join_key = overlapping_cols[0]
        else:
            join_key = ''
        ji = transform_schema.JoinInput(join_mapping=join_key,
                                        left_select=node_data.main_input.columns,
                                        right_select=node_data.right_input.columns
                                        )
        ji.auto_rename()
        node_data.setting_input = input_schema.NodeJoin(flow_id=node_data.flow_id,
                                                        node_id=node_data.node_id,
                                                        join_input=ji)
    return node_data


@setting_generator_method
def cross_join(node_data: "NodeData") -> NodeData:
    if node_data.right_input and node_data.main_input:
        ji = transform_schema.CrossJoinInput(left_select=node_data.main_input.columns,
                                             right_select=node_data.right_input.columns)
        ji.auto_rename()
        print(ji)
        node_data.setting_input = input_schema.NodeCrossJoin(flow_id=node_data.flow_id,
                                                             node_id=node_data.node_id,
                                                             cross_join_input=ji)
    return node_data


@setting_generator_method
def filter(node_data: "NodeData") -> NodeData:
    if node_data.main_input:
        fi = transform_schema.FilterInput(basic_filter=transform_schema.BasicFilter(), filter_type='advanced')
        node_data.setting_input = input_schema.NodeFilter(flow_id=node_data.flow_id,
                                                          node_id=node_data.node_id,
                                                          filter_input=fi)
    return node_data


@setting_updator_method
def join(node_data: NodeData):
    if node_data.right_input and node_data.main_input:
        setting_input: input_schema.NodeJoin = node_data.setting_input
        left_columns = set(node_data.main_input.columns)
        right_columns = set(node_data.right_input.columns)
        left_select = setting_input.join_input.left_select
        right_select = setting_input.join_input.right_select
        for ls in left_select.renames:
            if ls.old_name not in right_columns:
                left_select.remove_select_input(ls.old_name)
        for rs in right_select.renames:
            if rs.old_name not in right_columns:
                right_select.remove_select_input(rs.old_name)
        existing_columns_right = set(r.old_name for r in right_select.renames if r.is_available)
        existing_columns_left = set(r.old_name for r in left_select.renames if r.is_available)
        missing_incoming_left_columns = [ilc for ilc in left_columns if ilc not in existing_columns_left]
        missing_incoming_right_columns = [irc for irc in right_columns if irc not in existing_columns_right]
        if not hasattr(setting_input, 'auto_keep_left'):
            setting_input.auto_keep_left = False
        if not hasattr(setting_input, 'auto_keep_right'):
            setting_input.auto_keep_right = False
        for milc in missing_incoming_left_columns:
            select_input = transform_schema.SelectInput(old_name=milc, keep=setting_input.auto_keep_left)
            setting_input.join_input.add_new_select_column(select_input, 'left')
        for mirc in missing_incoming_right_columns:
            select_input = transform_schema.SelectInput(old_name=mirc, keep=setting_input.auto_keep_right)
            setting_input.join_input.add_new_select_column(select_input, 'right')
    return node_data


@setting_updator_method
def cross_join(node_data: NodeData):
    if node_data.right_input and node_data.main_input:
        setting_input: input_schema.NodeCrossJoin = node_data.setting_input
        left_columns = set(node_data.main_input.columns)
        right_columns = set(node_data.right_input.columns)
        left_select = setting_input.cross_join_input.left_select
        right_select = setting_input.cross_join_input.right_select
        for ls in left_select.renames:
            if ls.old_name not in right_columns:
                left_select.remove_select_input(ls.old_name)
        for rs in right_select.renames:
            if rs.old_name not in right_columns:
                right_select.remove_select_input(rs.old_name)
        existing_columns_right = set(r.old_name for r in right_select.renames if r.is_available)
        existing_columns_left = set(r.old_name for r in left_select.renames if r.is_available)
        missing_incoming_left_columns = [ilc for ilc in left_columns if ilc not in existing_columns_left]
        missing_incoming_right_columns = [irc for irc in right_columns if irc not in existing_columns_right]
        if not hasattr(setting_input, 'auto_keep_left'):
            setting_input.auto_keep_left = False
        if not hasattr(setting_input, 'auto_keep_right'):
            setting_input.auto_keep_right = False
        for milc in missing_incoming_left_columns:
            select_input = transform_schema.SelectInput(old_name=milc, keep=setting_input.auto_keep_left)
            setting_input.cross_join_input.add_new_select_column(select_input, 'left')
        for mirc in missing_incoming_right_columns:
            select_input = transform_schema.SelectInput(old_name=mirc, keep=setting_input.auto_keep_right)
            setting_input.cross_join_input.add_new_select_column(select_input, 'right')
    return node_data


def check_if_fuzzy_match_is_valid(left_columns: Iterable[str], right_columns: Iterable[str],
                                  fuzzy_map: transform_schema.FuzzyMap) -> bool:
    if fuzzy_map.left_col not in left_columns:
        return False
    if fuzzy_map.right_col not in right_columns:
        return False
    return True


@setting_updator_method
def fuzzy_match(node_data: NodeData):
    if node_data.right_input and node_data.main_input:
        setting_input: input_schema.NodeFuzzyMatch = node_data.setting_input
        left_columns = set(node_data.main_input.columns)
        right_columns = set(node_data.right_input.columns)
        left_select = setting_input.join_input.left_select
        right_select = setting_input.join_input.right_select
        for fuzzy_map in setting_input.join_input.join_mapping:
            fuzzy_map.valid = check_if_fuzzy_match_is_valid(left_columns, right_columns, fuzzy_map)
        for ls in left_select.renames:
            if ls.old_name not in right_columns:
                left_select.remove_select_input(ls.old_name)
        for rs in right_select.renames:
            if rs.old_name not in right_columns:
                right_select.remove_select_input(rs.old_name)
        existing_columns_right = set(r.old_name for r in right_select.renames if r.is_available)
        existing_columns_left = set(r.old_name for r in left_select.renames if r.is_available)
        missing_incoming_left_columns = [ilc for ilc in left_columns if ilc not in existing_columns_left]
        missing_incoming_right_columns = [irc for irc in right_columns if irc not in existing_columns_right]
        if not hasattr(setting_input, 'auto_keep_left'):
            setting_input.auto_keep_left = False
        if not hasattr(setting_input, 'auto_keep_right'):
            setting_input.auto_keep_right = False
        for milc in missing_incoming_left_columns:
            select_input = transform_schema.SelectInput(old_name=milc, keep=setting_input.auto_keep_left)
            setting_input.join_input.add_new_select_column(select_input, 'left')
        for mirc in missing_incoming_right_columns:
            select_input = transform_schema.SelectInput(old_name=mirc, keep=setting_input.auto_keep_right)
            setting_input.join_input.add_new_select_column(select_input, 'right')
    return node_data
