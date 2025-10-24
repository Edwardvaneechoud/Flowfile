from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine, execute_polars_code
from flowfile_core.flowfile.flow_data_engine.polars_code_parser import remove_comments_and_docstrings
from flowfile_core.schemas import transform_schema
import polars as pl
import pytest

from pl_fuzzy_frame_match.models import FuzzyMapping


def create_sample_data():
    flowfile_table = FlowDataEngine.create_random(100)
    flowfile_table.lazy = True
    return flowfile_table


def test_fuzzy_match_internal():
    r = transform_schema.SelectInputs([transform_schema.SelectInput(old_name='column_0', new_name='name')])
    left_flowfile_table = FlowDataEngine(['edward', 'eduward', 'court']).do_select(r)
    right_flowfile_table = left_flowfile_table
    left_select = [transform_schema.SelectInput(c) for c in left_flowfile_table.columns]
    right_select = [transform_schema.SelectInput(c) for c in right_flowfile_table.columns]
    fuzzy_match_input = transform_schema.FuzzyMatchInput(join_mapping=[FuzzyMapping(left_col='name')],
                                                         left_select=left_select, right_select=right_select
                                                         )
    fuzzy_match_result = left_flowfile_table.fuzzy_join(fuzzy_match_input, right_flowfile_table)
    assert fuzzy_match_result is not None, 'Fuzzy match failed'
    assert fuzzy_match_result.count() > 0, 'No fuzzy matches found'
    expected_data = FlowDataEngine([{'name': 'court', 'name_vs_name_right_levenshtein': 1.0, 'name_right': 'court'},
     {'name': 'eduward', 'name_vs_name_right_levenshtein': 1.0, 'name_right': 'eduward'},
     {'name': 'edward', 'name_vs_name_right_levenshtein': 0.8571428571428572, 'name_right': 'eduward'},
     {'name': 'eduward', 'name_vs_name_right_levenshtein': 0.8571428571428572, 'name_right': 'edward'},
     {'name': 'edward', 'name_vs_name_right_levenshtein': 1.0, 'name_right': 'edward'}])
    fuzzy_match_result.assert_equal(expected_data)


@pytest.fixture
def fuzzy_test_data_left() -> FlowDataEngine:
    """
    Generates a small, predictable test dataset with data designed for fuzzy matching challenges.

    Returns:
        LazyFrame with left side test data
    """
    return FlowDataEngine(pl.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "company_name": ["Apple Inc.", "Microsft", "Amazon", "Gogle", "Facebok"],
            "address": ["1 Apple Park", "One Microsoft Way", "410 Terry Ave N", "1600 Amphitheatre", "1 Hacker Way"],
            "contact": ["Tim Cook", "Satya Ndella", "Andy Jessy", "Sundar Pichai", "Mark Zukerberg"],
        }
    ))


@pytest.fixture
def fuzzy_test_data_right() -> FlowDataEngine:
    """
    Generates a small, predictable test dataset with variations for fuzzy matching.

    Returns:
        LazyFrame with right side test data
    """
    return FlowDataEngine(pl.DataFrame(
        {
            "id": [101, 102, 103, 104, 105],
            "organization": ["Apple Incorporated", "Microsoft Corp", "Amazon.com Inc", "Google LLC", "Facebook Inc"],
            "location": [
                "Apple Park, Cupertino",
                "Microsoft Way, Redmond",
                "Terry Ave North, Seattle",
                "Amphitheatre Pkwy, Mountain View",
                "Hacker Way, Menlo Park",
            ],
            "ceo": ["Timothy Cook", "Satya Nadella", "Andy Jassy", "Sundar Pichai", "Mark Zuckerberg"],
        }
    ))


def test_fuzzy_match_auto_select_columns_not_provided(fuzzy_test_data_left, fuzzy_test_data_right):
    left_select = [transform_schema.SelectInput(c) for c in fuzzy_test_data_left.columns[:-1]]
    right_select = [transform_schema.SelectInput(c) for c in fuzzy_test_data_right.columns[:-1]]
    fuzzy_match_input = transform_schema.FuzzyMatchInput(join_mapping=[
        FuzzyMapping(left_col='company_name', right_col='organization', threshold_score=50)
    ], left_select=left_select, right_select=right_select)
    fuzzy_match_result = fuzzy_test_data_left.fuzzy_join(fuzzy_match_input, fuzzy_test_data_right)
    assert fuzzy_match_result is not None, 'Fuzzy match failed'
    assert fuzzy_match_result.number_of_fields == 9


def test_fuzzy_match_auto_select_columns_not_selected(fuzzy_test_data_left, fuzzy_test_data_right):
    left_select = [transform_schema.SelectInput(c, keep=False) for c in fuzzy_test_data_left.columns[:-1]]
    right_select = [transform_schema.SelectInput(c, keep=False) for c in fuzzy_test_data_right.columns]
    fuzzy_match_input = transform_schema.FuzzyMatchInput(join_mapping=[
        FuzzyMapping(left_col='company_name', right_col='organization', threshold_score=50)
    ], left_select=left_select, right_select=right_select)
    fuzzy_match_result = fuzzy_test_data_left.fuzzy_join(fuzzy_match_input, fuzzy_test_data_right)
    assert fuzzy_match_result is not None, 'Fuzzy match failed'
    assert fuzzy_match_result.number_of_fields == 4


def test_fuzzy_match_external():
    r = transform_schema.SelectInputs([transform_schema.SelectInput(old_name='column_0', new_name='name')])
    left_flowfile_table = FlowDataEngine(['edward', 'eduward', 'court']).do_select(r)
    right_flowfile_table = left_flowfile_table
    left_select = [transform_schema.SelectInput(c) for c in left_flowfile_table.columns]
    right_select = [transform_schema.SelectInput(c) for c in right_flowfile_table.columns]
    fuzzy_match_input = transform_schema.FuzzyMatchInput(join_mapping=[FuzzyMapping(left_col='name')],
                                                         left_select=left_select, right_select=right_select
                                                         )
    fuzzy_match_result = left_flowfile_table.fuzzy_join_external(fuzzy_match_input, right_flowfile_table)
    assert fuzzy_match_result is not None, 'Fuzzy match failed'
    assert fuzzy_match_result.count() > 0, 'No fuzzy matches found'
    expected_data = FlowDataEngine([{'name': 'court', 'name_vs_name_right_levenshtein': 1.0, 'name_right': 'court'},
     {'name': 'eduward', 'name_vs_name_right_levenshtein': 1.0, 'name_right': 'eduward'},
     {'name': 'edward', 'name_vs_name_right_levenshtein': 0.8571428571428572, 'name_right': 'eduward'},
     {'name': 'eduward', 'name_vs_name_right_levenshtein': 0.8571428571428572, 'name_right': 'edward'},
     {'name': 'edward', 'name_vs_name_right_levenshtein': 1.0, 'name_right': 'edward'}])
    fuzzy_match_result.assert_equal(expected_data)



def test_cross_join():
    left_flowfile_table = FlowDataEngine.create_random(100)
    right_flowfile_table = FlowDataEngine.create_random(100)
    left_select = transform_schema.SelectInputs.create_from_pl_df(left_flowfile_table.data_frame).renames
    right_select = transform_schema.SelectInputs.create_from_pl_df(right_flowfile_table.data_frame).renames
    cross_join_input = transform_schema.CrossJoinInput(left_select=left_select,
                                                       right_select=right_select,
                                                       )
    cross_join_result = left_flowfile_table.do_cross_join(cross_join_input,
                                                          other=right_flowfile_table,
                                                          auto_generate_selection=True,
                                                          verify_integrity=True)
    right_columns = [c + "_right" for c in right_flowfile_table.columns]
    assert cross_join_result is not None, 'Cross join failed'
    assert cross_join_result.get_number_of_records() == 100 * 100, 'Number of records is not correct'
    assert cross_join_result.columns == left_flowfile_table.columns + right_columns, 'Columns are not correct'


def create_test_dataframe():
    data = {
        'id': [1, 2, 3, 4, 5, 6, 7],
        'category': ['A', 'A', 'B', 'B', 'C', 'C', 'A'],
        'sub_category': ['A1', 'A1', 'B1', 'B2', 'C1', 'C2', 'A1'],
        'value1': [10, 20, 15, 25, 30, 5, 10],
        'value2': [5, 15, 10, 20, 25, 30, 10],
        'value3': [15, 5, 20, 10, 25, 30, 10]
    }
    return pl.DataFrame(data)


@pytest.fixture
def join_df():
    return FlowDataEngine(create_test_dataframe().select("id", "category"))


def test_group_by_numeric():
    data = create_test_dataframe()
    fl_table = FlowDataEngine(data)
    agg_cols = [transform_schema.AggColl('category', agg='groupby'),
                transform_schema.AggColl('value1', agg='sum'),
                transform_schema.AggColl('value1', agg='mean'),
                transform_schema.AggColl('value1', agg='min'),
                transform_schema.AggColl('value1', agg='max'),
                transform_schema.AggColl('value1', agg='count'),
                transform_schema.AggColl('value1', agg='n_unique'),
                ]
    group_by_input = transform_schema.GroupByInput(agg_cols=agg_cols)
    grouped_table = fl_table.do_group_by(group_by_input)

    expected_output = FlowDataEngine([
        {'category': 'B', 'value1_sum': 40, 'value1_mean': 20.0, 'value1_min': 15, 'value1_max': 25, 'value1_count': 2, 'value1_n_unique': 2},
        {'category': 'C', 'value1_sum': 35, 'value1_mean': 17.5, 'value1_min': 5, 'value1_max': 30, 'value1_count': 2, 'value1_n_unique': 2},
        {'category': 'A', 'value1_sum': 40, 'value1_mean': 13.333333333333334, 'value1_min': 10, 'value1_max': 20, 'value1_count': 3, 'value1_n_unique': 2}
    ])
    grouped_table.assert_equal(expected_output)


def test_group_by_string():
    data = create_test_dataframe()
    fl_table = FlowDataEngine(data)
    agg_cols = [transform_schema.AggColl('category', agg='groupby'),
                transform_schema.AggColl('sub_category', agg='first'),
                transform_schema.AggColl('sub_category', agg='last'),
                transform_schema.AggColl('sub_category', agg='min'),
                transform_schema.AggColl('sub_category', agg='max'),
                transform_schema.AggColl('sub_category', agg='count'),
                transform_schema.AggColl('sub_category', agg='n_unique'),
                transform_schema.AggColl('sub_category', agg='concat')
                ]
    group_by_input = transform_schema.GroupByInput(agg_cols=agg_cols)
    grouped_table = fl_table.do_group_by(group_by_input)

    expected_output = FlowDataEngine([
        {'category': 'C', 'sub_category_first': 'C1', 'sub_category_last': 'C2', 'sub_category_min': 'C1', 'sub_category_max': 'C2', 'sub_category_count': 2, 'sub_category_n_unique': 2, 'sub_category_concat': 'C1,C2'},
        {'category': 'A', 'sub_category_first': 'A1', 'sub_category_last': 'A1', 'sub_category_min': 'A1', 'sub_category_max': 'A1', 'sub_category_count': 3, 'sub_category_n_unique': 1, 'sub_category_concat': 'A1,A1,A1'},
        {'category': 'B', 'sub_category_first': 'B1', 'sub_category_last': 'B2', 'sub_category_min': 'B1', 'sub_category_max': 'B2', 'sub_category_count': 2, 'sub_category_n_unique': 2, 'sub_category_concat': 'B1,B2'}])
    grouped_table.assert_equal(expected_output)


def test_grouped_record_id():
    fl_table = FlowDataEngine(pl.DataFrame({
        "id": [1, 2, 3, 4, 5, 6, 7, 8],
        "category": ["A", "A", "B", "B", "C", "C", 'C', 'B'],
        "sub_category": ["A1", "A1", "B1", "B2", "C1", "C2", 'A1', 'B2'],
        "value1": [10, 20, 15, 25, 30, 5, 12, 30],
    }))
    record_id_settings = transform_schema.RecordIdInput(group_by=True,
                                                        group_by_columns=['category', 'sub_category'],
                                                        output_column_name='ranking')
    output = fl_table.add_record_id(record_id_settings)
    expected_output = (fl_table.add_new_values([1, 2, 1, 1, 1, 1, 1, 2], 'ranking')
                       .select_columns(['ranking', 'id', 'category', 'sub_category', 'value1']))
    output.assert_equal(expected_output)


def test_pivot_numeric():
    fl_table = FlowDataEngine(pl.DataFrame({
        'id': [1, 1, 2, 2, 2, 1, 1],
        'category': ['A', 'A', 'B', 'B', 'C', 'C', 'A'],
        'value': [10, 20, 15, 25, 30, 5, 10],
    }))
    pivot_input = transform_schema.PivotInput(pivot_column='id', value_col='value', index_columns=['category'],
                                              aggregations=['sum'])
    output = fl_table.do_pivot(pivot_input)
    expected_output = FlowDataEngine([{'category': 'C', '1': 5, '2': 30},
                                     {'category': 'B', '1': None, '2': 40},
                                     {'category': 'A', '1': 40, '2': None}])
    output.assert_equal(expected_output)


def test_pivot_string_concat():
    fl_table = FlowDataEngine(pl.DataFrame({
        'id': [1, 1, 2, 2, 2, 1, 1],
        'category': ['A', 'A', 'B', 'B', 'C', 'C', 'A'],
        'value': ['10', '20', '15', '25', '30', '5', '10'],
    }))
    pivot_input = transform_schema.PivotInput(pivot_column='id', value_col='value', index_columns=['category'],
                                              aggregations=['concat'])
    output = fl_table.do_pivot(pivot_input)
    expected_output = FlowDataEngine([{'category': 'A', '1': '10,20,10', '2': None},
                                     {'category': 'B', '1': None, '2': '15,25'},
                                     {'category': 'C', '1': '5', '2': '30'}])
    output.assert_equal(expected_output)


def test_split_to_rows():
    fl_table = FlowDataEngine(pl.DataFrame(pl.DataFrame([["1,2,3", "1,2,3"], [1, 2]]), schema=['text', 'rank']))
    split_input = transform_schema.TextToRowsInput(column_to_split='text', output_column_name='splitted')
    output = fl_table.split(split_input)
    expected_output = FlowDataEngine(pl.DataFrame(
        {'text': ['1,2,3', '1,2,3', '1,2,3', '1,2,3', '1,2,3', '1,2,3'], 'rank': [1, 1, 1, 2, 2, 2],
         'splitted': ['1', '2', '3', '1', '2', '3']}))
    output.assert_equal(expected_output)


def test_split_to_rows_same_name():
    fl_table = FlowDataEngine(pl.DataFrame(pl.DataFrame([["1,2,3", "1,2,3"], [1, 2]]), schema=['text', 'rank']))
    split_input = transform_schema.TextToRowsInput(column_to_split='text')
    output = fl_table.split(split_input)
    output.data_frame.to_dict(as_series=False)
    expected_output = FlowDataEngine(pl.DataFrame({'text': ['1', '2', '3', '1', '2', '3'], 'rank': [1, 1, 1, 2, 2, 2]}))
    output.assert_equal(expected_output)


def test_split_to_rows_var_sep():
    fl_table = FlowDataEngine(
        pl.DataFrame(pl.DataFrame([["1|2,3", "1,2,3"], [1, 2], ['|', ',']]), schema=['text', 'rank', 'sep']))
    split_input = transform_schema.TextToRowsInput(column_to_split='text', split_by_column='sep',
                                                   split_by_fixed_value=False, output_column_name='splitted')
    output = fl_table.split(split_input)
    output.data_frame.to_dict(as_series=False)
    expected_output = FlowDataEngine(pl.DataFrame(
        {'text': ['1|2,3', '1|2,3', '1,2,3', '1,2,3', '1,2,3'], 'rank': [1, 1, 2, 2, 2],
         'sep': ['|', '|', ',', ',', ','], 'splitted': ['1', '2,3', '1', '2', '3']}))
    output.assert_equal(expected_output)


def test_execute_polars_code():
    fl_table = FlowDataEngine(create_test_dataframe())
    code = """
    def abc(df):
        return df.group_by('value3').len()
    output_df = abc(input_df)
    """
    result_data = execute_polars_code(fl_table, code=code)
    expected_data = FlowDataEngine([[30, 20, 5, 25, 10, 15], [1, 1, 1, 1, 2, 1]], schema=['value3', 'len'])
    result_data.assert_equal(expected_data)


def get_join_settings(how: str):
    return {'join_mapping': [{'left_col': 'name', 'right_col': 'name'}],
            'left_select': {'renames': [
                {'old_name': 'name', 'new_name': 'name', 'data_type': None,
                 'data_type_change': False, 'join_key': False,
                 'is_altered': False, 'position': None, 'is_available': True,
                 'keep': True}]}, 'right_select': {'renames': [
            {'old_name': 'name', 'new_name': 'right_name', 'data_type': None, 'data_type_change': False,
             'join_key': False, 'is_altered': False, 'position': None, 'is_available': True, 'keep': True}]},
            'how': how}


def test_join_inner():
    join_input = transform_schema.JoinInput(**get_join_settings('inner'))
    left_df = FlowDataEngine([{"name": "eduward"},
                             {"name": "edward"},
                             {"name": "courtney"}])
    right_df = FlowDataEngine([{"name": "edward"}])
    result_df = left_df.join(join_input=join_input, other=right_df, verify_integrity=False,
                             auto_generate_selection=True)
    expected_df = FlowDataEngine([{"name": "edward", "right_name": "edward"}])
    result_df.assert_equal(expected_df)


def test_join_left():
    join_input = transform_schema.JoinInput(**get_join_settings('left'))
    left_df = FlowDataEngine([{"name": "eduward"},
                             {"name": "edward"},
                             {"name": "courtney"}])
    right_df = FlowDataEngine([{"name": "edward"}])
    result_df = left_df.join(join_input=join_input, other=right_df, verify_integrity=False,
                                auto_generate_selection=True)
    result_df.to_dict()
    expected_df = FlowDataEngine({'name': ['eduward', 'edward', 'courtney'], 'right_name': [None, 'edward', None]})
    result_df.assert_equal(expected_df)


def test_join_right():
    join_input = transform_schema.JoinInput(**get_join_settings('right'))
    flow_data_engine = FlowDataEngine([{"name": "eduward"},
                             {"name": "edward"},
                             {"name": "courtney"}])
    other = FlowDataEngine([{"name": "edward"}])
    result_df = flow_data_engine.join(join_input=join_input, other=other, verify_integrity=False,
                                      auto_generate_selection=True)
    expected_df = FlowDataEngine([{"right_name": "edward", "name" :"edward"}])
    result_df.assert_equal(expected_df)


def test_join_outer():
    join_input = transform_schema.JoinInput(**get_join_settings('outer'))
    left_df = FlowDataEngine([{"name": "eduward"},
                             {"name": "edward"},
                             {"name": "courtney"}])
    right_df = FlowDataEngine([{"name": "edwin"}])
    result_df = left_df.join(join_input=join_input, other=right_df, verify_integrity=False,
                                auto_generate_selection=True)
    expected_df = FlowDataEngine([{"name": "eduward"},
                                 {"name": "edward"},
                                 {"name": "courtney"},
                                 {"right_name": "edwin"}])
    result_df.assert_equal(expected_df)


def test_join_semi():
    join_input = transform_schema.JoinInput(**get_join_settings('semi'))
    left_df = FlowDataEngine([{"name": "eduward"},
                             {"name": "edward"},
                             {"name": "courtney"}])
    right_df = FlowDataEngine([{"name": "edward"}])
    result_df = left_df.join(join_input=join_input, other=right_df, verify_integrity=False,
                                auto_generate_selection=True)
    expected_df = FlowDataEngine([{"name": "edward"}])
    result_df.assert_equal(expected_df)


def test_join_anti():
    join_input = transform_schema.JoinInput(
        join_mapping=[transform_schema.JoinMap('name')],
        left_select=transform_schema.JoinInputs(renames=[transform_schema.SelectInput(old_name='name', keep=True), transform_schema.SelectInput(old_name='other')]),
        right_select=transform_schema.JoinInputs(renames=[transform_schema.SelectInput(old_name='name', keep=False)]),
        how='anti'
    )
    left_df = FlowDataEngine([{"name": "eduward", "other": 1},
                             {"name": "edward", "other": 1},
                             {"name": "courtney", "other": 1}])
    right_df = FlowDataEngine([{"name": "edward", "other": 1}])
    result_df = left_df.join(join_input=join_input, other=right_df, verify_integrity=False,
                                auto_generate_selection=True)
    expected_df = FlowDataEngine([{"name": "eduward", "other": 1},
                                 {"name": "courtney", "other": 1}])
    result_df.assert_equal(expected_df)


def test_join_anti_not_selecting_join_key():
    join_input = transform_schema.JoinInput(
        join_mapping=[transform_schema.JoinMap('name')],
        left_select=transform_schema.JoinInputs(renames=[transform_schema.SelectInput(old_name='name', keep=False), transform_schema.SelectInput(old_name='other')]),
        right_select=transform_schema.JoinInputs(renames=[transform_schema.SelectInput(old_name='name', keep=False)]),
        how='anti'
    )
    left_df = FlowDataEngine([{"name": "eduward", "other": 1},
                              {"name": "edward", "other": 1},
                              {"name": "courtney", "other": 1}])
    right_df = FlowDataEngine([{"name": "edward", "other": 1}])
    result_df = left_df.join(join_input=join_input, other=right_df, verify_integrity=False,
                             auto_generate_selection=True)
    expected_df = FlowDataEngine([{"other": 1},
                                  {"other": 1}])
    result_df.assert_equal(expected_df)


def test_join_non_selecting_join_keys_inner(join_df: FlowDataEngine):
    join_input = transform_schema.JoinInput(
        join_mapping='id',
        left_select=[transform_schema.SelectInput(old_name='id', keep=False), transform_schema.SelectInput('category')],
        right_select=[transform_schema.SelectInput(old_name='id', keep=False), transform_schema.SelectInput('category')],
        how='inner'
    )

    right_df = join_df.get_sample(1)
    result = join_df.join(other=right_df, join_input=join_input, auto_generate_selection=True, verify_integrity=False)
    expected = FlowDataEngine([{'category': 'A', 'category_right': 'A'}])
    result.assert_equal(expected)


def test_outer_join_rename():
    df_1 = FlowDataEngine(pl.LazyFrame([[1, 2, 3], ['Alice', 'Bob', 'Charlie']], schema=pl.Schema([("id", pl.Int64), ("name", pl.String)])))
    df_2 = FlowDataEngine(pl.LazyFrame([[1, 2, 4], ['NYC', 'LA', 'Chicago']], schema=pl.Schema([("id", pl.Int64), ("city", pl.String)])))
    df_1.lazy = False; df_2.lazy = False
    join_input = transform_schema.JoinInput(
        join_mapping="id",
        left_select=[
            transform_schema.SelectInput(old_name='id', new_name='id', keep=True),
            transform_schema.SelectInput(old_name='name',new_name='name', keep=True)],
        right_select=[
            transform_schema.SelectInput(old_name='id', new_name='id', keep=False),
            transform_schema.SelectInput(old_name='city', new_name='city', keep=True)],
        how='outer')
    result = df_1.join(other=df_2, join_input=join_input, auto_generate_selection=True, verify_integrity=False)
    expected = FlowDataEngine({'id': [1, 2, None, 3], 'name': ['Alice', 'Bob', None, 'Charlie'], 'city': ['NYC', 'LA', 'Chicago', None]})
    result.assert_equal(expected)


def test_join_input_overlapping_columns():
    data_engine = FlowDataEngine.create_random(500)
    left_data = data_engine.select_columns(['ID', "Name", "Address", "Zipcode"])
    right_data = data_engine.get_sample(100, random=True).select_columns(["ID", "Name", "City"])
    join_input = transform_schema.JoinInput(
        join_mapping=[transform_schema.JoinMap("ID", "ID")],
        left_select=[transform_schema.SelectInput("ID"),
                     transform_schema.SelectInput("Address"),
                     transform_schema.SelectInput("Zipcode"),
                     transform_schema.SelectInput("Name", keep=False)],
        right_select=[transform_schema.SelectInput("ID", keep=False),
                      transform_schema.SelectInput("City", keep=False),
                      transform_schema.SelectInput("Name", keep=False)],
        how="inner"
    )
    output = left_data.join(other=right_data, join_input=join_input, auto_generate_selection=True, verify_integrity=False)


def test_join_no_selection(join_df: FlowDataEngine):
    breakpoint()
    join_input = transform_schema.JoinInput(
        join_mapping='id',
        left_select=[],
        right_select=[]
    )
    right_df = join_df.get_sample(1)
    result = join_df.join(other=right_df, join_input=join_input, auto_generate_selection=True, verify_integrity=False)


def test_join_non_selecting_join_keys_left(join_df: FlowDataEngine):
    join_input = transform_schema.JoinInput(
        join_mapping='id',
        left_select=[transform_schema.SelectInput(old_name='id', keep=False),
                     transform_schema.SelectInput('category')],
        right_select=[transform_schema.SelectInput(old_name='id', keep=False),
                      transform_schema.SelectInput('category')],
        how='left'
    )
    right_df = join_df.get_sample(1)
    result = join_df.join(other=right_df, join_input=join_input, auto_generate_selection=True,
                          verify_integrity=False)
    expected = FlowDataEngine({'category': ['A', 'A', 'B', 'B', 'C', 'C', 'A'],
                               'category_right': ['A', None, None, None, None, None, None]})
    result.assert_equal(expected)


def test_join_non_selecting_join_keys_right(join_df: FlowDataEngine):
    join_input = transform_schema.JoinInput(
        join_mapping='id',
        left_select=[transform_schema.SelectInput(old_name='id', keep=False),
                     transform_schema.SelectInput('category')],
        right_select=[transform_schema.SelectInput(old_name='id', keep=False),
                      transform_schema.SelectInput('category')],
        how='right'
    )
    right_df = join_df.get_sample(1)
    result = join_df.join(other=right_df, join_input=join_input, auto_generate_selection=True,
                          verify_integrity=False)
    expected = FlowDataEngine({'category': 'A',
                               'category_right': 'A'})
    result.assert_equal(expected)


def test_join_non_selecting_renamed_keys_right(join_df: FlowDataEngine):
    join_input = transform_schema.JoinInput(
        join_mapping='id',
        left_select=[transform_schema.SelectInput(old_name='id', keep=False),
                     transform_schema.SelectInput('category')],
        right_select=[transform_schema.SelectInput(old_name='id', keep=False, new_name="id_right"),
                      transform_schema.SelectInput('category', keep=False, new_name="category_right")],
        how='left'
    )
    right_df = join_df.get_sample(1)
    result = join_df.join(other=right_df, join_input=join_input, auto_generate_selection=True,
                          verify_integrity=False)
    result.assert_equal(join_df.select_columns("category"))


def test_join_select_join_key_right(join_df: FlowDataEngine):
    join_input = transform_schema.JoinInput(
        join_mapping='id',
        left_select=[transform_schema.SelectInput(old_name='id', keep=False),
                     transform_schema.SelectInput('category')],
        right_select=[transform_schema.SelectInput(old_name='id', new_name='id', keep=True),
                      transform_schema.SelectInput('category')],
        how='right'
    )
    right_df = join_df.get_sample(1)
    result = join_df.join(other=right_df, join_input=join_input, auto_generate_selection=True,
                          verify_integrity=False)
    expected = FlowDataEngine({'id': 1,
                               'category': 'A',
                               'category_right': 'A',
                               })
    result.assert_equal(expected)


def test_join_select_join_key_left(join_df: FlowDataEngine):
    join_input = transform_schema.JoinInput(
        join_mapping='id',
        left_select=[transform_schema.SelectInput(old_name='id', keep=True),
                     transform_schema.SelectInput('category')],
        right_select=[transform_schema.SelectInput(old_name='id', keep=False),
                      transform_schema.SelectInput('category')],
        how='left'
    )
    right_df = join_df.get_sample(1)
    result = join_df.join(other=right_df, join_input=join_input, auto_generate_selection=True,
                          verify_integrity=False)
    expected = FlowDataEngine({'id': [1, 2, 3, 4, 5, 6, 7],
                               'category': ['A', 'A', 'B', 'B', 'C', 'C', 'A'],
                               'category_right': ['A', None, None, None, None, None, None]})
    result.assert_equal(expected)


def test_join_select_join_key_left_rename(join_df: FlowDataEngine):
    join_input = transform_schema.JoinInput(
        join_mapping='id',
        left_select=[transform_schema.SelectInput(old_name='id', new_name='left_id', keep=True),
                     transform_schema.SelectInput('category')],
        right_select=[transform_schema.SelectInput(old_name='id', keep=False),
                      transform_schema.SelectInput('category')],
        how='left'
    )
    right_df = join_df.get_sample(1)
    result = join_df.join(other=right_df, join_input=join_input, auto_generate_selection=True,
                          verify_integrity=False)
    expected = FlowDataEngine({'left_id': [1, 2, 3, 4, 5, 6, 7],
                               'category': ['A', 'A', 'B', 'B', 'C', 'C', 'A'],
                               'category_right': ['A', None, None, None, None, None, None]})
    result.assert_equal(expected)


def test_join_select_join_key_right_rename(join_df: FlowDataEngine):
    join_input = transform_schema.JoinInput(
        join_mapping='id',
        left_select=[transform_schema.SelectInput(old_name='id', new_name='id', keep=True),
                     transform_schema.SelectInput('category')],
        right_select=[transform_schema.SelectInput(old_name='id', new_name='right_id', keep=True),
                      transform_schema.SelectInput('category')],
        how='left'
    )
    right_df = join_df.get_sample(1)
    result = join_df.join(other=right_df, join_input=join_input, auto_generate_selection=True,
                          verify_integrity=False)
    expected = FlowDataEngine({'id': [1, 2, 3, 4, 5, 6, 7],
                               'category': ['A', 'A', 'B', 'B', 'C', 'C', 'A'],
                               'category_right': ['A', None, None, None, None, None, None],
                               'right_id': [1, None, None, None, None, None, None]})
    result.assert_equal(expected)


def test_join_select_select_all(join_df: FlowDataEngine):
    join_input = transform_schema.JoinInput(
        join_mapping='id',
        left_select=[transform_schema.SelectInput(old_name='id', keep=True),
                     transform_schema.SelectInput('category')],
        right_select=[transform_schema.SelectInput(old_name='id', keep=True),
                      transform_schema.SelectInput('category')],
        how='left'
    )
    right_df = join_df.get_sample(1)
    result = join_df.join(other=right_df, join_input=join_input, auto_generate_selection=True,
                          verify_integrity=False)
    expected = FlowDataEngine({'id': [1, 2, 3, 4, 5, 6, 7],
                               'category': ['A', 'A', 'B', 'B', 'C', 'C', 'A'],
                               'category_right': ['A', None, None, None, None, None, None],
                               'id_right': [1, None, None, None, None, None, None]})
    result.assert_equal(expected)


def test_execute_polars_code_no_frame():
    result = execute_polars_code(code="output_df = pl.LazyFrame({'r':[1,2,3]})")
    assert len(result) == 3, 'Expecting three records'
    assert result.columns == ['r'], 'Columns should be r'
    result.assert_equal(FlowDataEngine({'r': [1, 2, 3]}))


def test_polars_code_one_frame():
    test_df = FlowDataEngine([{"name": "eduward"},
                             {"name": "edward"},
                             {"name": "courtney"}])
    result = execute_polars_code(test_df, code='input_df.with_columns([pl.col("name").alias("other_name")])')
    expected_result = FlowDataEngine([{'name': 'eduward', 'other_name': 'eduward'},
                                     {'name': 'edward', 'other_name': 'edward'},
                                     {'name': 'courtney', 'other_name': 'courtney'}])
    result.assert_equal(expected_result)


def test_execute_polars_code_function():
    test_df = FlowDataEngine([{"name": "eduward"},
                             {"name": "edward"},
                             {"name": "courtney"}])
    code = """def do_something(df):
    return df.with_columns([pl.col("name").alias("other_name")])
output_df = do_something(input_df)"""
    result = execute_polars_code(test_df, code=code)
    expected_result = FlowDataEngine([{'name': 'eduward', 'other_name': 'eduward'},
                                     {'name': 'edward', 'other_name': 'edward'},
                                     {'name': 'courtney', 'other_name': 'courtney'}])
    result.assert_equal(expected_result)


def test_execute_multi_line():
    test_df = FlowDataEngine([{"name": "eduward"},
                             {"name": "edward"},
                             {"name": "courtney"}])
    code = """temp_df = input_df.with_columns([pl.col("name").alias("other_name")])
output_df = temp_df.select("other_name")"""
    result = execute_polars_code(test_df, code=code)
    expected_result = FlowDataEngine([{'other_name': 'eduward'}, {'other_name': 'edward'}, {'other_name': 'courtney'}])
    result.assert_equal(expected_result)


def test_error_no_output_df():
    test_df = FlowDataEngine(
        [{"name": "eduward"}, {"name": "edward"}, {"name": "courtney"}]
    )
    code = """temp_df = input_df.with_columns([pl.col("name").alias("other_name")])
something_else_df = temp_df.select("other_name")"""

    # Using pytest to check for the expected exception
    with pytest.raises(NameError) as excinfo:
        execute_polars_code(test_df, code=code)

    # Verify the error message
    assert "name 'output_df' is not defined" in str(
        excinfo.value
    ), "Expected error about output_df not being defined"


def test_execute_polars_code_multiple_frames():
    # Create two test dataframes
    test_df1 = FlowDataEngine(
        [
            {"id": 1, "name": "eduward"},
            {"id": 2, "name": "edward"},
            {"id": 3, "name": "courtney"},
        ]
    )

    test_df2 = FlowDataEngine(
        [
            {"id": 1, "department": "Engineering"},
            {"id": 2, "department": "Marketing"},
            {"id": 4, "department": "Sales"},
        ]
    )

    # Code that joins the two dataframes
    code = """
# Join the two dataframes on id
joined_df = input_df_1.join(input_df_2, on="id", how="inner")
output_df = joined_df.select(["id", "name", "department"])
"""

    result = execute_polars_code(test_df1, test_df2, code=code)

    expected_result = FlowDataEngine(
        [
            {"id": 1, "name": "eduward", "department": "Engineering"},
            {"id": 2, "name": "edward", "department": "Marketing"},
        ]
    )

    result.assert_equal(expected_result)


def test_execute_polars_code_with_syntax_error():
    """Test handling of code with syntax errors"""
    test_df = FlowDataEngine(
        [{"name": "eduward"}, {"name": "edward"}, {"name": "courtney"}]
    )

    # Code with a syntax error (missing closing parenthesis)
    code = """
output_df = input_df.filter(pl.col("name").str.contains("e"
"""

    # Check that appropriate error is raised
    with pytest.raises(ValueError) as excinfo:
        execute_polars_code(test_df, code=code)

    # Verify the error message mentions syntax
    assert "syntax" in str(excinfo.value).lower(), "Expected error about syntax"


def test_remove_comments_and_docstrings():
    test_cases = [
        (
            '# Add your polars code here\ninput_df #this is the input_df\n"""this is doc string"""\n',
            'input_df'
        ),
        (
            '"""docstring"""\nx = 1',
            'x = 1'
        ),
        (
            '"""docstring"""\nx = 1\n"""test"""',
            'x = 1'
        ),
        (
            'x = """string literal"""',
            "x = 'string literal'"
        ),
        (
            'print("# not a comment")',
            "print('# not a comment')"
        ),
        (
            '"""first"""\n"""second"""\nx = 1',
            'x = 1'
        )
    ]

    for input_code, expected in test_cases:
        result = remove_comments_and_docstrings(input_code).strip()
        try:
            assert result == expected, f"\nInput:\n{input_code}\nExpected:\n{expected}\nGot:\n{result}"
        except Exception as e:
            print(f"Test failed for:\n{input_code}")
            raise e
