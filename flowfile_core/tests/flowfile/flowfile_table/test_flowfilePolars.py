from flowfile_core.flowfile.flowfile_table.flowfile_table import FlowfileTable
from flowfile_core.flowfile.flowfile_table.polars_code_parser import remove_comments_and_docstrings
from flowfile_core.schemas import transform_schema
import polars as pl


def create_sample_data():
    flowfile_table = FlowfileTable.create_random(100)
    flowfile_table.lazy = True
    return flowfile_table


def test_fuzzy_match():
    r = transform_schema.SelectInputs([transform_schema.SelectInput(old_name='column_0', new_name='name')])
    left_flowfile_table = FlowfileTable(['edward', 'eduward', 'court']).do_select(r)
    right_flowfile_table = left_flowfile_table
    left_select = [transform_schema.SelectInput(c) for c in left_flowfile_table.columns]
    right_select = [transform_schema.SelectInput(c) for c in right_flowfile_table.columns]
    fuzzy_match_input = transform_schema.FuzzyMatchInput(join_mapping=[transform_schema.FuzzyMap(left_col='name')],
                                                         left_select=left_select, right_select=right_select
                                                         )
    fuzzy_match_result = left_flowfile_table.do_fuzzy_join(fuzzy_match_input, right_flowfile_table, 'test')
    assert fuzzy_match_result is not None, 'Fuzzy match failed'
    assert fuzzy_match_result.count() > 0, 'No fuzzy matches found'
    expected_data = FlowfileTable([{'name': 'court', 'fuzzy_score_0': 1.0, 'right_name': 'court'},
     {'name': 'eduward', 'fuzzy_score_0': 1.0, 'right_name': 'eduward'},
     {'name': 'edward', 'fuzzy_score_0': 0.8571428571428572, 'right_name': 'eduward'},
     {'name': 'eduward', 'fuzzy_score_0': 0.8571428571428572, 'right_name': 'edward'},
     {'name': 'edward', 'fuzzy_score_0': 1.0, 'right_name': 'edward'}])
    fuzzy_match_result.assert_equal(expected_data)


def test_cross_join():
    left_flowfile_table = FlowfileTable.create_random(100)
    right_flowfile_table = FlowfileTable.create_random(100)
    left_select = transform_schema.SelectInputs.create_from_pl_df(left_flowfile_table.data_frame).renames
    right_select = transform_schema.SelectInputs.create_from_pl_df(right_flowfile_table.data_frame).renames
    cross_join_input = transform_schema.CrossJoinInput(left_select=left_select,
                                                       right_select=right_select,
                                                       )
    cross_join_result = left_flowfile_table.do_cross_join(cross_join_input,
                                                          other=right_flowfile_table,
                                                          auto_generate_selection=True,
                                                          verify_integrity=True)
    right_columns = ['right_' + c for c in right_flowfile_table.columns]
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


def test_group_by_numeric():
    data = create_test_dataframe()
    fl_table = FlowfileTable(data)
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

    expected_output = FlowfileTable([
        {'category': 'B', 'value1_sum': 40, 'value1_mean': 20.0, 'value1_min': 15, 'value1_max': 25, 'value1_count': 2, 'value1_n_unique': 2},
        {'category': 'C', 'value1_sum': 35, 'value1_mean': 17.5, 'value1_min': 5, 'value1_max': 30, 'value1_count': 2, 'value1_n_unique': 2},
        {'category': 'A', 'value1_sum': 40, 'value1_mean': 13.333333333333334, 'value1_min': 10, 'value1_max': 20, 'value1_count': 3, 'value1_n_unique': 2}
    ])
    grouped_table.assert_equal(expected_output)


def test_group_by_string():
    data = create_test_dataframe()
    fl_table = FlowfileTable(data)
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

    expected_output = FlowfileTable([
        {'category': 'C', 'sub_category_first': 'C1', 'sub_category_last': 'C2', 'sub_category_min': 'C1', 'sub_category_max': 'C2', 'sub_category_count': 2, 'sub_category_n_unique': 2, 'sub_category_concat': 'C1,C2'},
        {'category': 'A', 'sub_category_first': 'A1', 'sub_category_last': 'A1', 'sub_category_min': 'A1', 'sub_category_max': 'A1', 'sub_category_count': 3, 'sub_category_n_unique': 1, 'sub_category_concat': 'A1,A1,A1'},
        {'category': 'B', 'sub_category_first': 'B1', 'sub_category_last': 'B2', 'sub_category_min': 'B1', 'sub_category_max': 'B2', 'sub_category_count': 2, 'sub_category_n_unique': 2, 'sub_category_concat': 'B1,B2'}])
    grouped_table.assert_equal(expected_output)


def test_grouped_record_id():
    fl_table = FlowfileTable(pl.DataFrame({
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
    fl_table = FlowfileTable(pl.DataFrame({
        'id': [1, 1, 2, 2, 2, 1, 1],
        'category': ['A', 'A', 'B', 'B', 'C', 'C', 'A'],
        'value': [10, 20, 15, 25, 30, 5, 10],
    }))
    pivot_input = transform_schema.PivotInput(pivot_column='id', value_col='value', index_columns=['category'],
                                              aggregations=['sum'])
    output = fl_table.do_pivot(pivot_input)
    expected_output = FlowfileTable([{'category': 'C', '1_sum': 5, '2_sum': 30},
                                     {'category': 'B', '1_sum': None, '2_sum': 40},
                                     {'category': 'A', '1_sum': 40, '2_sum': None}])
    output.assert_equal(expected_output)


def test_pivot_string_concat():
    fl_table = FlowfileTable(pl.DataFrame({
        'id': [1, 1, 2, 2, 2, 1, 1],
        'category': ['A', 'A', 'B', 'B', 'C', 'C', 'A'],
        'value': ['10', '20', '15', '25', '30', '5', '10'],
    }))
    pivot_input = transform_schema.PivotInput(pivot_column='id', value_col='value', index_columns=['category'],
                                              aggregations=['concat'])
    output = fl_table.do_pivot(pivot_input)
    expected_output = FlowfileTable([{'category': 'A', '1_concat': '10,20,10', '2_concat': None},
                                     {'category': 'B', '1_concat': None, '2_concat': '15,25'},
                                     {'category': 'C', '1_concat': '5', '2_concat': '30'}])
    output.assert_equal(expected_output)


def test_split_to_rows():
    fl_table = FlowfileTable(pl.DataFrame(pl.DataFrame([["1,2,3", "1,2,3"], [1, 2]]), schema=['text', 'rank']))
    split_input = transform_schema.TextToRowsInput(column_to_split='text', output_column_name='splitted')
    output = fl_table.split(split_input)
    expected_output = FlowfileTable(pl.DataFrame(
        {'text': ['1,2,3', '1,2,3', '1,2,3', '1,2,3', '1,2,3', '1,2,3'], 'rank': [1, 1, 1, 2, 2, 2],
         'splitted': ['1', '2', '3', '1', '2', '3']}))
    output.assert_equal(expected_output)


def test_split_to_rows_same_name():
    fl_table = FlowfileTable(pl.DataFrame(pl.DataFrame([["1,2,3", "1,2,3"], [1, 2]]), schema=['text', 'rank']))
    split_input = transform_schema.TextToRowsInput(column_to_split='text')
    output = fl_table.split(split_input)
    output.data_frame.to_dict(as_series=False)
    expected_output = FlowfileTable(pl.DataFrame({'text': ['1', '2', '3', '1', '2', '3'], 'rank': [1, 1, 1, 2, 2, 2]}))
    output.assert_equal(expected_output)


def test_split_to_rows_var_sep():
    fl_table = FlowfileTable(
        pl.DataFrame(pl.DataFrame([["1|2,3", "1,2,3"], [1, 2], ['|', ',']]), schema=['text', 'rank', 'sep']))
    split_input = transform_schema.TextToRowsInput(column_to_split='text', split_by_column='sep',
                                                   split_by_fixed_value=False, output_column_name='splitted')
    output = fl_table.split(split_input)
    output.data_frame.to_dict(as_series=False)
    expected_output = FlowfileTable(pl.DataFrame(
        {'text': ['1|2,3', '1|2,3', '1,2,3', '1,2,3', '1,2,3'], 'rank': [1, 1, 2, 2, 2],
         'sep': ['|', '|', ',', ',', ','], 'splitted': ['1', '2,3', '1', '2', '3']}))
    output.assert_equal(expected_output)


def test_execute_polars_code():
    fl_table = FlowfileTable(create_test_dataframe())
    code = """
    def abc(df):
        return df.group_by('value3').len()
    output_df = abc(input_df)
    """
    result_data = fl_table.execute_polars_code(code)
    expected_data = FlowfileTable([[30, 20, 5, 25, 10, 15], [1, 1, 1, 1, 2, 1]], schema=['value3', 'len'])
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
    left_df = FlowfileTable([{"name": "eduward"},
                             {"name": "edward"},
                             {"name": "courtney"}])
    right_df = FlowfileTable([{"name": "edward"}])
    result_df = left_df.join(join_input=join_input, other=right_df, verify_integrity=False,
                                auto_generate_selection=True)
    expected_df = FlowfileTable([{"name": "edward"}])
    result_df.assert_equal(expected_df)


def test_join_left():
    join_input = transform_schema.JoinInput(**get_join_settings('left'))
    left_df = FlowfileTable([{"name": "eduward"},
                             {"name": "edward"},
                             {"name": "courtney"}])
    right_df = FlowfileTable([{"name": "edward"}])
    result_df = left_df.join(join_input=join_input, other=right_df, verify_integrity=False,
                                auto_generate_selection=True)
    expected_df = FlowfileTable([{"name": "eduward"},
                                 {"name": "edward"},
                                 {"name": "courtney"}])
    result_df.assert_equal(expected_df)


def test_join_right():
    join_input = transform_schema.JoinInput(**get_join_settings('right'))
    self = FlowfileTable([{"name": "eduward"},
                             {"name": "edward"},
                             {"name": "courtney"}])
    other = FlowfileTable([{"name": "edward"}])
    result_df = self.join(join_input=join_input, other=other, verify_integrity=False,
                             auto_generate_selection=True)
    expected_df = FlowfileTable([{"right_name": "edward"}])
    result_df.assert_equal(expected_df)


def test_join_outer():
    join_input = transform_schema.JoinInput(**get_join_settings('outer'))
    left_df = FlowfileTable([{"name": "eduward"},
                             {"name": "edward"},
                             {"name": "courtney"}])
    right_df = FlowfileTable([{"name": "edwin"}])
    result_df = left_df.join(join_input=join_input, other=right_df, verify_integrity=False,
                                auto_generate_selection=True)
    expected_df = FlowfileTable([{"name": "eduward"},
                                 {"name": "edward"},
                                 {"name": "courtney"},
                                 {"right_name": "edwin"}])
    result_df.assert_equal(expected_df)


def test_join_semi():
    join_input = transform_schema.JoinInput(**get_join_settings('semi'))
    left_df = FlowfileTable([{"name": "eduward"},
                             {"name": "edward"},
                             {"name": "courtney"}])
    right_df = FlowfileTable([{"name": "edward"}])
    result_df = left_df.join(join_input=join_input, other=right_df, verify_integrity=False,
                                auto_generate_selection=True)
    expected_df = FlowfileTable([{"name": "edward"}])
    result_df.assert_equal(expected_df)


def test_join_anti():
    join_input = transform_schema.JoinInput(**get_join_settings('anti'))
    left_df = FlowfileTable([{"name": "eduward"},
                             {"name": "edward"},
                             {"name": "courtney"}])
    right_df = FlowfileTable([{"name": "edward"}])
    result_df = left_df.join(join_input=join_input, other=right_df, verify_integrity=False,
                                auto_generate_selection=True)
    expected_df = FlowfileTable([{"name": "eduward"},
                                 {"name": "courtney"}])
    result_df.assert_equal(expected_df)


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
