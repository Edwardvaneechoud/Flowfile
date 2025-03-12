from flowfile_core.flowfile.flowfile_table.flowfile_table import FlowfileTable
from flowfile_core.flowfile.flowfile_table.fuzzy_matching.prepare_for_fuzzy_match import prepare_for_fuzzy_match
from flowfile_core.schemas import transform_schema


def test_prepare_for_fuzzy_match():
    r = transform_schema.SelectInputs([transform_schema.SelectInput(old_name='column_0', new_name='name')])
    left_flowfile_table = FlowfileTable(['edward', 'eduward', 'court']).do_select(r)
    right_flowfile_table = left_flowfile_table
    left_flowfile_table.calculate_schema()
    left_select = [transform_schema.SelectInput(c) for c in left_flowfile_table.columns]
    right_select = [transform_schema.SelectInput(c) for c in right_flowfile_table.columns]
    fuzzy_match_input = transform_schema.FuzzyMatchInput(join_mapping=[transform_schema.FuzzyMap(left_col='name')],
                                                         left_select=left_select, right_select=right_select
                                                         )
    f = prepare_for_fuzzy_match(left_flowfile_table, right_flowfile_table, fuzzy_match_input)
    assert f[0].columns == ['name'], 'Left column should still be named name'
    assert f[1].columns == ['right_name'], 'Right column should be renamed to right_name'

    assert fuzzy_match_input.join_mappings[0].left_col == 'name', 'Left column should still be named name'
    assert fuzzy_match_input.join_mappings[0].right_col == 'right_name', 'Right column should be renamed to right_name'

