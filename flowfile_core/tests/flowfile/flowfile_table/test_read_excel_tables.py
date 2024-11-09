from flowfile_core.flowfile.flowfile_table.flowFilePolars import (FlowFileTable)
from flowfile_core.schemas.input_schema import ReceivedTable


def test_read_excel_table_basic():
    excel_table = ReceivedTable(path='backend/tests/data/excel_file.xlsx', name='excel_file', file_type='excel',
                                sheet_name='Sheet1')
    try:
        flowfile_table = FlowFileTable.create_from_path(excel_table)
    except:
        assert False, 'Failed to read excel table'


def test_read_excel_table_external():
    excel_table = ReceivedTable(path='//tests/data/excel_file.xlsx',
                                name='excel_file', file_type='excel',
                                sheet_name='Sheet1')
    try:
        flowfile_table = FlowFileTable.create_from_path_worker(excel_table)
        flowfile_table.collect()
    except:
        assert False, 'Failed to read excel table'


def test_read_excel_table_with_type_interference():
    excel_table = ReceivedTable(path='//tests/data/excel_file.xlsx',
                                name='excel_file', file_type='excel',
                                sheet_name='Sheet1', type_inference=True)
    try:
        flowfile_table = FlowFileTable.create_from_path(excel_table)
        flowfile_table.collect()
    except:
        assert False, 'Failed to read excel table'


def test_read_excel_starting_from_line10():
    received_table = ReceivedTable(path='//tests/data/excel_file.xlsx',
                                   name='excel_file', file_type='excel',
                                   sheet_name='Sheet1', type_inference=True, start_row=10)
    try:
        flowfile_table = FlowFileTable.create_from_path(received_table)
        flowfile_table.collect()
    except:
        assert False, 'Failed to read excel table'


def test_read_excel_starting_from_line10_no_headers():
    received_table = ReceivedTable(path='//tests/data/excel_file.xlsx',
                                   name='excel_file', file_type='excel',
                                   sheet_name='Sheet1', type_inference=True, start_row=10, has_headers=False)
    try:
        flowfile_table = FlowFileTable.create_from_path(received_table)
        flowfile_table.count()
    except:
        assert False, 'Failed to read excel table'


def test_read_excel_starting_line_10_no_type_interference():
    received_table = ReceivedTable(path='//tests/data/excel_file.xlsx',
                                   name='excel_file', file_type='excel',
                                   sheet_name='Sheet1', start_row=10)
    try:
        flowfile_table = FlowFileTable.create_from_path(received_table)
        flowfile_table.collect()
    except:
        assert False, 'Failed to read excel table'