from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
from flowfile_core.schemas.input_schema import InputExcelTable, ReceivedTable


def test_read_excel_table_basic():
    excel_table = ReceivedTable(
        path='flowfile_core/tests/support_files/data/excel_file.xlsx',
        name='excel_file',
        file_type='excel',
        table_settings=InputExcelTable(sheet_name='Sheet1')
    )
    try:
        flowfile_table = FlowDataEngine.create_from_path(excel_table)
    except Exception as e:
        assert False, e


def test_read_excel_table_external():
    excel_table = ReceivedTable(
        path='flowfile_core/tests/support_files/data/excel_file.xlsx',
        name='excel_file',
        file_type='excel',
        table_settings=InputExcelTable(sheet_name='Sheet1')
    )
    try:
        flowfile_table = FlowDataEngine.create_from_path_worker(excel_table, flow_id=-1, node_id=-1)
        flowfile_table.collect()
    except Exception as e:
        assert False, f'Failed to read excel table{e}'


def test_read_excel_table_with_type_interference():
    excel_table = ReceivedTable(
        path='flowfile_core/tests/support_files/data/excel_file.xlsx',
        name='excel_file',
        file_type='excel',
        table_settings=InputExcelTable(sheet_name='Sheet1', type_inference=True)
    )
    try:
        flowfile_table = FlowDataEngine.create_from_path(excel_table)
        flowfile_table.collect()
    except Exception as e:
        assert False, f'Failed to read excel table{e}'


def test_read_excel_starting_from_line10():
    received_table = ReceivedTable(
        path='flowfile_core/tests/support_files/data/excel_file.xlsx',
        name='excel_file',
        file_type='excel',
        table_settings=InputExcelTable(sheet_name='Sheet1', type_inference=True, start_row=10)
    )
    try:
        flowfile_table = FlowDataEngine.create_from_path(received_table)
        flowfile_table.collect()
    except Exception as e:
        assert False, f'Failed to read excel table{e}'


def test_read_excel_starting_from_line10_no_headers():
    received_table = ReceivedTable(
        path='flowfile_core/tests/support_files/data/excel_file.xlsx',
        name='excel_file',
        file_type='excel',
        table_settings=InputExcelTable(
            sheet_name='Sheet1',
            type_inference=True,
            start_row=10,
            has_headers=False
        )
    )
    try:
        flowfile_table = FlowDataEngine.create_from_path(received_table)
        flowfile_table.count()
    except Exception as e:
        assert False, f'Failed to read excel table{e}'


def test_read_excel_starting_line_10_no_type_interference():
    received_table = ReceivedTable(
        path='flowfile_core/tests/support_files/data/excel_file.xlsx',
        name='excel_file',
        file_type='excel',
        table_settings=InputExcelTable(sheet_name='Sheet1', start_row=10)
    )
    try:
        flowfile_table = FlowDataEngine.create_from_path(received_table)
        flowfile_table.collect()
    except:
        assert False, 'Failed to read excel table'
