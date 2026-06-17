from pathlib import Path

from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
from flowfile_core.schemas.input_schema import InputExcelTable, ReceivedTable

SUPPORT_FILES = Path(__file__).resolve().parents[2] / "support_files" / "data"


def test_read_excel_table_basic():
    excel_table = ReceivedTable(
        path=str(SUPPORT_FILES / "excel_file.xlsx"),
        name="excel_file",
        file_type="excel",
        table_settings=InputExcelTable(sheet_name="Sheet1"),
    )
    try:
        flowfile_table = FlowDataEngine.create_from_path(excel_table)
    except Exception as e:
        assert False, e


def test_read_excel_table_external():
    excel_table = ReceivedTable(
        path=str(SUPPORT_FILES / "excel_file.xlsx"),
        name="excel_file",
        file_type="excel",
        table_settings=InputExcelTable(sheet_name="Sheet1"),
    )
    try:
        flowfile_table = FlowDataEngine.create_from_path_worker(excel_table, flow_id=-1, node_id=-1)
        flowfile_table.collect()
    except Exception as e:
        assert False, f"Failed to read excel table{e}"


def test_read_excel_table_with_type_interference():
    excel_table = ReceivedTable(
        path=str(SUPPORT_FILES / "excel_file.xlsx"),
        name="excel_file",
        file_type="excel",
        table_settings=InputExcelTable(sheet_name="Sheet1", type_inference=True),
    )
    try:
        flowfile_table = FlowDataEngine.create_from_path(excel_table)
        flowfile_table.collect()
    except Exception as e:
        assert False, f"Failed to read excel table{e}"


def test_read_excel_starting_from_line10():
    received_table = ReceivedTable(
        path=str(SUPPORT_FILES / "excel_file.xlsx"),
        name="excel_file",
        file_type="excel",
        table_settings=InputExcelTable(sheet_name="Sheet1", type_inference=True, start_row=10),
    )
    try:
        flowfile_table = FlowDataEngine.create_from_path(received_table)
        flowfile_table.collect()
    except Exception as e:
        assert False, f"Failed to read excel table{e}"


def test_read_excel_starting_from_line10_no_headers():
    received_table = ReceivedTable(
        path=str(SUPPORT_FILES / "excel_file.xlsx"),
        name="excel_file",
        file_type="excel",
        table_settings=InputExcelTable(sheet_name="Sheet1", type_inference=True, start_row=10, has_headers=False),
    )
    try:
        flowfile_table = FlowDataEngine.create_from_path(received_table)
        flowfile_table.count()
    except Exception as e:
        assert False, f"Failed to read excel table{e}"


def test_read_excel_starting_line_10_no_type_interference():
    received_table = ReceivedTable(
        path=str(SUPPORT_FILES / "excel_file.xlsx"),
        name="excel_file",
        file_type="excel",
        table_settings=InputExcelTable(sheet_name="Sheet1", start_row=10),
    )
    try:
        flowfile_table = FlowDataEngine.create_from_path(received_table)
        flowfile_table.collect()
        assert flowfile_table.columns == ['.38 Special',
                                          'Caught Up in You',
                                          'KGLK',
                                          'KGLK1446',
                                          '.38 Special_v2',
                                          'Caught Up in You by .38 Special',
                                          '0',
                                          '1402970932',
                                          'Caught Up In You']
    except:
        assert False, "Failed to read excel table"


def test_read_excel_file_second_row_date_type():
    received_table = ReceivedTable(
        path=str(SUPPORT_FILES / "excel_file_issue_356.xlsx"),
        name="excel_file",
        file_type="excel",
        table_settings=InputExcelTable(sheet_name="Sheet1", start_row=1),
    )
    flowfile_table = FlowDataEngine.create_from_path(received_table)
    assert "2025-06-01 00:00:00" in flowfile_table.columns, f"Expected '2025-06-01 00:00:00' in columns but got {flowfile_table.columns}"


def test_read_excel_file_second_row_date_type_no_headers():
    received_table = ReceivedTable(
        path=str(SUPPORT_FILES / "excel_file_issue_356.xlsx"),
        name="excel_file",
        file_type="excel",
        table_settings=InputExcelTable(sheet_name="Sheet1", start_row=1, has_headers=False),
    )
    flowfile_table = FlowDataEngine.create_from_path(received_table)

    assert all(column_name.startswith("column_") for column_name in flowfile_table.columns)


