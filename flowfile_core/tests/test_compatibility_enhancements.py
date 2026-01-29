"""Tests for flowfile/manage/compatibility_enhancements module."""

from dataclasses import dataclass, fields
from unittest.mock import MagicMock

import pytest

from flowfile_core.flowfile.manage.compatibility_enhancements import (
    _is_dataclass_instance,
    _migrate_dataclass_to_basemodel,
    _build_input_table_settings,
    _build_output_table_settings,
    ensure_description,
    ensure_compatibility_node_read,
    ensure_compatibility_node_output,
    ensure_flow_settings,
)
from flowfile_core.schemas import input_schema, schemas


class TestIsDataclassInstance:
    """Test _is_dataclass_instance function."""

    def test_dataclass_instance(self):
        @dataclass
        class MyDC:
            x: int = 1

        obj = MyDC()
        assert _is_dataclass_instance(obj) is True

    def test_pydantic_model(self):
        from pydantic import BaseModel

        class MyModel(BaseModel):
            x: int = 1

        obj = MyModel()
        assert _is_dataclass_instance(obj) is False

    def test_regular_object(self):
        class MyClass:
            pass

        obj = MyClass()
        assert _is_dataclass_instance(obj) is False

    def test_none(self):
        assert _is_dataclass_instance(None) is False

    def test_dict(self):
        assert _is_dataclass_instance({"key": "value"}) is False


class TestMigrateDataclassToBasemodel:
    """Test _migrate_dataclass_to_basemodel function."""

    def test_none_returns_none(self):
        from pydantic import BaseModel

        class M(BaseModel):
            pass

        assert _migrate_dataclass_to_basemodel(None, M) is None

    def test_non_dataclass_returns_as_is(self):
        from pydantic import BaseModel

        class M(BaseModel):
            x: int = 1

        obj = M(x=5)
        result = _migrate_dataclass_to_basemodel(obj, M)
        assert result is obj

    def test_migrate_dataclass(self):
        from pydantic import BaseModel

        @dataclass
        class OldDC:
            name: str = "test"
            value: int = 42

        class NewModel(BaseModel):
            name: str = ""
            value: int = 0

        old = OldDC(name="hello", value=99)
        result = _migrate_dataclass_to_basemodel(old, NewModel)
        assert isinstance(result, NewModel)
        assert result.name == "hello"
        assert result.value == 99


class TestBuildInputTableSettings:
    """Test _build_input_table_settings function."""

    def test_csv_settings(self):
        received_file = MagicMock()
        received_file.reference = "test_ref"
        received_file.starting_from_line = 0
        received_file.delimiter = ","
        received_file.has_headers = True
        received_file.encoding = "utf-8"
        received_file.parquet_ref = None
        received_file.row_delimiter = "\n"
        received_file.quote_char = '"'
        received_file.infer_schema_length = 10_000
        received_file.truncate_ragged_lines = False
        received_file.ignore_errors = False

        result = _build_input_table_settings(received_file, "csv")
        assert result["file_type"] == "csv"
        assert result["delimiter"] == ","
        assert result["encoding"] == "utf-8"
        assert result["has_headers"] is True

    def test_json_settings(self):
        received_file = MagicMock()
        received_file.reference = ""
        received_file.starting_from_line = 0
        received_file.delimiter = ","
        received_file.has_headers = True
        received_file.encoding = "utf-8"
        received_file.parquet_ref = None
        received_file.row_delimiter = "\n"
        received_file.quote_char = '"'
        received_file.infer_schema_length = 10_000
        received_file.truncate_ragged_lines = False
        received_file.ignore_errors = False

        result = _build_input_table_settings(received_file, "json")
        assert result["file_type"] == "json"

    def test_parquet_settings(self):
        received_file = MagicMock()
        result = _build_input_table_settings(received_file, "parquet")
        assert result == {"file_type": "parquet"}

    def test_excel_settings(self):
        received_file = MagicMock()
        received_file.sheet_name = "Sheet1"
        received_file.start_row = 0
        received_file.start_column = 0
        received_file.end_row = 0
        received_file.end_column = 0
        received_file.has_headers = True
        received_file.type_inference = False

        result = _build_input_table_settings(received_file, "excel")
        assert result["file_type"] == "excel"
        assert result["sheet_name"] == "Sheet1"

    def test_unknown_type_defaults_csv(self):
        received_file = MagicMock()
        result = _build_input_table_settings(received_file, "unknown")
        assert result["file_type"] == "csv"


class TestBuildOutputTableSettings:
    """Test _build_output_table_settings function."""

    def test_csv_with_old_csv_table(self):
        output_settings = MagicMock()
        old_csv = MagicMock()
        old_csv.delimiter = ";"
        old_csv.encoding = "latin-1"
        output_settings.output_csv_table = old_csv

        result = _build_output_table_settings(output_settings, "csv")
        assert result["file_type"] == "csv"
        assert result["delimiter"] == ";"
        assert result["encoding"] == "latin-1"

    def test_csv_without_old_csv_table(self):
        output_settings = MagicMock()
        output_settings.output_csv_table = None

        result = _build_output_table_settings(output_settings, "csv")
        assert result["file_type"] == "csv"
        assert result["delimiter"] == ","

    def test_parquet(self):
        output_settings = MagicMock()
        result = _build_output_table_settings(output_settings, "parquet")
        assert result == {"file_type": "parquet"}

    def test_excel_with_old_excel_table(self):
        output_settings = MagicMock()
        old_excel = MagicMock()
        old_excel.sheet_name = "Data"
        output_settings.output_excel_table = old_excel

        result = _build_output_table_settings(output_settings, "excel")
        assert result["file_type"] == "excel"
        assert result["sheet_name"] == "Data"

    def test_excel_without_old_excel_table(self):
        output_settings = MagicMock()
        output_settings.output_excel_table = None

        result = _build_output_table_settings(output_settings, "excel")
        assert result["file_type"] == "excel"
        assert result["sheet_name"] == "Sheet1"

    def test_unknown_type_defaults_csv(self):
        output_settings = MagicMock()
        result = _build_output_table_settings(output_settings, "xml")
        assert result["file_type"] == "csv"


class TestEnsureDescription:
    """Test ensure_description function."""

    def test_adds_description(self):
        class NodeLike:
            pass

        node = NodeLike()
        ensure_description(node)
        assert hasattr(node, "description")
        assert node.description == ""

    def test_preserves_existing_description(self):
        class NodeLike:
            def __init__(self):
                self.description = "existing"

        node = NodeLike()
        ensure_description(node)
        assert node.description == "existing"


class TestEnsureFlowSettings:
    """Test ensure_flow_settings function."""

    def test_creates_flow_settings_when_missing(self):
        flow_obj = MagicMock(spec=schemas.FlowInformation)
        flow_obj.flow_id = 1
        flow_obj.flow_name = "test_flow"
        flow_obj.flow_settings = None
        # Mock model_validate to return the flow_obj itself
        schemas.FlowInformation.model_validate = MagicMock(return_value=flow_obj)

        result = ensure_flow_settings(flow_obj, "/path/to/flow")
        assert result is not None

    def test_updates_auto_execution_location(self):
        class FakeFlowSettings:
            execution_location = "auto"
            is_running = False
            is_canceled = False
            show_detailed_progress = True
            track_history = True
            max_parallel_workers = 4

        class FakeFlowObj:
            flow_settings = FakeFlowSettings()

        flow_obj = FakeFlowObj()
        result = ensure_flow_settings(flow_obj, "/path")
        assert flow_obj.flow_settings.execution_location == "remote"
