"""Tests for the four flowframe API quality-of-life improvements:

1. ``write_mode="virtual"`` Literal accepts the value (typing fix).
2. ``save_flow_to_catalog`` / ``FlowFrame.save_to_catalog`` register a flow
   so ``write_mode="virtual"`` works against it.
3. ``with_columns(flowfile_formulas=...)`` translates supported formulas
   into a polars-code node (instead of a formula node).
4. ``output_field_config`` flows through ``read_from_cloud_storage`` and
   ``FlowFrame.with_output_validation`` attaches a validator to any node;
   the flowframe code-generator emits ``.with_output_validation(...)``.
"""

from __future__ import annotations

import pytest

from flowfile_core.catalog import CatalogService, SQLAlchemyCatalogRepository
from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import (
    CatalogNamespace,
    CatalogTable,
    FlowFavorite,
    FlowFollow,
    FlowRegistration,
    FlowRun,
)
from flowfile_core.schemas import input_schema

import flowfile_frame as ff
from flowfile_frame import OutputFieldConfig, OutputFieldInfo, save_flow_to_catalog


def _cleanup_catalog() -> None:
    with get_db_context() as db:
        db.query(CatalogTable).delete()
        db.query(FlowFollow).delete()
        db.query(FlowFavorite).delete()
        db.query(FlowRun).delete()
        db.query(FlowRegistration).delete()
        db.query(CatalogNamespace).delete()
        db.commit()


@pytest.fixture(autouse=True)
def _clean_catalog():
    _cleanup_catalog()
    yield
    _cleanup_catalog()


def _seed_general_namespace() -> int:
    with get_db_context() as db:
        service = CatalogService(SQLAlchemyCatalogRepository(db))
        ns = service.create_namespace(name="General", owner_id=1, parent_id=None)
        return ns.id


# ---------------------------------------------------------------------------
# Issue 1 — WriteMode literal accepts "virtual"
# ---------------------------------------------------------------------------


def test_write_mode_virtual_in_literal():
    """The WriteMode alias / write_catalog_table Literal must accept 'virtual'."""
    from typing import get_args

    from flowfile_frame.catalog_reference import WriteMode

    assert "virtual" in get_args(WriteMode)


# ---------------------------------------------------------------------------
# Issue 2 — save_to_catalog
# ---------------------------------------------------------------------------


def test_save_to_catalog_creates_python_flows_namespace_on_first_use(tmp_path, monkeypatch):
    _seed_general_namespace()
    from shared import storage_config

    monkeypatch.setattr(storage_config.storage, "_base_dir", tmp_path)

    df = ff.from_dict({"a": [1, 2, 3]})
    reg_id = save_flow_to_catalog(df, "py_demo")

    assert reg_id is not None
    assert df.flow_graph._flow_settings.source_registration_id == reg_id

    with get_db_context() as db:
        service = CatalogService(SQLAlchemyCatalogRepository(db))
        general = service.repo.get_namespace_by_name("General", parent_id=None)
        assert general is not None
        py_flows = service.repo.get_namespace_by_name("Python Flows", parent_id=general.id)
        assert py_flows is not None
        # Registration is linked to the Python Flows namespace.
        reg = service.repo.get_flow(reg_id)
        assert reg is not None
        assert reg.namespace_id == py_flows.id
        assert reg.name == "py_demo"


def test_save_to_catalog_method_returns_self_and_is_idempotent():
    _seed_general_namespace()
    df = ff.from_dict({"a": [1, 2, 3]})

    returned = df.save_to_catalog("py_demo_idempotent")
    assert returned is df
    first_reg = df.flow_graph._flow_settings.source_registration_id
    assert first_reg is not None

    # Second save (same flow, same name) should not raise and should keep
    # the same registration id.
    df.save_to_catalog("py_demo_idempotent")
    assert df.flow_graph._flow_settings.source_registration_id == first_reg


def test_save_to_catalog_rejects_invalid_names():
    _seed_general_namespace()
    df = ff.from_dict({"a": [1, 2, 3]})

    for bad_name in ("../escape", "with/slash", "with space", "", "with.dot"):
        with pytest.raises(ValueError):
            save_flow_to_catalog(df, bad_name)


def test_save_to_catalog_raises_on_name_collision():
    _seed_general_namespace()

    df1 = ff.from_dict({"a": [1, 2, 3]})
    df2 = ff.from_dict({"a": [4, 5, 6]})

    save_flow_to_catalog(df1, "shared_name")
    with pytest.raises(ValueError, match="already exists"):
        save_flow_to_catalog(df2, "shared_name")


# ---------------------------------------------------------------------------
# Issue 3 — with_columns formula translation
# ---------------------------------------------------------------------------


def test_with_columns_translates_supported_formula_to_polars_code():
    df = ff.from_dict({"a": [1, 2, 3]})
    out = df.with_columns(flowfile_formulas=["[a]+1"], output_column_names=["b"])

    node = out.flow_graph.get_node(out.node_id)
    assert node.node_type == "polars_code", (
        f"expected polars_code node, got {node.node_type!r}"
    )

    result = out.collect()
    assert result["b"].to_list() == [2, 3, 4]


def test_with_columns_falls_back_for_unsupported_formula():
    df = ff.from_dict({"a": [1, 2, 3]})
    # string_similarity is a flowfile-only function not translatable to polars code.
    out = df.with_columns(
        flowfile_formulas=['string_similarity([a], "foo")'],
        output_column_names=["b"],
    )

    node = out.flow_graph.get_node(out.node_id)
    assert node.node_type == "formula"


def test_with_columns_all_or_nothing_translation():
    """If any formula in the list can't be translated, fall back for all."""
    df = ff.from_dict({"a": [1, 2, 3]})
    out = df.with_columns(
        flowfile_formulas=["[a]+1", 'string_similarity([a], "foo")'],
        output_column_names=["b", "c"],
    )

    # The last node should be a formula node (the fall-back path chains
    # _with_flowfile_formula calls).
    node = out.flow_graph.get_node(out.node_id)
    assert node.node_type == "formula"


# ---------------------------------------------------------------------------
# Issue 4 — output_field_config plumbing
# ---------------------------------------------------------------------------


def test_with_output_validation_attaches_config_to_current_node():
    df = ff.from_dict({"a": [1, 2, 3]})
    df = df.with_output_validation(
        fields=[{"name": "a", "data_type": "Int64"}],
        validation_mode_behavior="select_only",
        validate_data_types=True,
    )

    node = df.flow_graph.get_node(df.node_id)
    cfg = node.setting_input.output_field_config
    assert cfg is not None
    assert cfg.enabled is True
    assert cfg.validation_mode_behavior == "select_only"
    assert cfg.validate_data_types is True
    assert len(cfg.fields) == 1
    assert cfg.fields[0].name == "a"
    assert cfg.fields[0].data_type == "Int64"


def test_with_output_validation_accepts_typed_objects():
    df = ff.from_dict({"a": [1, 2, 3]})
    df = df.with_output_validation(
        fields=[OutputFieldInfo(name="a", data_type="Int64")],
        validation_mode_behavior="raise_on_missing",
    )

    cfg = df.flow_graph.get_node(df.node_id).setting_input.output_field_config
    assert cfg is not None
    assert cfg.validation_mode_behavior == "raise_on_missing"
    assert cfg.fields[0].name == "a"


def test_output_field_config_re_export_is_pydantic_model():
    """OutputFieldConfig and OutputFieldInfo should be importable from the
    flowfile_frame package surface."""
    assert OutputFieldConfig is input_schema.OutputFieldConfig
    assert OutputFieldInfo is input_schema.OutputFieldInfo


def test_code_generator_emits_with_output_validation():
    """Round-trip test: a flow with output_field_config on a cloud_storage_reader
    should generate flowframe code containing ``.with_output_validation(...)``."""
    from flowfile_core.flowfile.code_generator.code_generator import export_flow_to_flowframe
    from flowfile_core.flowfile.flow_graph import FlowGraph
    from flowfile_core.schemas import cloud_storage_schemas, schemas

    fg = FlowGraph(flow_settings=schemas.FlowSettings(flow_id=42, name="demo", path="demo"))
    settings = input_schema.NodeCloudStorageReader(
        flow_id=42,
        node_id=1,
        cloud_storage_settings=cloud_storage_schemas.CloudStorageReadSettings(
            resource_path="s3://bucket/path/",
            file_format="parquet",
            connection_name="conn",
            scan_mode="directory",
        ),
        user_id=1,
        output_field_config=input_schema.OutputFieldConfig(
            enabled=True,
            validation_mode_behavior="select_only",
            fields=[
                input_schema.OutputFieldInfo(name="vbMonth", data_type="String"),
                input_schema.OutputFieldInfo(name="contracts", data_type="Float64"),
            ],
            validate_data_types=True,
        ),
    )
    fg.add_cloud_storage_reader(settings)

    code = export_flow_to_flowframe(fg)
    assert ".with_output_validation(" in code
    assert '"name": "vbMonth"' in code
    assert '"data_type": "Float64"' in code
    assert 'validation_mode_behavior="select_only"' in code
    assert "validate_data_types=True" in code
