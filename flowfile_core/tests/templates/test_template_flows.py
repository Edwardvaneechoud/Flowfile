"""Tests that verify all template flows in data/templates/flows/ load and run correctly."""

from pathlib import Path

import yaml

from flowfile_core.flowfile.manage.io_flowfile import open_flow
from flowfile_core.schemas.schemas import FlowfileData
from flowfile_core.templates.models import FlowTemplateMeta
from flowfile_core.templates.template_definitions import (
    _load_template_yaml,
    _replace_data_dir_placeholder,
)


class TestTemplateYamlValidity:
    """Verify that every template YAML is structurally valid."""

    def test_yaml_parses(self, template_yaml_path: Path):
        """Template YAML can be parsed without errors."""
        with open(template_yaml_path, encoding="utf-8") as f:
            data = yaml.safe_load(f)
        assert isinstance(data, dict)

    def test_has_required_meta_fields(self, template_yaml_path: Path):
        """Template contains _template_meta and _required_csv_files."""
        with open(template_yaml_path, encoding="utf-8") as f:
            data = yaml.safe_load(f)
        assert "_template_meta" in data, f"Missing _template_meta in {template_yaml_path.name}"
        assert "_required_csv_files" in data, f"Missing _required_csv_files in {template_yaml_path.name}"

    def test_meta_validates(self, template_yaml_path: Path):
        """_template_meta validates against FlowTemplateMeta model."""
        meta, required_files, flow_dict = _load_template_yaml(template_yaml_path)
        assert isinstance(meta, FlowTemplateMeta)
        assert meta.template_id
        assert meta.name
        assert meta.category in ("Beginner", "Intermediate", "Advanced")
        assert meta.node_count > 0

    def test_required_csv_files_exist(self, template_yaml_path: Path, template_data_dir: Path):
        """All CSV files listed in _required_csv_files exist in data/templates/."""
        _, required_files, _ = _load_template_yaml(template_yaml_path)
        for csv_file in required_files:
            csv_path = template_data_dir / csv_file
            assert csv_path.exists(), f"Required CSV '{csv_file}' not found at {csv_path}"

    def test_node_count_matches_meta(self, template_yaml_path: Path):
        """node_count in metadata matches actual number of nodes."""
        meta, _, flow_dict = _load_template_yaml(template_yaml_path)
        actual_nodes = len(flow_dict.get("nodes", []))
        assert actual_nodes == meta.node_count, (
            f"{template_yaml_path.name}: meta says {meta.node_count} nodes " f"but YAML contains {actual_nodes}"
        )

    def test_flowfile_data_validates(self, template_yaml_path: Path, template_data_dir: Path):
        """Flow dict with resolved paths validates against FlowfileData schema."""
        _, _, flow_dict = _load_template_yaml(template_yaml_path)
        resolved = _replace_data_dir_placeholder(flow_dict, template_data_dir)
        flowfile_data = FlowfileData.model_validate(resolved)
        assert flowfile_data.flowfile_name
        assert len(flowfile_data.nodes) > 0


class TestTemplateFlowExecution:
    """Verify that every template flow can be loaded and executed end-to-end."""

    def test_open_flow_from_template(self, template_yaml_path: Path, template_data_dir: Path, tmp_path: Path):
        """Template can be opened as a FlowGraph via the standard import path."""
        _, _, flow_dict = _load_template_yaml(template_yaml_path)
        resolved = _replace_data_dir_placeholder(flow_dict, template_data_dir)
        flowfile_data = FlowfileData.model_validate(resolved)

        # Write resolved flow to a temp YAML and open it
        temp_yaml = tmp_path / f"{template_yaml_path.stem}.yaml"
        with open(temp_yaml, "w", encoding="utf-8") as f:
            yaml.dump(flowfile_data.model_dump(), f, default_flow_style=False, allow_unicode=True)

        flow = open_flow(temp_yaml)
        assert flow is not None
        assert len(flow.nodes) > 0

    def test_run_template_flow(self, template_yaml_path: Path, template_data_dir: Path, tmp_path: Path):
        """Template flow runs successfully with sample data."""
        _, _, flow_dict = _load_template_yaml(template_yaml_path)
        resolved = _replace_data_dir_placeholder(flow_dict, template_data_dir)
        flowfile_data = FlowfileData.model_validate(resolved)

        temp_yaml = tmp_path / f"{template_yaml_path.stem}.yaml"
        with open(temp_yaml, "w", encoding="utf-8") as f:
            yaml.dump(flowfile_data.model_dump(), f, default_flow_style=False, allow_unicode=True)

        flow = open_flow(temp_yaml)
        flow.execution_location = "local"

        result = flow.run_graph()

        assert result is not None, f"run_graph() returned None for {template_yaml_path.name}"
        assert result.success, f"Flow {template_yaml_path.name} failed: " + "; ".join(
            f"node {s.node_id} ({s.node_name}): {s.error}" for s in result.node_step_result if not s.success and s.error
        )
        assert result.nodes_completed > 0
