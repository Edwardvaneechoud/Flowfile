"""Template flow definitions loaded from YAML files.

Templates are stored as YAML files in data/templates/flows/ in the repo.
Each YAML contains the flow definition plus embedded metadata (_template_meta)
and required CSV file list (_required_csv_files).

At instantiation time, the placeholder __TEMPLATE_DATA_DIR__ in read node paths
is replaced with the actual local template data directory.
"""

import logging
from pathlib import Path

import yaml

from flowfile_core.schemas.schemas import FlowfileData
from flowfile_core.templates.models import FlowTemplateMeta

logger = logging.getLogger(__name__)

TEMPLATE_PATH_PLACEHOLDER = "__TEMPLATE_DATA_DIR__"

# Flow YAML files are downloaded from GitHub to the same local cache as CSV data.
# They live in data/templates/flows/ in the repo.
_FLOW_YAML_FILENAMES = [
    "sales_data_overview.yaml",
    "customer_deduplication.yaml",
    "employee_directory_cleanup.yaml",
    "order_enrichment.yaml",
    "survey_results_pivot.yaml",
    "web_analytics_funnel.yaml",
    "customer_360.yaml",
    "product_fuzzy_match.yaml",
]


def _load_template_yaml(yaml_path: Path) -> tuple[FlowTemplateMeta, list[str], dict]:
    """Load a template YAML file and extract metadata, required files, and flow data."""
    with open(yaml_path, encoding="utf-8") as f:
        data = yaml.safe_load(f)

    if "_template_meta" not in data:
        raise ValueError(f"Missing '_template_meta' in {yaml_path.name}")
    if "_required_csv_files" not in data:
        raise ValueError(f"Missing '_required_csv_files' in {yaml_path.name}")

    meta = FlowTemplateMeta.model_validate(data.pop("_template_meta"))
    required_files = data.pop("_required_csv_files")

    if not isinstance(required_files, list):
        raise TypeError(f"'_required_csv_files' must be a list in {yaml_path.name}")

    return meta, required_files, data


def _replace_data_dir_placeholder(flow_dict: dict, data_dir: Path) -> dict:
    """Recursively replace __TEMPLATE_DATA_DIR__ placeholders with the actual path."""
    data_dir_str = str(data_dir)

    def _replace(obj):
        if isinstance(obj, str):
            return obj.replace(TEMPLATE_PATH_PLACEHOLDER, data_dir_str)
        if isinstance(obj, dict):
            return {k: _replace(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [_replace(item) for item in obj]
        return obj

    return _replace(flow_dict)


class _TemplateRegistry:
    """Lazy-loading registry that discovers templates from YAML files."""

    def __init__(self):
        self._loaded = False
        self._templates: dict[str, tuple[FlowTemplateMeta, list[str], Path]] = {}

    def _ensure_loaded(self):
        if self._loaded:
            return

        # Look for flow YAMLs in two locations:
        # 1. Repo checkout (development): data/templates/flows/
        # 2. Local cache (production): ~/.flowfile/template_data/flows/
        repo_flows_dir = Path(__file__).resolve().parent.parent.parent.parent / "data" / "templates" / "flows"
        cache_flows_dir = None
        try:
            from shared.storage_config import storage

            cache_flows_dir = storage.template_data_directory / "flows"
        except Exception:
            pass

        flows_dir = None
        if repo_flows_dir.exists() and any(repo_flows_dir.glob("*.yaml")):
            flows_dir = repo_flows_dir
        elif cache_flows_dir and cache_flows_dir.exists() and any(cache_flows_dir.glob("*.yaml")):
            flows_dir = cache_flows_dir

        if flows_dir is None:
            logger.warning("No template flow YAMLs found in repo or cache")
            self._loaded = True
            return

        for yaml_file in sorted(flows_dir.glob("*.yaml")):
            try:
                meta, required_files, _ = _load_template_yaml(yaml_file)
                self._templates[meta.template_id] = (meta, required_files, yaml_file)
            except Exception:
                logger.exception("Failed to load template: %s", yaml_file)

        self._loaded = True

    def get_all_metas(self) -> list[FlowTemplateMeta]:
        self._ensure_loaded()
        return [meta for meta, _, _ in self._templates.values()]

    def get_required_files(self, template_id: str) -> list[str]:
        self._ensure_loaded()
        entry = self._templates.get(template_id)
        if entry is None:
            raise ValueError(f"Unknown template: {template_id}")
        return entry[1]

    def get_flowfile_data(self, template_id: str, data_dir: Path) -> FlowfileData:
        self._ensure_loaded()
        entry = self._templates.get(template_id)
        if entry is None:
            raise ValueError(f"Unknown template: {template_id}")
        _, _, yaml_path = entry
        _, _, flow_dict = _load_template_yaml(yaml_path)
        resolved = _replace_data_dir_placeholder(flow_dict, data_dir)
        return FlowfileData.model_validate(resolved)

    def get_yaml_filenames(self) -> list[str]:
        """Returns the list of flow YAML filenames to download."""
        return list(_FLOW_YAML_FILENAMES)


_registry = _TemplateRegistry()


def get_all_template_metas() -> list[FlowTemplateMeta]:
    """Returns metadata for all available templates."""
    return _registry.get_all_metas()


def get_template_required_files(template_id: str) -> list[str]:
    """Returns the list of CSV filenames required by a template."""
    return _registry.get_required_files(template_id)


def get_template_flowfile_data(template_id: str, data_dir: Path) -> FlowfileData:
    """Builds and returns the FlowfileData for a template with resolved paths."""
    return _registry.get_flowfile_data(template_id, data_dir)


def get_flow_yaml_filenames() -> list[str]:
    """Returns the list of flow YAML filenames that should be available."""
    return _registry.get_yaml_filenames()
