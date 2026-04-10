from pathlib import Path

import pytest

TEMPLATE_FLOWS_DIR = Path(__file__).resolve().parent.parent.parent.parent / "data" / "templates" / "flows"
TEMPLATE_DATA_DIR = Path(__file__).resolve().parent.parent.parent.parent / "data" / "templates"


def get_template_yaml_files() -> list[Path]:
    """Collect all template YAML files for parametrization."""
    return sorted(TEMPLATE_FLOWS_DIR.glob("*.yaml"))


@pytest.fixture(params=get_template_yaml_files(), ids=lambda p: p.stem)
def template_yaml_path(request) -> Path:
    """Parametrized fixture that yields each template YAML path."""
    return request.param


@pytest.fixture
def template_data_dir() -> Path:
    return TEMPLATE_DATA_DIR
