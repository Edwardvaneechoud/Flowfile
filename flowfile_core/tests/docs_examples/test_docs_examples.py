"""Execute every docs example end-to-end so the docs can never drift from the API.

Core examples (docs/examples/*.py) must always pass. Integration examples
(docs/examples/integrations/*.py) skip when their backing Docker service is
unavailable, reusing the same fixture helpers the rest of the suite gates on.
"""

import os
import re
import runpy
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
EXAMPLES_DIR = REPO_ROOT / "docs" / "examples"
INTEGRATIONS_DIR = EXAMPLES_DIR / "integrations"


def _core_examples() -> list[Path]:
    return sorted(EXAMPLES_DIR.glob("*.py"))


def _integration_examples() -> list[Path]:
    return sorted(INTEGRATIONS_DIR.glob("*.py"))


_REMOTE_URL_RE = re.compile(r"https://raw\.githubusercontent\.com/\S+?\.(?:csv|parquet|json)")


def _skip_unless_remote_data_available(example_path: Path) -> None:
    """User-facing examples read sample data by public URL so they run without a
    checkout. Those URLs only resolve once the data is merged to main — skip (never
    mock) when a referenced URL is unreachable, e.g. pre-merge or offline."""
    import urllib.error
    import urllib.request

    for url in set(_REMOTE_URL_RE.findall(example_path.read_text())):
        try:
            req = urllib.request.Request(url, method="HEAD")
            with urllib.request.urlopen(req, timeout=10) as resp:
                if resp.status != 200:
                    pytest.skip(f"remote sample data not available yet: {url} -> {resp.status}")
        except (urllib.error.URLError, TimeoutError) as exc:
            pytest.skip(f"remote sample data not reachable: {url} ({exc})")


@pytest.mark.parametrize("example_path", _core_examples(), ids=lambda p: p.stem)
def test_core_example_runs(example_path: Path, monkeypatch):
    """A core docs example runs cleanly with the repo root as CWD."""
    _skip_unless_remote_data_available(example_path)
    monkeypatch.chdir(REPO_ROOT)
    runpy.run_path(str(example_path), run_name="__main__")


def _postgres_available() -> bool:
    from test_utils.postgres import fixtures as pg_fixtures

    return pg_fixtures.can_connect_to_db()


def _minio_available() -> bool:
    from test_utils.s3 import fixtures as s3_fixtures

    if not s3_fixtures.is_docker_available():
        return False
    # start_minio_container is idempotent: True if already up, else brings it up.
    return s3_fixtures.start_minio_container()


def _kafka_available() -> bool:
    from test_utils.kafka import fixtures as kafka_fixtures

    if not kafka_fixtures.is_docker_available():
        return False
    # start_redpanda_container is idempotent: True if already up, else brings it up.
    return kafka_fixtures.start_redpanda_container()


INTEGRATION_GATES = {
    "database_read": _postgres_available,
    "database_read_duckdb": lambda: True,  # in-process, no backing service
    "database_transform_write": _postgres_available,
    "cloud_storage_s3": _minio_available,
    "kafka_read": _kafka_available,
}


def test_live_demo_deep_link_matches_sources():
    """The docs' try-in-browser redirect embeds the whole flow + CSV in its URL fragment
    (the wasm share-link format: deflate-raw + base64url JSON). Pin it to the tested
    sources so regenerating the dataset or changing the flow can't silently strand it."""
    import base64
    import json
    import re
    import zlib

    import yaml

    html = (REPO_ROOT / "docs" / "assets" / "try-sales-pipeline.html").read_text()
    blob = re.search(r"#flow=([A-Za-z0-9_-]+)", html).group(1)
    padded = blob.replace("-", "+").replace("_", "/")
    padded += "=" * (-len(padded) % 4)
    envelope = json.loads(zlib.decompress(base64.b64decode(padded), -15))

    assert envelope["v"] == 1
    assert "files" not in envelope  # data travels by public URL, not embedded
    read_settings = envelope["flow"]["nodes"][0]["setting_input"]["received_file"]
    from flowfile_core.templates.data_downloader import TEMPLATE_DATA_BASE_URL

    assert read_settings["path"] == f"{TEMPLATE_DATA_BASE_URL}/supermarket_sales.csv"

    template = yaml.safe_load(
        (REPO_ROOT / "data" / "templates" / "flows" / "sales_pipeline.yaml").read_text()
    )
    assert [n["type"] for n in envelope["flow"]["nodes"]] == [n["type"] for n in template["nodes"]]

    by_type = {n["type"]: n["setting_input"] for n in envelope["flow"]["nodes"]}
    aggs = {(c["old_name"], c["agg"], c["new_name"]) for c in by_type["group_by"]["groupby_input"]["agg_cols"]}
    assert aggs == {
        ("city", "groupby", "city"),
        ("gross_income", "sum", "total_income"),
        ("gross_income", "median", "median_income"),
    }
    basic = by_type["filter"]["filter_input"]["basic_filter"]
    assert (basic["field"], basic["operator"], basic["value"]) == ("quantity", "greater_than", "7")


def test_landing_flow_attachment_matches_source():
    """The site-served download is the tested template with its data path resolved to the
    public GitHub raw URL, so a downloaded flow opens and runs without any local setup.
    Regenerate: copy the template and substitute __TEMPLATE_DATA_DIR__ with the URL below."""
    from flowfile_core.templates.data_downloader import TEMPLATE_DATA_BASE_URL

    source = (REPO_ROOT / "data" / "templates" / "flows" / "sales_pipeline.yaml").read_text()
    attachment = (REPO_ROOT / "docs" / "assets" / "flows" / "sales_pipeline.yaml").read_text()
    assert attachment == source.replace("__TEMPLATE_DATA_DIR__", TEMPLATE_DATA_BASE_URL)


@pytest.mark.parametrize("example_path", _integration_examples(), ids=lambda p: p.stem)
def test_integration_example_runs(example_path: Path, monkeypatch):
    """An integration docs example runs when its backing service is reachable, else skips."""
    gate = INTEGRATION_GATES.get(example_path.stem)
    if gate is None:
        pytest.fail(f"No availability gate registered for integration example '{example_path.stem}'")
    if not gate():
        pytest.skip(f"Backing service for '{example_path.stem}' is not available")

    monkeypatch.chdir(REPO_ROOT)
    runpy.run_path(str(example_path), run_name="__main__")


def test_database_tutorial_flow_yaml_runs(monkeypatch):
    """The database tutorial's downloadable flow (docs/assets/flows/database_transform_write.yaml)
    opens and runs end-to-end against the Postgres fixture, so the visual flow shown in the docs
    can't drift from a working read -> enrich -> aggregate -> rank -> write pipeline. Skips when
    Postgres is unavailable, mirroring the integration-example gate."""
    if not _postgres_available():
        pytest.skip("Postgres fixture is not available")

    import flowfile as ff
    from flowfile_core.flowfile.manage.io_flowfile import open_flow

    # The flow reads/writes in reference mode, so the named connection must exist.
    ff.create_database_connection_if_not_exists(
        "analytics-postgres",
        database_type="postgresql",
        host=os.environ.get("DOCS_PG_HOST", "localhost"),
        port=int(os.environ.get("DOCS_PG_PORT", 5433)),
        database="testdb",
        username="testuser",
        password="testpass",
    )

    from flowfile_frame.database.connection_manager import get_current_user_id

    monkeypatch.chdir(REPO_ROOT)
    flow = open_flow(REPO_ROOT / "docs" / "assets" / "flows" / "database_transform_write.yaml")
    flow.execution_location = "local"
    # The flow file is user-agnostic (no user_id baked in); the app supplies the executing
    # user at open time. Headless, we stamp it so reference-mode connections resolve.
    current_uid = get_current_user_id()
    for node in flow.nodes:
        if getattr(node.setting_input, "user_id", "missing") is None:
            node.setting_input.user_id = current_uid
    result = flow.run_graph()
    assert result.success, "; ".join(
        f"node {s.node_id} ({s.node_name}): {s.error}" for s in result.node_step_result if not s.success and s.error
    )

    ranked = ff.read_database(
        "analytics-postgres", schema_name="public", table_name="language_profitability"
    ).collect()
    assert ranked.height == 5
    assert ranked["original_language"].to_list()[0] == "en"
