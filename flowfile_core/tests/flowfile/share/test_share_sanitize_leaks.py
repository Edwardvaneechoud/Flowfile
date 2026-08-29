"""A share link must carry a flow, never an environment.

Every settings field that names the sender's machine, credentials store or
catalog carries a sentinel string here; none of them may appear anywhere in the
encoded envelope — including in the auto-generated node descriptions, which
embed settings verbatim ("Read from analytics.customer_pii").
"""

import json
import re
from pathlib import Path

import pytest

from flowfile_core.flowfile.manage.io_flowfile import open_flow
from flowfile_core.flowfile.share import build_share_link
from flowfile_core.flowfile.share.transform import build_share_envelope
from flowfile_core.schemas import transform_schema

from tests.flowfile.share.conftest import CANARIES, add_filter, add_read, make_graph

REPO_ROOT = Path(__file__).resolve().parents[4]
FLOW_YAMLS = sorted((REPO_ROOT / "data" / "templates" / "flows").glob("*.yaml")) + sorted(
    (REPO_ROOT / "docs" / "assets" / "flows").glob("*.yaml")
)

FORBIDDEN_KEYS = {
    "abs_file_path",
    "analysis_file_available",
    "catalog_namespace_id",
    "catalog_table_id",
    "connection_name",
    "database_connection_name",
    "directory",
    "ga_connection_name",
    "kafka_connection_id",
    "kafka_connection_name",
    "namespace_id",
    "source_registration_id",
    "user_id",
}

_ABSOLUTE_PATH_RE = re.compile(r"^(?:/|\\\\|[A-Za-z]:[\\/])")


def _walk(value, path="$"):
    yield path, value
    if isinstance(value, dict):
        for key, item in value.items():
            yield from _walk(item, f"{path}.{key}")
    elif isinstance(value, list):
        for index, item in enumerate(value):
            yield from _walk(item, f"{path}[{index}]")


def _assert_no_environment_leaks(envelope: dict) -> None:
    blob = json.dumps(envelope)
    assert "$ffsec$" not in blob, "an encrypted secret reference reached the share payload"
    for path, value in _walk(envelope):
        if isinstance(value, dict):
            leaked = FORBIDDEN_KEYS & set(value)
            assert not leaked, f"{path} still carries machine-local keys {sorted(leaked)}"
        if isinstance(value, str):
            assert not _ABSOLUTE_PATH_RE.match(value), f"{path} carries an absolute path: {value!r}"
            assert str(REPO_ROOT) not in value, f"{path} carries a path from this machine: {value!r}"


def test_canary_flow_leaks_nothing(canary_flow):
    envelope = build_share_envelope(canary_flow).envelope
    blob = json.dumps(envelope)
    leaked = sorted(name for name, sentinel in CANARIES.items() if sentinel in blob)
    assert not leaked, f"these settings reached the share payload: {leaked}"
    _assert_no_environment_leaks(envelope)


def test_canary_flow_placeholders_carry_no_description(canary_flow):
    """Auto-generated descriptions embed the settings, so placeholders drop them."""
    envelope = build_share_envelope(canary_flow).envelope
    for node in envelope["flow"]["nodes"]:
        if node["setting_input"].get("is_placeholder"):
            assert node["description"] == ""
            assert node["setting_input"]["description"] == ""


def test_round_tripped_auto_descriptions_do_not_leak(canary_flow):
    """A save/open round trip copies the auto-generated description into the
    user field (open_flow hydrates setting_input.description from the file's
    node-level description). It still must not travel: the transform compares
    against get_default_description() and suppresses matches."""
    for node in canary_flow.nodes:
        setting_input = node.setting_input
        if hasattr(setting_input, "get_default_description"):
            auto = setting_input.get_default_description()
            if auto:
                setting_input.description = auto

    envelope = build_share_envelope(canary_flow).envelope
    blob = json.dumps(envelope)
    leaked = sorted(name for name, sentinel in CANARIES.items() if sentinel in blob)
    assert not leaked, f"round-tripped auto-descriptions leaked: {leaked}"
    for node in envelope["flow"]["nodes"]:
        if node["setting_input"].get("is_placeholder"):
            assert node["description"] == ""
            assert node["setting_input"]["description"] == ""


def test_placeholder_keeps_a_user_written_description():
    graph = make_graph(name="described")
    add_read(graph, 1, "/tmp/data.csv")
    add_filter(
        graph,
        2,
        1,
        transform_schema.FilterInput(mode="advanced", advanced_filter="[a] > 1"),
    )
    graph.get_node(2).setting_input.description = "Only the interesting rows"

    envelope = build_share_envelope(graph).envelope
    placeholder = next(node for node in envelope["flow"]["nodes"] if node["id"] == 2)
    assert placeholder["description"] == "Only the interesting rows"
    assert placeholder["setting_input"]["description"] == "Only the interesting rows"
    assert "[a] > 1" not in json.dumps(envelope)


def test_local_read_path_is_reduced_to_a_basename():
    graph = make_graph(name="local_read")
    add_read(graph, 1, f"/home/someone/{CANARIES['local_dir']}/sales.csv")
    result = build_share_envelope(graph)

    received_file = result.envelope["flow"]["nodes"][0]["setting_input"]["received_file"]
    assert received_file["path"] == "sales.csv"
    assert result.local_file_nodes == [1]
    assert any("has to supply that file" in warning for warning in result.warnings)
    _assert_no_environment_leaks(result.envelope)


def test_remote_read_path_travels_verbatim():
    url = "https://raw.githubusercontent.com/edwardvaneechoud/flowfile/main/data/x.csv"
    graph = make_graph(name="remote_read")
    add_read(graph, 1, url)
    result = build_share_envelope(graph)

    assert result.envelope["flow"]["nodes"][0]["setting_input"]["received_file"]["path"] == url
    assert result.local_file_nodes == []


@pytest.mark.parametrize("flow_path", FLOW_YAMLS, ids=lambda p: f"{p.parent.name}/{p.stem}")
def test_shipped_flows_share_without_leaking(flow_path):
    """Every template and docs flow must mint a link, and none may leak its environment."""
    graph = open_flow(flow_path)
    envelope = build_share_envelope(graph).envelope
    _assert_no_environment_leaks(envelope)

    response = build_share_link(graph)
    assert response.url is not None
    assert response.hash_chars > 0
