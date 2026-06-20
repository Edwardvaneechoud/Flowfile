"""Pure determinism + resolver tests — no DB, no app state."""

import yaml

from flowfile_core.project.normalize import (
    deterministic_flow_id,
    dump_yaml,
    normalize_flow_data,
    safe_stem,
)
from flowfile_core.project.secrets_resolver import (
    env_key,
    load_dotenv,
    make_placeholder,
    placeholder_name,
    resolve,
)


def _flow_data(flow_id: int) -> dict:
    return {
        "flowfile_version": "0.12.1",
        "flowfile_id": flow_id,
        "flowfile_name": "sales",
        "flowfile_settings": {
            "execution_mode": "Performance",
            "source_registration_id": 99,
            "parameters": [],
        },
        "nodes": [],
        "groups": [],
    }


def test_deterministic_flow_id_is_stable_and_positive():
    a = deterministic_flow_id("b3f1-uuid")
    assert a == deterministic_flow_id("b3f1-uuid")
    assert a != deterministic_flow_id("other-uuid")
    assert 0 <= a <= 0x7FFFFFFFFFFFFFFF


def test_normalize_strips_volatile_fields():
    nd = normalize_flow_data(_flow_data(111111), "b3f1-uuid", "Daily FX Sync")
    assert nd["flow_uuid"] == "b3f1-uuid"
    assert nd["catalog_name"] == "Daily FX Sync"
    assert nd["flowfile_id"] == deterministic_flow_id("b3f1-uuid")
    assert nd["flowfile_settings"]["source_registration_id"] is None


def test_normalize_is_idempotent_regardless_of_input_id():
    # Two different volatile ids must normalize to byte-identical YAML.
    a = dump_yaml(normalize_flow_data(_flow_data(111111), "u", "Daily FX Sync"))
    b = dump_yaml(normalize_flow_data(_flow_data(999999), "u", "Daily FX Sync"))
    assert a == b
    # Re-normalizing the output changes nothing.
    once = normalize_flow_data(_flow_data(1), "u", "Daily FX Sync")
    twice = normalize_flow_data(once, "u", "Daily FX Sync")
    assert dump_yaml(once) == dump_yaml(twice)


def test_dump_yaml_preserves_key_order():
    text = dump_yaml({"b": 1, "a": 2, "c": 3})
    assert text.index("b:") < text.index("a:") < text.index("c:")


def test_safe_stem():
    assert safe_stem("Sales / Pipeline!") == "Sales_Pipeline"
    assert safe_stem("") == "flow"
    assert safe_stem(None) == "flow"


def test_secret_placeholder_roundtrip():
    assert placeholder_name(make_placeholder("prod_pw")) == "prod_pw"
    assert placeholder_name("not a placeholder") is None
    assert placeholder_name(None) is None


def test_env_key_uppercases_and_sanitizes():
    assert env_key("prod_postgres") == "FLOWFILE_SECRET_PROD_POSTGRES"
    assert env_key("s3 lake-key") == "FLOWFILE_SECRET_S3_LAKE_KEY"


def test_resolve_env_wins_over_dotenv(monkeypatch):
    monkeypatch.setenv("FLOWFILE_SECRET_X", "from_env")
    assert resolve("x", {"FLOWFILE_SECRET_X": "from_dotenv"}) == "from_env"
    monkeypatch.delenv("FLOWFILE_SECRET_X", raising=False)
    assert resolve("x", {"FLOWFILE_SECRET_X": "from_dotenv"}) == "from_dotenv"
    assert resolve("x", {}) is None


def test_load_dotenv(tmp_path):
    (tmp_path / ".env").write_text('FLOWFILE_SECRET_A=val1\n# comment\nFLOWFILE_SECRET_B="quoted"\n', encoding="utf-8")
    env = load_dotenv(tmp_path)
    assert env["FLOWFILE_SECRET_A"] == "val1"
    assert env["FLOWFILE_SECRET_B"] == "quoted"


def test_normalize_output_loads_back_as_yaml():
    # The projected file must remain parseable YAML (FlowfileData ignores the extra keys).
    nd = normalize_flow_data(_flow_data(5), "u", "Daily FX Sync")
    reloaded = yaml.safe_load(dump_yaml(nd))
    assert reloaded["flowfile_id"] == deterministic_flow_id("u")
    assert "flow_uuid" in reloaded
    assert reloaded["catalog_name"] == "Daily FX Sync"
