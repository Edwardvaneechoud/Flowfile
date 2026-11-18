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


def _no_lookup(kind, settings):
    raise AssertionError("a flow without node references needs no lookup")


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
    assert 0 < a <= 0x1FFFFFFFFFFFFF


def test_deterministic_flow_id_stays_js_safe():
    # Must never exceed Number.MAX_SAFE_INTEGER, or the frontend rounds it and loses the flow.
    for uuid in ("b3f1-uuid", "other-uuid", "3390df79-65b2-40eb-b592-16c9191c1b17", "0", "z" * 64):
        assert deterministic_flow_id(uuid) <= 9007199254740991


def test_normalize_strips_volatile_fields():
    nd = normalize_flow_data(_flow_data(111111), "b3f1-uuid", "Daily FX Sync", lookup=_no_lookup)
    assert nd["flow_uuid"] == "b3f1-uuid"
    assert nd["catalog_name"] == "Daily FX Sync"
    assert nd["flowfile_id"] == deterministic_flow_id("b3f1-uuid")
    assert nd["flowfile_settings"]["source_registration_id"] is None


def test_normalize_is_idempotent_regardless_of_input_id():
    # Two different volatile ids must normalize to byte-identical YAML.
    a = dump_yaml(normalize_flow_data(_flow_data(111111), "u", "Daily FX Sync", lookup=_no_lookup))
    b = dump_yaml(normalize_flow_data(_flow_data(999999), "u", "Daily FX Sync", lookup=_no_lookup))
    assert a == b
    # Re-normalizing the output changes nothing.
    once = normalize_flow_data(_flow_data(1), "u", "Daily FX Sync", lookup=_no_lookup)
    twice = normalize_flow_data(once, "u", "Daily FX Sync", lookup=_no_lookup)
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
    nd = normalize_flow_data(_flow_data(5), "u", "Daily FX Sync", lookup=_no_lookup)
    reloaded = yaml.safe_load(dump_yaml(nd))
    assert reloaded["flowfile_id"] == deterministic_flow_id("u")
    assert "flow_uuid" in reloaded
    assert reloaded["catalog_name"] == "Daily FX Sync"


_RESOLVING_NAMESPACES = {"Cat.Sch", "Kept.name"}

_NAMES = {
    ("table", 7): {"catalog_full_table_name": "Cat.Sch.orders"},
    ("table", ("orders", 3)): {"catalog_full_table_name": "Cat.Sch.orders"},
    ("namespace", 3): {"namespace_full_name": "Cat.Sch"},
    ("flow", 12): {"flow_uuid": "child-uuid", "namespace": "Cat.Sch", "name": "child"},
}


def _dict_lookup(kind, settings):
    if kind == "table":
        by_name = (settings.get("catalog_table_name"), settings.get("catalog_namespace_id"))
        key = settings.get("catalog_table_id") or by_name
    elif kind == "namespace":
        if settings.get("namespace_full_name") in _RESOLVING_NAMESPACES:
            return {"namespace_full_name": settings["namespace_full_name"]}
        key = settings.get("namespace_id")
    else:
        if settings.get("flow_uuid"):
            return None
        key = settings.get("registration_id")
    return _NAMES.get((kind, key))


def _with_nodes(*nodes) -> dict:
    data = _flow_data(1)
    data["nodes"] = [{"id": i + 1, "type": t, "setting_input": s} for i, (t, s) in enumerate(nodes)]
    return data


def _settings(nd: dict, index: int, key: str | None = None) -> dict:
    s = nd["nodes"][index]["setting_input"]
    return s[key] if key else s


def _normalize(data: dict) -> dict:
    return normalize_flow_data(data, "u", "Daily FX Sync", lookup=_dict_lookup)


def test_reader_id_is_replaced_by_its_table_name():
    nd = _normalize(_with_nodes(("catalog_reader", {"catalog_table_id": 7, "catalog_namespace_id": 3})))
    s = _settings(nd, 0)
    assert s["catalog_full_table_name"] == "Cat.Sch.orders"
    assert s["catalog_table_id"] is None and s["catalog_namespace_id"] is None


def test_reader_id_wins_over_a_stored_name():
    # The run time reads the reader's id first, so the id's table is the one named.
    nd = _normalize(_with_nodes(("catalog_reader", {"catalog_table_id": 7, "catalog_full_table_name": "Old.other"})))
    assert _settings(nd, 0)["catalog_full_table_name"] == "Cat.Sch.orders"


def test_reader_name_in_a_namespace_id_gets_the_full_name():
    nd = _normalize(_with_nodes(("catalog_reader", {"catalog_table_name": "orders", "catalog_namespace_id": 3})))
    s = _settings(nd, 0)
    assert s["catalog_full_table_name"] == "Cat.Sch.orders"
    assert s["catalog_table_name"] == "orders" and s["catalog_namespace_id"] is None


def test_writer_and_models_keep_a_stored_name_that_resolves_and_drop_the_id():
    nd = _normalize(
        _with_nodes(
            ("catalog_writer", {"catalog_write_settings": {"namespace_id": 3, "namespace_full_name": "Kept.name"}}),
            ("train_model", {"train_input": {"namespace_id": 3}}),
            ("apply_model", {"apply_input": {"namespace_id": 3, "namespace_full_name": "Kept.name"}}),
        )
    )
    writer = _settings(nd, 0, "catalog_write_settings")
    assert writer["namespace_full_name"] == "Kept.name" and writer["namespace_id"] is None
    train = _settings(nd, 1, "train_input")
    assert train["namespace_full_name"] == "Cat.Sch" and train["namespace_id"] is None
    apply = _settings(nd, 2, "apply_input")
    assert apply["namespace_full_name"] == "Kept.name" and apply["namespace_id"] is None


def test_writer_and_models_name_the_id_when_the_stored_name_resolves_to_nothing():
    # The run time falls back to the id when the name resolves to nothing, so the id's namespace is written.
    nd = _normalize(
        _with_nodes(
            ("catalog_writer", {"catalog_write_settings": {"namespace_id": 3, "namespace_full_name": "Gone.name"}}),
            ("train_model", {"train_input": {"namespace_id": 3, "namespace_full_name": "Gone.name"}}),
            ("apply_model", {"apply_input": {"namespace_id": 3, "namespace_full_name": "Gone.name"}}),
        )
    )
    for index, key in ((0, "catalog_write_settings"), (1, "train_input"), (2, "apply_input")):
        settings = _settings(nd, index, key)
        assert settings["namespace_full_name"] == "Cat.Sch" and settings["namespace_id"] is None


def test_run_flow_registration_id_becomes_the_flow_identity():
    nd = _normalize(_with_nodes(("run_flow", {"flow_reference": {"registration_id": 12}})))
    ref = _settings(nd, 0, "flow_reference")
    assert ref == {"registration_id": 0, "flow_uuid": "child-uuid", "namespace": "Cat.Sch", "name": "child"}


def test_run_flow_with_a_uuid_only_loses_the_id():
    nd = _normalize(_with_nodes(("run_flow", {"flow_reference": {"registration_id": 12, "flow_uuid": "kept"}})))
    ref = _settings(nd, 0, "flow_reference")
    assert ref == {"registration_id": 0, "flow_uuid": "kept"}


def test_an_id_that_resolves_to_nothing_is_dropped_and_the_names_stay():
    nd = _normalize(
        _with_nodes(
            ("catalog_reader", {"catalog_table_id": 99, "catalog_full_table_name": "Old.orders"}),
            ("catalog_writer", {"catalog_write_settings": {"namespace_id": 99}}),
            ("run_flow", {"flow_reference": {"registration_id": 99, "namespace": "A.b", "name": "c"}}),
        )
    )
    reader = _settings(nd, 0)
    assert reader["catalog_table_id"] is None and reader["catalog_full_table_name"] == "Old.orders"
    writer = _settings(nd, 1, "catalog_write_settings")
    assert writer == {"namespace_id": None}
    ref = _settings(nd, 2, "flow_reference")
    assert ref == {"registration_id": 0, "namespace": "A.b", "name": "c"}


def test_naming_references_twice_is_byte_identical():
    nodes = (
        ("catalog_reader", {"catalog_table_id": 7}),
        ("catalog_writer", {"catalog_write_settings": {"namespace_id": 3}}),
        ("run_flow", {"flow_reference": {"registration_id": 12}}),
        ("manual_input", {"raw_data_format": None}),
    )
    once = _normalize(_with_nodes(*nodes))
    twice = normalize_flow_data(once, "u", "Daily FX Sync", lookup=_no_lookup)
    assert dump_yaml(once) == dump_yaml(twice)
