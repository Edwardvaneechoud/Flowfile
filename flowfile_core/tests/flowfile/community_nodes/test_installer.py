"""Installer + client tests against a real on-disk fixture registry (fixture mode).

No HTTP: FLOWFILE_COMMUNITY_INDEX_URL points at a local index.json, so the real
CommunityClient reads index + artifacts straight off disk and verifies pins.
Storage is redirected to tmp so the singleton registry / receipts never touch the
developer's ~/.flowfile.
"""
import hashlib
import json
import uuid
from pathlib import Path

import pytest

import flowfile_core.flowfile.community_nodes.client as client_mod
from flowfile_core.flowfile.community_nodes import installer
from flowfile_core.flowfile.community_nodes.client import PinMismatchError, get_community_client
from flowfile_core.flowfile.community_nodes.models import InstallRequest
from flowfile_core.flowfile.community_nodes.receipts import load_receipts
from flowfile_core.flowfile.user_defined.registry import registry
from shared import storage

BENIGN_NODE = '''
import polars as pl
from shared.node_designer import CustomNodeBase, NodeSettings, Section, TextInput


class {class_name}(CustomNodeBase):
    node_name: str = "{node_name}"
    node_category: str = "Community Test"

    settings_schema: NodeSettings = NodeSettings(
        main=Section(title="Main", value=TextInput(label="Value", default="x")),
    )

    def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
        return inputs[0]
'''

NETWORK_NODE = '''
import urllib.request

import polars as pl
from shared.node_designer import CustomNodeBase, NodeSettings, Section, TextInput


class {class_name}(CustomNodeBase):
    node_name: str = "{node_name}"
    node_category: str = "Community Test"

    settings_schema: NodeSettings = NodeSettings(
        main=Section(title="Main", value=TextInput(label="Value", default="x")),
    )

    def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
        return inputs[0]
'''

_FAKE_PNG = b"\x89PNG\r\n\x1a\n" + b"0" * 48


def _pin(path_rel: str, data: bytes) -> dict:
    return {"path": path_rel, "sha256": hashlib.sha256(data).hexdigest(), "size": len(data)}


def build_fixture(
    root: Path,
    *,
    node_id: str,
    node_name: str,
    class_name: str,
    version: str = "1.0.0",
    source_template: str = BENIGN_NODE,
    with_icon: bool = True,
    commit: str = "commit0",
    min_version: str = "0.0.1",
) -> Path:
    """Write nodes/<id>/{node.py,manifest.json,icon.png} + index.json; return index path."""
    node_dir = root / "nodes" / node_id
    node_dir.mkdir(parents=True, exist_ok=True)
    source = source_template.format(class_name=class_name, node_name=node_name)
    node_bytes = source.encode("utf-8")
    (node_dir / "node.py").write_bytes(node_bytes)

    manifest_bytes = json.dumps({"id": node_id, "node_name": node_name, "version": version}).encode("utf-8")
    (node_dir / "manifest.json").write_bytes(manifest_bytes)

    artifacts = {
        "node": _pin(f"nodes/{node_id}/node.py", node_bytes),
        "manifest": _pin(f"nodes/{node_id}/manifest.json", manifest_bytes),
    }
    if with_icon:
        (node_dir / "icon.png").write_bytes(_FAKE_PNG)
        artifacts["icon"] = _pin(f"nodes/{node_id}/icon.png", _FAKE_PNG)

    index = {
        "schema_version": 1,
        "registry": {"repo": "test/community", "commit": commit},
        "categories": ["Community Test"],
        "nodes": [
            {
                "id": node_id,
                "node_name": node_name,
                "version": version,
                "description": "A community test node.",
                "author": {"github": "octocat"},
                "category": "Community Test",
                "min_flowfile_version": min_version,
                "artifacts": artifacts,
            }
        ],
    }
    index_path = root / "index.json"
    index_path.write_text(json.dumps(index), encoding="utf-8")
    return index_path


@pytest.fixture
def isolated_storage(tmp_path):
    original_base = storage._base_dir
    storage._base_dir = tmp_path / "storage"
    (storage.user_defined_nodes_directory / "icons").mkdir(parents=True, exist_ok=True)
    client_mod._client_instance = None
    registry.scan()
    yield tmp_path
    storage._base_dir = original_base
    client_mod._client_instance = None
    registry.scan()


@pytest.fixture
def make_registry(tmp_path, monkeypatch):
    """Build a fixture registry, point the client at it, return (index_path, ids)."""
    fixture_dir = tmp_path / "registry"
    fixture_dir.mkdir(parents=True, exist_ok=True)

    def _make(**kwargs):
        suffix = uuid.uuid4().hex[:8]
        node_id = kwargs.pop("node_id", f"community_{suffix}")
        node_name = kwargs.pop("node_name", f"Community {suffix}")
        class_name = kwargs.pop("class_name", f"CommunityNode{suffix}")
        index_path = build_fixture(
            fixture_dir, node_id=node_id, node_name=node_name, class_name=class_name, **kwargs
        )
        monkeypatch.setenv("FLOWFILE_COMMUNITY_INDEX_URL", str(index_path))
        return index_path, node_id, node_name, fixture_dir

    return _make


def _install(node_id, *, risks=True, capabilities=None):
    client = get_community_client()
    index, _ = client.get_index()
    request = InstallRequest(
        node_id=node_id,
        version="1.0.0",
        acknowledged_risks=risks,
        acknowledged_capabilities=capabilities or [],
    )
    return installer.install(request, index, client=client, user_id=1, user_email="tester@example.com")


def test_install_happy_path_writes_file_receipt_and_registers(isolated_storage, make_registry):
    _index, node_id, node_name, _dir = make_registry()

    outcome = _install(node_id)

    node_file = storage.user_defined_nodes_directory / f"{node_id}.py"
    assert node_file.exists()
    assert outcome.load_error is None

    receipts = load_receipts()
    assert node_id in receipts
    assert receipts[node_id].version == "1.0.0"
    assert receipts[node_id].consent.acknowledged is True
    assert receipts[node_id].source_commit == "commit0"

    entry = registry.get(node_id)
    assert entry is not None and entry.node_class is not None

    icon_file = receipts[node_id].icon_file
    assert icon_file == f"{node_id}__icon.png"
    assert (storage.user_defined_nodes_icons / icon_file).exists()


def test_pin_mismatch_writes_nothing(isolated_storage, make_registry):
    index_path, node_id, _name, fixture_dir = make_registry()
    # Tamper with the on-disk node after the index pinned it.
    (fixture_dir / "nodes" / node_id / "node.py").write_text("# tampered\n", encoding="utf-8")

    with pytest.raises(PinMismatchError):
        _install(node_id)

    assert not (storage.user_defined_nodes_directory / f"{node_id}.py").exists()
    assert load_receipts() == {}
    assert registry.get(node_id) is None


def test_collision_with_own_node_errors(isolated_storage, make_registry):
    _index, node_id, node_name, _dir = make_registry()
    # A user's own node already owns this node key, with no receipt.
    own = storage.user_defined_nodes_directory / "my_own.py"
    own.write_text(BENIGN_NODE.format(class_name="OwnNode", node_name=node_name), encoding="utf-8")
    registry.load_file(own)
    assert registry.get(node_id) is not None

    with pytest.raises(installer.CollisionError) as exc:
        _install(node_id)
    assert exc.value.holder_file == "my_own.py"
    assert load_receipts() == {}


def test_consent_false_errors(isolated_storage, make_registry):
    _index, node_id, _name, _dir = make_registry()
    with pytest.raises(installer.ConsentRequiredError):
        _install(node_id, risks=False)
    assert not (storage.user_defined_nodes_directory / f"{node_id}.py").exists()
    assert load_receipts() == {}


def test_capability_under_acknowledgment_errors(isolated_storage, make_registry):
    _index, node_id, _name, _dir = make_registry(source_template=NETWORK_NODE)
    with pytest.raises(installer.ConsentCapabilityError) as exc:
        _install(node_id, capabilities=[])
    assert "network" in exc.value.missing
    assert not (storage.user_defined_nodes_directory / f"{node_id}.py").exists()

    # Acknowledging the capability lets the install through.
    outcome = _install(node_id, capabilities=["network"])
    assert outcome.load_error is None
    assert node_id in load_receipts()


def test_uninstall_removes_and_unregisters(isolated_storage, make_registry):
    _index, node_id, _name, _dir = make_registry()
    _install(node_id)
    assert registry.get(node_id) is not None

    installer.uninstall(node_id)

    assert not (storage.user_defined_nodes_directory / f"{node_id}.py").exists()
    assert node_id not in load_receipts()
    assert registry.get(node_id) is None


def test_uninstall_without_receipt_errors(isolated_storage, make_registry):
    with pytest.raises(installer.ReceiptRequiredError):
        installer.uninstall("never_installed")


def test_modified_locally_after_edit(isolated_storage, make_registry):
    _index, node_id, _name, _dir = make_registry()
    _install(node_id)

    node_file = storage.user_defined_nodes_directory / f"{node_id}.py"
    node_file.write_text(node_file.read_text() + "\n# local tweak\n", encoding="utf-8")

    installed = {node.receipt.node_id: node for node in installer.list_installed()}
    assert installed[node_id].modified_locally is True


def test_receipt_pruned_when_file_deleted(isolated_storage, make_registry):
    _index, node_id, _name, _dir = make_registry()
    _install(node_id)

    (storage.user_defined_nodes_directory / f"{node_id}.py").unlink()

    installed = installer.list_installed()
    assert all(node.receipt.node_id != node_id for node in installed)
    assert node_id not in load_receipts()


def test_check_updates_on_version_bump(isolated_storage, make_registry):
    _index, node_id, node_name, fixture_dir = make_registry()
    _install(node_id)

    # Re-publish a higher version with different bytes (new sha) at the same location.
    build_fixture(
        fixture_dir,
        node_id=node_id,
        node_name=node_name,
        class_name=f"Bumped{node_id.title().replace('_', '')}",
        version="1.1.0",
        commit="commit1",
    )
    new_index, _ = get_community_client().get_index()
    updates = installer.check_updates(new_index)

    assert len(updates) == 1
    assert updates[0].node_id == node_id
    assert updates[0].installed_version == "1.0.0"
    assert updates[0].latest_version == "1.1.0"


def test_install_state_transitions(isolated_storage, make_registry):
    _index, node_id, _name, _dir = make_registry()
    client = get_community_client()
    index, _ = client.get_index()
    entry = index.entry(node_id)

    assert installer.install_state(entry, None, False, app_version="99.0.0") == "not_installed"

    incompatible_entry = entry.model_copy(update={"min_flowfile_version": "99.0.0"})
    assert installer.install_state(incompatible_entry, None, False, app_version="1.0.0") == "incompatible"

    _install(node_id)
    receipt = load_receipts()[node_id]
    assert installer.install_state(entry, receipt, False, app_version="99.0.0") == "installed"
    assert installer.install_state(entry, receipt, True, app_version="99.0.0") == "modified_locally"

    newer_entry = entry.model_copy(update={"version": "2.0.0"})
    assert installer.install_state(newer_entry, receipt, False, app_version="99.0.0") == "update_available"
