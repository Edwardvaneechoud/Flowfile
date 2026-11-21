"""What a kernel in docker sees of the user data folder: the catalog's tables, the uploads and outputs folders and its
owner's workspace, at the paths core sees them.

These read the container's run arguments; no Docker is needed.
"""

from flowfile_core.kernel.manager import KernelManager
from flowfile_core.kernel.models import KernelInfo
from shared.storage_config import storage

INTERNAL = "/app/internal_storage"


def _manager(monkeypatch, tmp_path, *, catalog_type, catalog_target=None, catalog_volume=None):
    monkeypatch.setenv("FLOWFILE_MODE", "docker")
    monkeypatch.setattr(storage, "_user_data_dir", tmp_path)
    mgr = KernelManager.__new__(KernelManager)
    mgr._shared_volume = f"{INTERNAL}/cache"
    mgr._catalog_tables_dir = str(tmp_path / "catalog_tables")
    mgr._kernel_volume = "flowfile-internal-storage"
    mgr._kernel_volume_type = "volume"
    mgr._kernel_mount_target = INTERNAL
    mgr._catalog_volume = catalog_volume or "/srv/flowfile_data"
    mgr._catalog_volume_type = catalog_type
    mgr._catalog_mount_target = catalog_target or str(tmp_path)
    mgr._docker_network = "flowfile-network"
    mgr._core_instance_id = mgr._runtime_id = "x"
    mgr._kernel_owners = {"k": 7}
    return mgr


def _user_data_mounts(mgr) -> dict[str, dict]:
    kernel = KernelInfo(id="k", name="k")
    mgr._kernels = {"k": kernel}
    mounts = mgr._build_run_kwargs("k", kernel, {})["mounts"]
    return {m["Target"]: m for m in mounts if m["Target"] != INTERNAL}


def test_a_bind_of_the_user_data_folder_gives_the_tables_uploads_outputs_and_the_owners_workspace(
    monkeypatch, tmp_path
):
    mounts = _user_data_mounts(_manager(monkeypatch, tmp_path, catalog_type="bind"))
    assert {target: m["Source"] for target, m in mounts.items()} == {
        str(tmp_path / "catalog_tables"): "/srv/flowfile_data/catalog_tables",
        str(tmp_path / "uploads"): "/srv/flowfile_data/uploads",
        str(tmp_path / "outputs"): "/srv/flowfile_data/outputs",
        str(tmp_path / "workspaces" / "7"): "/srv/flowfile_data/workspaces/7",
    }
    assert all(m["Type"] == "bind" for m in mounts.values())
    assert (tmp_path / "workspaces" / "7").is_dir()


def test_a_kernel_without_an_owner_gets_no_workspace(monkeypatch, tmp_path):
    mgr = _manager(monkeypatch, tmp_path, catalog_type="bind")
    mgr._kernel_owners = {}
    assert str(tmp_path / "workspaces" / "7") not in _user_data_mounts(mgr)


def test_a_bind_of_the_tables_folder_itself_is_mounted_as_it_is(monkeypatch, tmp_path):
    mgr = _manager(
        monkeypatch,
        tmp_path,
        catalog_type="bind",
        catalog_volume="/srv/tables",
        catalog_target=str(tmp_path / "catalog_tables"),
    )
    mounts = _user_data_mounts(mgr)
    assert {target: m["Source"] for target, m in mounts.items()} == {str(tmp_path / "catalog_tables"): "/srv/tables"}


def test_a_named_volume_is_mounted_at_its_mount_point(monkeypatch, tmp_path):
    mounts = _user_data_mounts(_manager(monkeypatch, tmp_path, catalog_type="volume", catalog_volume="flowfile-data"))
    assert {target: m["Source"] for target, m in mounts.items()} == {str(tmp_path): "flowfile-data"}
