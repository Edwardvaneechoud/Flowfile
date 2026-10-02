"""The kernel mount table and folder validation (no Docker)."""

import pytest

from flowfile_core.kernel import notebook_mounts
from flowfile_core.kernel.models import KernelInfo
from flowfile_core.kernel.notebook_mounts import (
    build_mount_table,
    kernel_side,
    masked_paths,
    translate,
    validate_mounted_folders,
)
from flowfile_core.kernel.notebook_support import is_notebook_kernel_config


@pytest.mark.parametrize(
    "host, expected",
    [
        (r"C:\Users\me\x", "/host/c/Users/me/x"),
        ("C:/Users/me/x", "/host/c/Users/me/x"),
        ("d:\\data\\", "/host/d/data"),
        ("/Users/me/x", "/Users/me/x"),
        ("/home/me/x/", "/home/me/x"),
    ],
)
def test_kernel_side(host, expected):
    assert kernel_side(host) == expected


def test_translate_longest_prefix_wins():
    table = {"/data": "/data", "/data/deep": "/elsewhere"}
    assert translate("/data/deep/f.csv", table) == "/elsewhere/f.csv"
    assert translate("/data/other/f.csv", table) == "/data/other/f.csv"
    assert translate("/data", table) == "/data"


def test_translate_windows_paths():
    table = {r"C:\Users\me\.flowfile\database": "/host/c/Users/me/.flowfile/database"}
    assert translate(r"c:\users\me\.flowfile\database\x.db", table) == "/host/c/Users/me/.flowfile/database/x.db"


def test_translate_uncovered_path_is_none():
    table = {"/data": "/data"}
    assert translate("/datasets/f.csv", table) is None
    assert translate("/other", {}) is None


def test_validate_folders(tmp_path, monkeypatch):
    monkeypatch.delenv("FLOWFILE_MODE", raising=False)
    assert validate_mounted_folders([str(tmp_path) + "/", str(tmp_path)]) == [str(tmp_path)]
    with pytest.raises(ValueError, match="absolute"):
        validate_mounted_folders(["relative/dir"])
    with pytest.raises(ValueError, match="does not exist"):
        validate_mounted_folders([str(tmp_path / "missing")])
    with pytest.raises(ValueError, match="kernel's own files"):
        validate_mounted_folders(["/usr"])


@pytest.mark.parametrize("mode", ["docker", "package"])
def test_folders_refused_outside_electron(tmp_path, monkeypatch, mode):
    monkeypatch.setenv("FLOWFILE_MODE", mode)
    assert validate_mounted_folders([]) == []
    with pytest.raises(ValueError, match="desktop app"):
        validate_mounted_folders([str(tmp_path)])


def test_mount_table_for_plain_and_notebook_kernels(tmp_path, monkeypatch):
    monkeypatch.setenv("FLOWFILE_MODE", "electron")
    chosen = tmp_path / "chosen"
    chosen.mkdir()
    plain = KernelInfo(id="k", name="k", mounted_folders=[str(chosen), str(tmp_path / "gone")])
    assert build_mount_table(plain) == {str(chosen): str(chosen)}

    notebook = KernelInfo(id="n", name="n", packages=["flowfile==0.21.0"])
    assert is_notebook_kernel_config(notebook)
    table = build_mount_table(notebook)
    from shared.storage_config import storage

    assert table[str(storage.flows_directory)] == kernel_side(str(storage.flows_directory))
    assert str(storage.database_directory) not in table

    monkeypatch.setenv("FLOWFILE_MODE", "docker")
    assert build_mount_table(notebook) == {}


def test_custom_image_marker():
    assert is_notebook_kernel_config(KernelInfo(id="c", name="c", custom_image="flowfile-kernel-notebook:dev"))
    assert not is_notebook_kernel_config(KernelInfo(id="c", name="c", custom_image="python:3.12"))


def test_key_store_masked_when_a_folder_holds_it(tmp_path, monkeypatch):
    monkeypatch.setenv("FLOWFILE_SECURE_STORAGE_PATH", str(tmp_path / ".config" / "flowfile"))
    assert masked_paths({str(tmp_path): str(tmp_path)}) == [str(tmp_path / ".config" / "flowfile")]
    assert masked_paths({str(tmp_path / "data"): str(tmp_path / "data")}) == []
    assert notebook_mounts.key_store_dir() == str(tmp_path / ".config" / "flowfile")


def test_run_kwargs_and_env_for_a_folder_kernel(tmp_path, monkeypatch):
    from flowfile_core.kernel.manager import KernelManager

    monkeypatch.setenv("FLOWFILE_MODE", "electron")
    monkeypatch.setenv("FLOWFILE_SECURE_STORAGE_PATH", str(tmp_path / "home" / ".config" / "flowfile"))
    (tmp_path / "home" / ".config" / "flowfile").mkdir(parents=True)
    mgr = KernelManager.__new__(KernelManager)
    mgr._shared_volume = str(tmp_path / "shared")
    mgr._catalog_tables_dir = str(tmp_path / "catalog")
    mgr._kernel_volume = None
    mgr._docker_network = None
    mgr._core_instance_id = mgr._runtime_id = "x"
    kernel = KernelInfo(id="k", name="k", port=19001, mounted_folders=[str(tmp_path / "home")])
    mgr._kernels = {"k": kernel}

    kwargs = mgr._build_run_kwargs("k", kernel, {})
    assert [(m["Source"], m["Target"], m["ReadOnly"]) for m in kwargs["mounts"]] == [
        (str(tmp_path / "home"), str(tmp_path / "home"), True)
    ]
    assert kwargs["tmpfs"] == {str(tmp_path / "home" / ".config" / "flowfile"): "ro"}
    assert mgr.to_kernel_path(str(tmp_path / "shared" / "a")) == "/shared/a"

    env = mgr._build_kernel_env("k", kernel)
    assert "FLOWFILE_SKIP_INIT_DB" not in env
    assert env["FLOWFILE_NOTEBOOK_MOUNTS"]
    assert "FLOWFILE_DB_PATH" not in env
    kernel.custom_image = "flowfile-kernel-notebook:dev"
    env = mgr._build_kernel_env("k", kernel)
    assert env["FLOWFILE_SKIP_STARTUP_MIGRATION"] == "1" and env["FLOWFILE_STORAGE_DIR"]
    assert env["FLOWFILE_DB_PATH"] == "/shared/notebook_db/k/flowfile_catalog.db"


def _storage_at(monkeypatch, base):
    from shared.storage_config import storage

    monkeypatch.setattr(storage, "_base_dir", base)


def test_folders_holding_flowfile_storage_are_refused(tmp_path, monkeypatch):
    monkeypatch.delenv("FLOWFILE_MODE", raising=False)
    base = tmp_path / ".flowfile"
    for folder in (base / "outputs", tmp_path / "data"):
        folder.mkdir(parents=True)
    _storage_at(monkeypatch, base)
    for folder in (tmp_path, base):
        with pytest.raises(ValueError, match="holds Flowfile's own folder"):
            validate_mounted_folders([str(folder)])
    assert validate_mounted_folders([str(tmp_path / "data"), str(base / "outputs")]) == [
        str(tmp_path / "data"),
        str(base / "outputs"),
    ]
    saved = KernelInfo(id="k", name="k", mounted_folders=[str(tmp_path), str(tmp_path / "data")])
    assert build_mount_table(saved) == {str(tmp_path / "data"): str(tmp_path / "data")}


@pytest.mark.parametrize(
    "folder, held",
    [
        (r"C:\Users\me", True),
        (r"c:\users\me", True),
        ("C:\\", True),
        (r"C:\Users\me\.flowfile", True),
        (r"C:\Users\me\data", False),
        (r"C:\Users\me\.flowfile\outputs", False),
        ("D:\\", False),
    ],
)
def test_windows_folders_holding_flowfile_storage(monkeypatch, folder, held):
    from pathlib import PureWindowsPath

    _storage_at(monkeypatch, PureWindowsPath(r"C:\Users\me\.flowfile"))
    expected = r"C:\Users\me\.flowfile" if held else None
    assert notebook_mounts._held_storage(notebook_mounts._normalized(folder)) == expected


def test_writable_folders_validate_persist_and_mount_read_write(tmp_path, monkeypatch):
    import json
    from types import SimpleNamespace

    from flowfile_core.kernel import persistence
    from flowfile_core.kernel.manager import KernelManager
    from flowfile_core.kernel.models import KernelConfig, MountedFolder

    monkeypatch.setenv("FLOWFILE_MODE", "electron")
    ref, out = tmp_path / "ref", tmp_path / "out"
    ref.mkdir()
    out.mkdir()
    folders = validate_mounted_folders(
        [str(ref), MountedFolder(path=str(out) + "/", writable=True), MountedFolder(path=str(ref), writable=True)]
    )
    assert folders == [str(ref), MountedFolder(path=str(out), writable=True)]
    assert validate_mounted_folders([MountedFolder(path=str(ref))]) == [str(ref)]

    row = SimpleNamespace(mounted_folders=persistence._folders_to_json(folders))
    assert json.loads(row.mounted_folders) == [str(ref), {"path": str(out), "writable": True}]
    assert persistence._row_to_folders(row) == folders
    junk = [
        "/a",
        {"path": "/b", "writable": True},
        {"path": "/c", "writable": False},
        {"path": "/d", "writable": "yes"},
        {"nope": 1},
        5,
        None,
    ]
    assert persistence._row_to_folders(SimpleNamespace(mounted_folders=json.dumps(junk))) == [
        "/a",
        MountedFolder(path="/b", writable=True),
        "/c",
        "/d",
    ]

    config = KernelConfig.model_validate(
        {"id": "k", "name": "k", "mounted_folders": ["/a", {"path": "/b", "writable": True}]}
    )
    assert config.mounted_folders == ["/a", MountedFolder(path="/b", writable=True)]
    assert config.model_dump(mode="json")["mounted_folders"] == ["/a", {"path": "/b", "writable": True}]
    assert KernelInfo(id="k", name="k", mounted_folders=["/a"]).model_dump(mode="json")["mounted_folders"] == ["/a"]

    mgr = KernelManager.__new__(KernelManager)
    mgr._shared_volume = str(tmp_path / "shared")
    mgr._catalog_tables_dir = str(tmp_path / "catalog")
    mgr._kernel_volume = None
    mgr._docker_network = None
    mgr._core_instance_id = mgr._runtime_id = "x"
    kernel = KernelInfo(id="k", name="k", port=19001, mounted_folders=folders)
    mgr._kernels = {"k": kernel}
    kwargs = mgr._build_run_kwargs("k", kernel, {})
    assert {(m["Source"], m["ReadOnly"]) for m in kwargs["mounts"]} == {(str(ref), True), (str(out), False)}


HOST_FOLDERS = {"/host/c/Users/me/data": r"C:\Users\me\data", "/host/c/Users/me/data/deep": r"C:\Users\me\data\deep"}


@pytest.mark.parametrize(
    "path, expected",
    [
        ("/host/c/Users/me/data/out.csv", r"C:\Users\me\data\out.csv"),
        ("/host/c/Users/me/data/", r"C:\Users\me\data"),
        ("/host/c/Users/me/data/**/*.csv", r"C:\Users\me\data\**\*.csv"),
        ("/host/c/Users/me/data/deep/x.csv", r"C:\Users\me\data\deep\x.csv"),
        (r"C:\Users\me\data\out.csv", None),
        ("/host/c/Users/me/database/x.csv", None),
        ("/host/C/Users/me/data/x.csv", None),
        ("s3://bucket/x.csv", None),
    ],
)
def test_host_side(path, expected):
    assert notebook_mounts.host_side(path, HOST_FOLDERS) == expected


@pytest.mark.parametrize("host, rest", [(r"C:\Users\me\data", r"\f.csv"), (r"d:\x", r"\f.csv"), ("/Users/me/data", "/f.csv")])
def test_host_side_inverts_translate(host, rest):
    table = {notebook_mounts._normalized(host): kernel_side(host)}
    inverse = {target: source for source, target in table.items()}
    assert notebook_mounts.host_side(translate(host + rest, table), inverse) == notebook_mounts._normalized(host) + rest


def test_host_folders_inverts_the_mount_table_without_identity_entries(monkeypatch):
    from flowfile_core.kernel.manager import KernelManager

    mgr = KernelManager.__new__(KernelManager)
    mgr._kernel_volume = None
    mgr._kernels = {"k": KernelInfo(id="k", name="k")}
    table = {r"C:\Users\me\data": "/host/c/Users/me/data", "/Users/me/x": "/Users/me/x"}
    monkeypatch.setattr(notebook_mounts, "build_mount_table", lambda kernel: table)
    assert mgr.host_folders("k") == {"/host/c/Users/me/data": r"C:\Users\me\data"}
    assert mgr.host_folders("gone") == {}
    mgr._kernel_volume = "vol"
    assert mgr.host_folders("k") == {}
