"""A notebook kernel's container: the two binds every kernel gets and nothing else, and the env that keeps its
flowfile from migrating, seeding or offloading (no Docker)."""

from flowfile_core.kernel.models import ImageFlavour, KernelConfig, KernelInfo


def _manager(tmp_path):
    from flowfile_core.kernel.manager import KernelManager

    mgr = KernelManager.__new__(KernelManager)
    mgr._shared_volume = str(tmp_path / "shared")
    mgr._catalog_tables_dir = str(tmp_path / "catalog")
    mgr._kernel_volume = None
    mgr._docker_network = None
    mgr._core_instance_id = mgr._runtime_id = "x"
    return mgr


def test_a_kernel_mounts_only_the_shared_and_catalog_folders(tmp_path, monkeypatch):
    monkeypatch.setenv("FLOWFILE_MODE", "electron")
    mgr = _manager(tmp_path)
    kernel = KernelInfo(id="k", name="k", port=19001, image_flavour=ImageFlavour.NOTEBOOK)
    mgr._kernels = {"k": kernel}
    kwargs = mgr._build_run_kwargs("k", kernel, {})
    assert {v["bind"] for v in kwargs["volumes"].values()} == {"/shared", "/catalog_tables"}
    assert "mounts" not in kwargs and "tmpfs" not in kwargs
    assert mgr.to_kernel_path(str(tmp_path / "shared" / "a")) == "/shared/a"


def test_env_of_a_notebook_kernel_and_of_a_plain_one(tmp_path, monkeypatch):
    monkeypatch.setenv("FLOWFILE_MODE", "electron")
    mgr = _manager(tmp_path)
    plain = KernelInfo(id="k", name="k", port=19001)
    mgr._kernels = {"k": plain}
    env = mgr._build_kernel_env("k", plain)
    assert "FLOWFILE_SKIP_INIT_DB" not in env and "FLOWFILE_OFFLOAD_TO_WORKER" not in env

    # A custom image is never a notebook kernel, whatever it is called; only the flavour is.
    custom = KernelInfo(
        id="k", name="k", port=19001, image_flavour=ImageFlavour.CUSTOM, custom_image="me/notebook-kernel:1.0"
    )
    assert "FLOWFILE_SKIP_INIT_DB" not in mgr._build_kernel_env("k", custom)

    notebook = KernelInfo(id="k", name="k", port=19001, image_flavour=ImageFlavour.NOTEBOOK)
    env = mgr._build_kernel_env("k", notebook)
    assert env["FLOWFILE_SKIP_STARTUP_MIGRATION"] == "1" and env["FLOWFILE_SKIP_INIT_DB"] == "1"
    assert env["FLOWFILE_KERNEL_GC"] == "0" and env["FLOWFILE_TELEMETRY"] == "0"
    assert env["FLOWFILE_OFFLOAD_TO_WORKER"] == "0"
    for name in ("FLOWFILE_STORAGE_DIR", "FLOWFILE_NOTEBOOK_MOUNTS", "FLOWFILE_DB_PATH"):
        assert name not in env, f"a notebook kernel mounts no host folder and holds no database: {name}"


def test_an_older_clients_mounted_folders_are_ignored():
    config = KernelConfig.model_validate({"id": "k", "name": "k", "mounted_folders": ["/x"]})
    assert "mounted_folders" not in config.model_dump()
