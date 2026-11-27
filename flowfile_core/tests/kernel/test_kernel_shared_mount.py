"""A kernel that a core in a container starts mounts the shared folder and nothing else of the mount that folder is
in, at the path core sees it. The first tests read the container's run arguments (no Docker); the rest run a kernel
image over a real volume and a real bind."""

import subprocess
import uuid
from types import SimpleNamespace

import pytest

from flowfile_core.kernel.manager import KernelManager, KernelMountRefused
from flowfile_core.kernel.models import KernelInfo, KernelState

VOLUME = "flowfile-internal-storage"


def _manager(storage_dir, *, source=VOLUME, mount_type="volume", shared=None, api="1.47", client=None):
    """A manager as a core in a container builds it, its internal storage being *source* mounted at *storage_dir*.

    *api* stands in for the Engine API of a Docker older than the one at hand; *client* is a real Docker client.
    """
    mgr = KernelManager.__new__(KernelManager)
    mgr._shared_volume = shared or f"{storage_dir}/temp/kernel_shared"
    mgr._catalog_tables_dir = f"{storage_dir}/catalog_tables"
    mgr._kernel_volume = source
    mgr._kernel_volume_type = mount_type
    mgr._kernel_mount_target = str(storage_dir)
    mgr._catalog_volume = None
    mgr._docker_network = None
    mgr._core_instance_id = mgr._runtime_id = "x"
    mgr._started_here = set()
    mgr._docker = client or SimpleNamespace(api=SimpleNamespace(api_version=api))
    mgr._mount_refusal = mgr._shared_mount_refusal()
    return mgr


def _kernel(kernel_id: str = "k") -> KernelInfo:
    return KernelInfo(id=kernel_id, name=kernel_id, memory_gb=1, cpu_cores=1)


def _mounts(mgr) -> list[dict]:
    return mgr._build_run_kwargs("k", _kernel(), {})["mounts"]


def test_of_a_named_volume_a_kernel_mounts_the_shared_folder_by_subpath(tmp_path):
    assert _mounts(_manager(tmp_path)) == [
        {
            "Target": f"{tmp_path}/temp/kernel_shared",
            "Source": VOLUME,
            "Type": "volume",
            "ReadOnly": False,
            "VolumeOptions": {"Subpath": "temp/kernel_shared"},
        }
    ]


def test_of_a_bind_a_kernel_mounts_the_matching_subfolder(tmp_path):
    assert _mounts(_manager(tmp_path, source="/srv/flowfile/internal", mount_type="bind")) == [
        {
            "Target": f"{tmp_path}/temp/kernel_shared",
            "Source": "/srv/flowfile/internal/temp/kernel_shared",
            "Type": "bind",
            "ReadOnly": False,
        }
    ]


def test_a_volume_that_is_the_shared_folder_is_mounted_whole(tmp_path):
    assert _mounts(_manager(tmp_path, shared=str(tmp_path))) == [
        {"Target": str(tmp_path), "Source": VOLUME, "Type": "volume", "ReadOnly": False}
    ]


def test_docker_before_26_starts_no_kernel_on_a_folder_of_a_named_volume(tmp_path):
    mgr = _manager(tmp_path, api="1.44")
    with pytest.raises(KernelMountRefused) as refused:
        _mounts(mgr)
    assert str(refused.value) == (
        "Docker on this server is older than 26.0, so a kernel cannot mount only its shared folder; upgrade "
        f"Docker, or mount a separate volume at {tmp_path}/temp/kernel_shared in core and the worker."
    )


def test_docker_before_26_mounts_a_binds_subfolder_and_a_volume_of_its_own(tmp_path):
    assert _manager(tmp_path, source="/srv/flowfile/internal", mount_type="bind", api="1.44")._mount_refusal is None
    assert _manager(tmp_path, shared=str(tmp_path), api="1.44")._mount_refusal is None
    assert _manager(tmp_path, api="1.45")._mount_refusal is None


def test_a_container_with_other_mounts_is_told_apart_by_its_label(tmp_path):
    mgr = _manager(tmp_path)
    label = mgr._build_run_kwargs("k", _kernel(), {})["labels"]["flowfile_mounts"]
    assert not mgr._mounts_changed("k", _kernel(), SimpleNamespace(labels={"flowfile_mounts": label}))
    assert mgr._mounts_changed("k", _kernel(), SimpleNamespace(labels={}))
    of_a_bind = _manager(tmp_path, source="/srv/flowfile/internal", mount_type="bind")
    assert of_a_bind._mounts_changed("k", _kernel(), SimpleNamespace(labels={"flowfile_mounts": label}))
    assert _manager(tmp_path, api="1.44")._mounts_changed("k", _kernel(), SimpleNamespace(labels={}))


def test_a_kernel_on_this_machine_keeps_its_binds_and_is_never_replaced_for_them(tmp_path, monkeypatch):
    monkeypatch.setenv("FLOWFILE_MODE", "electron")
    mgr = _manager(tmp_path)
    mgr._kernel_volume = None
    kernel = KernelInfo(id="k", name="k", port=19001)
    kwargs = mgr._build_run_kwargs("k", kernel, {})
    assert {v["bind"] for v in kwargs["volumes"].values()} == {"/shared", "/catalog_tables"}
    assert "mounts" not in kwargs and "flowfile_mounts" not in kwargs["labels"]
    assert not mgr._mounts_changed("k", kernel, SimpleNamespace(labels={}))


def _docker_ready() -> bool:
    try:
        return subprocess.run(["docker", "info"], capture_output=True, timeout=10).returncode == 0
    except (OSError, subprocess.TimeoutExpired):
        return False


@pytest.fixture
def docker_client():
    import docker

    if not _docker_ready():
        pytest.skip("Docker is not available")
    client = docker.from_env()
    yield client
    client.close()


@pytest.fixture
def image(docker_client) -> str:
    import docker

    from flowfile_core.kernel.images import _kernel_image_base

    tag = _kernel_image_base()
    try:
        docker_client.images.get(tag)
    except docker.errors.ImageNotFound:
        pytest.skip(f"kernel image {tag} is not present locally")
    return tag


def _in_volume(client, image: str, volume: str, command: str) -> str:
    """Run *command* in a throwaway container that mounts the whole of *volume* at ``/v``, as core has it."""
    from docker.types import Mount

    return client.containers.run(
        image, entrypoint=["sh", "-c", command], mounts=[Mount(target="/v", source=volume, type="volume")], remove=True
    ).decode()


@pytest.fixture
def storage_volume(docker_client, image):
    """A named volume holding what internal storage holds: the catalog database beside the shared folder."""
    volume = docker_client.volumes.create(f"flowfile-test-internal-{uuid.uuid4().hex[:8]}")
    _in_volume(
        docker_client,
        image,
        volume.name,
        "mkdir -p /v/database /v/temp/kernel_shared && echo db > /v/database/flowfile_catalog.db"
        " && echo rows > /v/temp/kernel_shared/from_core.txt",
    )
    yield volume.name
    volume.remove(force=True)


def _ls(container, path) -> list[str]:
    return container.exec_run(["ls", str(path)]).output.decode().split()


def _remove(client, name: str) -> None:
    import docker

    try:
        client.containers.get(name).remove(force=True)
    except docker.errors.NotFound:
        pass


@pytest.mark.kernel
def test_a_kernel_sees_the_shared_folder_and_not_the_rest_of_its_volume(tmp_path, docker_client, image, storage_volume):
    mgr = _manager(tmp_path, source=storage_volume, client=docker_client)
    if mgr._mount_refusal:
        pytest.skip("Docker before 26.0 mounts no folder of a volume")
    kernel_id = f"shared-mount-{uuid.uuid4().hex[:8]}"
    kernel = _kernel(kernel_id)
    container = docker_client.containers.run(image, **mgr._build_run_kwargs(kernel_id, kernel, {}))
    try:
        assert _ls(container, tmp_path) == ["temp"]
        assert _ls(container, tmp_path / "temp") == ["kernel_shared"]
        assert _ls(container, mgr._shared_volume) == ["from_core.txt"]
        container.exec_run(["sh", "-c", f"echo rows > {mgr._shared_volume}/from_kernel.txt"])
        seen_by_core = _in_volume(docker_client, image, storage_volume, "ls /v/temp/kernel_shared")
        assert seen_by_core.split() == ["from_core.txt", "from_kernel.txt"]
    finally:
        container.remove(force=True)


@pytest.mark.kernel
def test_of_a_bind_a_kernel_sees_the_shared_folder_only(tmp_path, docker_client, image):
    storage_dir = tmp_path / "internal"
    (storage_dir / "database").mkdir(parents=True)
    (storage_dir / "database" / "flowfile_catalog.db").write_text("db")
    shared = storage_dir / "temp" / "kernel_shared"
    shared.mkdir(parents=True)
    (shared / "from_core.txt").write_text("rows")
    mgr = _manager(storage_dir, source=str(storage_dir), mount_type="bind", client=docker_client)
    kernel_id = f"shared-mount-{uuid.uuid4().hex[:8]}"
    container = docker_client.containers.run(image, **mgr._build_run_kwargs(kernel_id, _kernel(kernel_id), {}))
    try:
        assert _ls(container, storage_dir) == ["temp"]
        assert _ls(container, shared) == ["from_core.txt"]
        container.exec_run(["sh", "-c", f"echo rows > {shared}/from_kernel.txt"])
        assert (shared / "from_kernel.txt").read_text() == "rows\n"
    finally:
        container.remove(force=True)


@pytest.mark.kernel
def test_a_start_replaces_a_container_that_has_the_whole_volume(tmp_path, docker_client, image, storage_volume):
    from docker.types import Mount

    mgr = _manager(tmp_path, source=storage_volume, client=docker_client)
    if mgr._mount_refusal:
        pytest.skip("Docker before 26.0 mounts no folder of a volume")
    kernel_id = f"shared-mount-{uuid.uuid4().hex[:8]}"
    kernel = _kernel(kernel_id)
    name = f"flowfile-kernel-{kernel_id}"
    whole = docker_client.containers.run(
        image, name=name, detach=True, mounts=[Mount(target=str(tmp_path), source=storage_volume, type="volume")]
    )
    try:
        assert "database" in _ls(whole, tmp_path)
        assert mgr._mounts_changed(kernel_id, kernel, whole)
        run_kwargs = mgr._build_run_kwargs(kernel_id, kernel, {})
        container, found_running = mgr._create_or_adopt_container(kernel_id, kernel, image, run_kwargs)
        assert container.id != whole.id and not found_running
        assert _ls(container, tmp_path) == ["temp"]
        assert not mgr._mounts_changed(kernel_id, kernel, container)
    finally:
        _remove(docker_client, name)


@pytest.mark.kernel
def test_a_starting_core_removes_a_running_kernel_that_has_the_whole_volume(
    tmp_path, docker_client, image, storage_volume
):
    import docker
    from docker.types import Mount

    mgr = _manager(tmp_path, source=storage_volume, client=docker_client)
    if mgr._mount_refusal:
        pytest.skip("Docker before 26.0 mounts no folder of a volume")
    # The kernels have no rows here, so the two steps that read and write them are left out.
    mgr._kernel_run_ids = lambda: {}
    mgr._begin_kernel_run = lambda *args, **kwargs: True
    old_id, new_id = (f"shared-mount-{uuid.uuid4().hex[:8]}" for _ in range(2))
    mgr._kernels = {old_id: _kernel(old_id), new_id: _kernel(new_id)}
    docker_client.containers.run(
        image,
        name=f"flowfile-kernel-{old_id}",
        detach=True,
        mounts=[Mount(target=str(tmp_path), source=storage_volume, type="volume")],
    )
    kept = docker_client.containers.run(image, **mgr._build_run_kwargs(new_id, mgr._kernels[new_id], {}))
    try:
        mgr._reclaim_running_containers(prune_orphans=False)
        with pytest.raises(docker.errors.NotFound):
            docker_client.containers.get(f"flowfile-kernel-{old_id}")
        assert mgr._kernels[old_id].container_id is None
        assert mgr._kernels[old_id].state == KernelState.STOPPED
        assert mgr._kernels[new_id].container_id == kept.id
        assert mgr._kernels[new_id].state == KernelState.IDLE
    finally:
        _remove(docker_client, f"flowfile-kernel-{old_id}")
        _remove(docker_client, f"flowfile-kernel-{new_id}")


@pytest.mark.kernel
def test_a_kernel_persists_its_artifacts_in_the_folder_its_delete_removes(tmp_path, docker_client, image, monkeypatch):
    """``delete_kernel`` removes ``<shared>/artifacts/<kernel id>``, which is where the kernel image keeps them."""
    monkeypatch.setenv("FLOWFILE_MODE", "electron")
    mgr = _manager(tmp_path)
    mgr._kernel_volume = None
    mgr._kernels = {}
    kernel_id = f"shared-mount-{uuid.uuid4().hex[:8]}"
    kernel = KernelInfo(id=kernel_id, name=kernel_id, memory_gb=1, cpu_cores=1, port=mgr._allocate_port())
    mgr._kernels[kernel_id] = kernel
    persisted = tmp_path / "temp" / "kernel_shared" / "artifacts" / kernel_id
    persisted.parent.parent.mkdir(parents=True)
    env = mgr._build_kernel_env(kernel_id, kernel, "a-token")
    container = docker_client.containers.run(image, **mgr._build_run_kwargs(kernel_id, kernel, env))
    try:
        mgr._wait_for_healthy_sync(kernel_id, timeout=90)
        assert persisted.is_dir()
    finally:
        container.remove(force=True)
