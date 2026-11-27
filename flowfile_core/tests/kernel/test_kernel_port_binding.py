"""A kernel's port is published on the loopback only, and core reaches it there; on core's Docker network nothing is
published at all. The first tests read the container's run arguments (no Docker); the last runs a kernel image."""

import socket
import subprocess
import uuid

import httpx
import pytest

from flowfile_core.kernel.models import KernelInfo


def _manager(tmp_path, *, volume=None, network=None):
    from flowfile_core.kernel.manager import KernelManager

    mgr = KernelManager.__new__(KernelManager)
    mgr._shared_volume = str(tmp_path / "shared")
    mgr._catalog_tables_dir = str(tmp_path / "catalog")
    mgr._kernel_volume = volume
    mgr._kernel_volume_type = "volume"
    mgr._kernel_mount_target = str(tmp_path)
    mgr._catalog_volume = None
    mgr._docker_network = network
    mgr._core_instance_id = mgr._runtime_id = "x"
    return mgr


def test_a_kernel_on_this_machine_is_published_on_the_loopback_only(tmp_path, monkeypatch):
    monkeypatch.setenv("FLOWFILE_MODE", "electron")
    mgr = _manager(tmp_path)
    kernel = KernelInfo(id="k", name="k", port=19001)
    mgr._kernels = {"k": kernel}
    kwargs = mgr._build_run_kwargs("k", kernel, {})
    assert kwargs["ports"] == {"9999/tcp": ("127.0.0.1", 19001)}
    assert mgr._kernel_url(kernel) == "http://127.0.0.1:19001"


def test_a_kernel_on_cores_docker_network_publishes_no_port(tmp_path, monkeypatch):
    monkeypatch.setenv("FLOWFILE_MODE", "docker")
    mgr = _manager(tmp_path, volume="flowfile-storage", network="flowfile-network")
    kernel = KernelInfo(id="k", name="k")
    mgr._kernels = {"k": kernel}
    kwargs = mgr._build_run_kwargs("k", kernel, {})
    assert "ports" not in kwargs
    assert kwargs["network"] == "flowfile-network"
    assert mgr._kernel_url(kernel) == "http://flowfile-kernel-k:9999"


def _docker_ready() -> bool:
    try:
        return subprocess.run(["docker", "info"], capture_output=True, timeout=10).returncode == 0
    except (OSError, subprocess.TimeoutExpired):
        return False


def _image_present(tag: str) -> bool:
    return subprocess.run(["docker", "image", "inspect", tag], capture_output=True).returncode == 0


def _free_kernel_port() -> int:
    from flowfile_core.kernel.manager import _BASE_PORT, _PORT_RANGE, _is_port_available

    for port in range(_BASE_PORT + _PORT_RANGE - 1, _BASE_PORT, -1):
        if _is_port_available(port):
            return port
    pytest.skip("no free port in the kernel range")


def _address_off_the_loopback() -> str | None:
    """This machine's address on its default route, found without sending anything."""
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        try:
            s.connect(("192.0.2.1", 9))
            address = s.getsockname()[0]
        except OSError:
            return None
    return None if address.startswith("127.") or address == "0.0.0.0" else address


@pytest.mark.kernel
def test_a_running_kernel_answers_on_the_loopback_and_not_on_the_network(tmp_path, monkeypatch):
    import docker

    from flowfile_core.kernel.images import _kernel_image_lite

    if not _docker_ready():
        pytest.skip("Docker is not available")
    image = _kernel_image_lite()
    if not _image_present(image):
        pytest.skip(f"kernel image {image} is not present locally")

    monkeypatch.setenv("FLOWFILE_MODE", "electron")
    mgr = _manager(tmp_path)
    kernel_id = f"port-binding-{uuid.uuid4().hex[:8]}"
    kernel = KernelInfo(id=kernel_id, name=kernel_id, port=_free_kernel_port())
    mgr._kernels = {kernel_id: kernel}
    client = docker.from_env()
    container = client.containers.run(image, **mgr._build_run_kwargs(kernel_id, kernel, {}))
    try:
        mgr._wait_for_healthy_sync(kernel_id, timeout=90)
        container.reload()
        bindings = container.attrs["NetworkSettings"]["Ports"]["9999/tcp"]
        assert {b["HostIp"] for b in bindings} == {"127.0.0.1"}
        assert {int(b["HostPort"]) for b in bindings} == {kernel.port}
        assert mgr._host_port_of(container) == kernel.port

        address = _address_off_the_loopback()
        if address is not None:
            with pytest.raises(httpx.TransportError):
                httpx.get(f"http://{address}:{kernel.port}/health", timeout=3)
    finally:
        container.remove(force=True)
