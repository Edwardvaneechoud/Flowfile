"""Fixtures for Docker-based integration tests.

Handles building images, starting/stopping compose services, authentication,
and kernel lifecycle. All fixtures are module-scoped so the heavy Docker
operations happen once per test module.
"""

import os
import secrets
import socket
import subprocess
import tempfile
import time

import httpx
import pytest

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
COMPOSE_FILE = os.path.join(REPO_ROOT, "docker-compose.yml")
CORE_URL = "http://localhost:63578"
WORKER_URL = "http://localhost:63579"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _is_port_free(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.bind(("127.0.0.1", port))
            return True
        except OSError:
            return False


def _compose(*args: str, env: dict | None = None, timeout: int = 300) -> subprocess.CompletedProcess:
    merged_env = {**os.environ, **(env or {})}
    return subprocess.run(
        ["docker", "compose", "-f", COMPOSE_FILE, *args],
        capture_output=True,
        text=True,
        timeout=timeout,
        env=merged_env,
    )


def _wait_for_service(url: str, path: str = "/health/status", timeout: float = 120) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with httpx.Client(timeout=5.0) as client:
                resp = client.get(f"{url}{path}")
                if resp.status_code == 200:
                    return True
        except (httpx.HTTPError, OSError):
            pass
        time.sleep(2)
    return False


def _dump_compose_logs(services: list[str]) -> str:
    """Capture docker compose logs for debugging on failure."""
    output_parts: list[str] = []
    for svc in services:
        result = subprocess.run(
            ["docker", "compose", "-f", COMPOSE_FILE, "logs", "--tail=100", svc],
            capture_output=True,
            text=True,
            timeout=30,
        )
        output_parts.append(f"\n{'=' * 60}\n{svc} logs:\n{'=' * 60}\n{result.stdout}")
        if result.stderr:
            output_parts.append(result.stderr)
    return "\n".join(output_parts)


def _dump_kernel_logs(kernel_id: str) -> str:
    """Capture kernel container logs for debugging."""
    result = subprocess.run(
        ["docker", "logs", f"flowfile-kernel-{kernel_id}", "--tail=100"],
        capture_output=True,
        text=True,
        timeout=30,
    )
    return f"\n{'=' * 60}\nkernel ({kernel_id}) logs:\n{'=' * 60}\n{result.stdout}\n{result.stderr}"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def compose_services():
    """Build images, start core + worker, yield env config, then tear down.

    Performs pre-flight checks (Docker available, ports free), builds all
    images, generates one-time secrets, and starts the services.
    """
    # -- Step 1: pre-flight checks --
    try:
        subprocess.run(["docker", "info"], capture_output=True, check=True, timeout=10)
    except (subprocess.CalledProcessError, FileNotFoundError, subprocess.TimeoutExpired):
        pytest.skip("Docker is not available")

    try:
        subprocess.run(
            ["docker", "compose", "version"],
            capture_output=True,
            check=True,
            timeout=10,
        )
    except (subprocess.CalledProcessError, FileNotFoundError, subprocess.TimeoutExpired):
        pytest.skip("docker compose is not available")

    if not _is_port_free(63578):
        pytest.skip("Port 63578 is in use — cannot start flowfile-core")
    if not _is_port_free(63579):
        pytest.skip("Port 63579 is in use — cannot start flowfile-worker")

    # -- Step 2: build images --
    build_core = _compose("build", "flowfile-core", "flowfile-worker", timeout=600)
    if build_core.returncode != 0:
        pytest.skip(f"Could not build core/worker images:\n{build_core.stderr}")

    build_kernel = _compose("--profile", "kernel", "build", "flowfile-kernel", timeout=600)
    if build_kernel.returncode != 0:
        pytest.skip(f"Could not build kernel image:\n{build_kernel.stderr}")

    # -- Step 3: generate one-time secrets --
    env = {
        "FLOWFILE_INTERNAL_TOKEN": secrets.token_hex(32),
        "JWT_SECRET_KEY": secrets.token_hex(32),
        "FLOWFILE_ADMIN_USER": "admin",
        "FLOWFILE_ADMIN_PASSWORD": "test-password",
    }

    # Write to a temporary .env file so compose picks them up
    env_file = tempfile.NamedTemporaryFile(
        mode="w", suffix=".env", delete=False, dir=REPO_ROOT
    )
    try:
        for key, value in env.items():
            env_file.write(f"{key}={value}\n")
        env_file.close()

        # -- Step 4: start services --
        up = _compose("--env-file", env_file.name, "up", "-d", "flowfile-core", "flowfile-worker")
        if up.returncode != 0:
            pytest.fail(f"docker compose up failed:\n{up.stderr}")

        try:
            if not _wait_for_service(CORE_URL, timeout=120):
                logs = _dump_compose_logs(["flowfile-core"])
                pytest.fail(f"Core service did not become healthy.{logs}")

            if not _wait_for_service(WORKER_URL, timeout=120):
                logs = _dump_compose_logs(["flowfile-worker"])
                pytest.fail(f"Worker service did not become healthy.{logs}")

            yield env
        finally:
            # -- Step 11: teardown (always runs) --
            _compose("down", "-v", "--remove-orphans")
    finally:
        os.unlink(env_file.name)


@pytest.fixture(scope="module")
def auth_client(compose_services):
    """Authenticated httpx client pointed at the core API.

    Uses the admin credentials generated by compose_services.
    """
    env = compose_services
    with httpx.Client(base_url=CORE_URL, timeout=30.0) as client:
        resp = client.post(
            "/auth/token",
            data={
                "username": env["FLOWFILE_ADMIN_USER"],
                "password": env["FLOWFILE_ADMIN_PASSWORD"],
            },
        )
        resp.raise_for_status()
        token = resp.json()["access_token"]
        client.headers["Authorization"] = f"Bearer {token}"
        yield client


@pytest.fixture(scope="module")
def kernel_ready(auth_client):
    """Create and start the e2e-test kernel, yield its ID, then stop + delete."""
    kernel_id = "e2e-test"

    # Step 6: create kernel
    resp = auth_client.post(
        "/kernels/",
        json={"id": kernel_id, "name": "E2E Integration Test"},
    )
    resp.raise_for_status()

    # Step 7: start kernel
    resp = auth_client.post(f"/kernels/{kernel_id}/start")
    resp.raise_for_status()

    # Wait for kernel to become idle
    deadline = time.monotonic() + 120
    info = None
    while time.monotonic() < deadline:
        resp = auth_client.get(f"/kernels/{kernel_id}")
        info = resp.json()
        if info.get("state") == "idle":
            break
        time.sleep(2)
    else:
        kernel_logs = _dump_kernel_logs(kernel_id)
        pytest.fail(f"Kernel did not become idle: {info}{kernel_logs}")

    yield kernel_id

    # Cleanup: stop + delete kernel
    try:
        auth_client.post(f"/kernels/{kernel_id}/stop")
    except Exception:
        pass
    try:
        auth_client.delete(f"/kernels/{kernel_id}")
    except Exception:
        pass
