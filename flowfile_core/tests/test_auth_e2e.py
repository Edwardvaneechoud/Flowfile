"""End-to-end tests for Docker authentication.

This test suite builds the actual Docker image and tests authentication
in a real containerized environment.
"""

import os
import time
import pytest
import requests
import docker
from pathlib import Path
from typing import Optional, Dict, Tuple

# Test configuration
TEST_ADMIN_USERNAME = "e2e_admin"
TEST_ADMIN_PASSWORD = "e2e_test_password_123"
FLOWFILE_CORE_PORT = 63578
CONTAINER_STARTUP_TIMEOUT = 120  # seconds
HEALTH_CHECK_INTERVAL = 2  # seconds


def is_docker_available() -> bool:
    """Check if Docker is available and running."""
    try:
        client = docker.from_env()
        client.ping()
        return True
    except Exception:
        return False


@pytest.fixture(scope="module")
def docker_client():
    """Provide a Docker client for the test session."""
    if not is_docker_available():
        pytest.skip("Docker is not available")

    client = docker.from_env()
    yield client
    client.close()


@pytest.fixture(scope="module")
def project_root() -> Path:
    """Get the project root directory."""
    # Navigate from flowfile_core/tests to project root
    current_file = Path(__file__).resolve()
    # Go up: test_auth_e2e.py -> tests -> flowfile_core -> Flowfile
    return current_file.parent.parent.parent


@pytest.fixture(scope="module")
def docker_image(docker_client, project_root):
    """Build the Docker image for testing."""
    print(f"\n{'='*60}")
    print(f"Building Docker image from: {project_root}")
    print(f"{'='*60}")

    # Ensure master_key.txt exists
    master_key_path = project_root / "master_key.txt"
    if not master_key_path.exists():
        # Create a temporary master key for testing
        master_key_path.write_text("test-master-key-for-e2e-testing-only-12345")
        created_master_key = True
    else:
        created_master_key = False

    try:
        # Build the image
        image, build_logs = docker_client.images.build(
            path=str(project_root),
            dockerfile="flowfile_core/Dockerfile",
            tag="flowfile-core:e2e-test",
            rm=True,  # Remove intermediate containers
            forcerm=True  # Always remove intermediate containers
        )

        # Print build logs
        for log in build_logs:
            if 'stream' in log:
                print(log['stream'].strip())

        print(f"\n{'='*60}")
        print(f"Docker image built successfully: {image.tags}")
        print(f"{'='*60}\n")

        yield image

        # Cleanup: Remove the test image
        print(f"\nRemoving test image: {image.tags}")
        docker_client.images.remove(image.id, force=True)

    finally:
        # Cleanup: Remove temporary master key if we created it
        if created_master_key and master_key_path.exists():
            master_key_path.unlink()


def wait_for_service(
    url: str,
    timeout: int = CONTAINER_STARTUP_TIMEOUT,
    interval: int = HEALTH_CHECK_INTERVAL
) -> bool:
    """Wait for a service to become available."""
    start_time = time.time()

    while time.time() - start_time < timeout:
        try:
            response = requests.get(url, timeout=5)
            if response.status_code in [200, 404]:  # 404 is ok, means server is up
                return True
        except requests.exceptions.RequestException:
            pass

        elapsed = time.time() - start_time
        print(f"Waiting for service... ({elapsed:.1f}s / {timeout}s)")
        time.sleep(interval)

    return False


@pytest.fixture(scope="module")
def running_container(docker_client, docker_image, project_root):
    """Start a Docker container with the test image."""
    print(f"\n{'='*60}")
    print(f"Starting Docker container...")
    print(f"{'='*60}")

    # Prepare volumes
    volumes = {
        str(project_root / "master_key.txt"): {
            'bind': '/run/secrets/flowfile_master_key',
            'mode': 'ro'
        }
    }

    # Environment variables for Docker mode with admin user
    environment = {
        'FLOWFILE_MODE': 'docker',
        'FLOWFILE_ADMIN_USER': TEST_ADMIN_USERNAME,
        'FLOWFILE_ADMIN_PASSWORD': TEST_ADMIN_PASSWORD,
        'FLOWFILE_STORAGE_DIR': '/app/internal_storage',
        'FLOWFILE_USER_DATA_DIR': '/app/user_data',
        'RUNNING_IN_DOCKER': 'true',
        'PYTHONDONTWRITEBYTECODE': '1',
        'PYTHONUNBUFFERED': '1'
    }

    # Start the container
    container = docker_client.containers.run(
        image=docker_image.id,
        detach=True,
        ports={f'{FLOWFILE_CORE_PORT}/tcp': FLOWFILE_CORE_PORT},
        environment=environment,
        volumes=volumes,
        name=f"flowfile-e2e-test-{int(time.time())}",
        remove=True  # Auto-remove when stopped
    )

    try:
        print(f"Container started: {container.name} (ID: {container.short_id})")

        # Wait for the service to be ready
        service_url = f"http://localhost:{FLOWFILE_CORE_PORT}/docs"
        print(f"\nWaiting for service at {service_url}...")

        if not wait_for_service(service_url):
            # Print container logs for debugging
            logs = container.logs(tail=50).decode('utf-8')
            print(f"\nContainer logs:\n{logs}")
            pytest.fail(f"Service did not start within {CONTAINER_STARTUP_TIMEOUT} seconds")

        print(f"\n{'='*60}")
        print(f"Container is ready!")
        print(f"{'='*60}\n")

        # Give it a moment to fully initialize
        time.sleep(3)

        yield container

    finally:
        # Cleanup: Stop and remove the container
        print(f"\nStopping container: {container.name}")
        try:
            logs = container.logs(tail=100).decode('utf-8')
            print(f"\nFinal container logs:\n{logs}")
        except Exception as e:
            print(f"Could not retrieve logs: {e}")

        try:
            container.stop(timeout=10)
            print(f"Container stopped successfully")
        except Exception as e:
            print(f"Error stopping container: {e}")
            try:
                container.kill()
            except Exception:
                pass


class TestDockerE2EAuthentication:
    """End-to-end tests for Docker authentication."""

    @pytest.fixture(autouse=True)
    def setup(self, running_container):
        """Setup fixture that ensures container is running."""
        self.container = running_container
        self.base_url = f"http://localhost:{FLOWFILE_CORE_PORT}"

    def test_container_is_running(self):
        """Test that the container is running."""
        assert self.container.status in ['running', 'created']
        self.container.reload()
        assert self.container.status == 'running'

    def test_service_is_accessible(self):
        """Test that the service responds to requests."""
        response = requests.get(f"{self.base_url}/docs", timeout=10)
        assert response.status_code == 200

    def test_authentication_endpoint_exists(self):
        """Test that the authentication endpoint exists."""
        # Try to access without credentials (should fail in Docker mode)
        response = requests.post(f"{self.base_url}/auth/token", timeout=10)
        # Should get 422 (validation error) or 401 (unauthorized)
        assert response.status_code in [401, 422]

    def test_login_with_valid_admin_credentials(self):
        """Test successful login with admin credentials."""
        response = requests.post(
            f"{self.base_url}/auth/token",
            data={
                'username': TEST_ADMIN_USERNAME,
                'password': TEST_ADMIN_PASSWORD
            },
            timeout=10
        )

        assert response.status_code == 200, f"Response: {response.text}"

        data = response.json()
        assert 'access_token' in data
        assert 'token_type' in data
        assert data['token_type'] == 'bearer'
        assert len(data['access_token']) > 0

        # Store token for subsequent tests
        self.token = data['access_token']

    def test_login_with_invalid_password(self):
        """Test login fails with invalid password."""
        response = requests.post(
            f"{self.base_url}/auth/token",
            data={
                'username': TEST_ADMIN_USERNAME,
                'password': 'wrongpassword'
            },
            timeout=10
        )

        assert response.status_code == 401
        data = response.json()
        assert 'detail' in data
        assert data['detail'] == 'Incorrect username or password'

    def test_login_with_invalid_username(self):
        """Test login fails with non-existent username."""
        response = requests.post(
            f"{self.base_url}/auth/token",
            data={
                'username': 'nonexistentuser',
                'password': TEST_ADMIN_PASSWORD
            },
            timeout=10
        )

        assert response.status_code == 401
        data = response.json()
        assert data['detail'] == 'Incorrect username or password'

    def test_login_without_credentials(self):
        """Test login fails without credentials."""
        response = requests.post(f"{self.base_url}/auth/token", timeout=10)

        assert response.status_code in [401, 422]

    def test_authenticated_request(self):
        """Test making an authenticated request with the token."""
        # First login to get a token
        login_response = requests.post(
            f"{self.base_url}/auth/token",
            data={
                'username': TEST_ADMIN_USERNAME,
                'password': TEST_ADMIN_PASSWORD
            },
            timeout=10
        )

        assert login_response.status_code == 200
        token = login_response.json()['access_token']

        # Try to access a protected endpoint
        headers = {'Authorization': f'Bearer {token}'}
        response = requests.get(
            f"{self.base_url}/auth/users/me",
            headers=headers,
            timeout=10
        )

        # Should succeed with valid token
        assert response.status_code == 200
        user_data = response.json()
        assert user_data['username'] == TEST_ADMIN_USERNAME

    def test_unauthenticated_request_fails(self):
        """Test that protected endpoints reject requests without token."""
        response = requests.get(
            f"{self.base_url}/auth/users/me",
            timeout=10
        )

        # Should fail without authentication
        assert response.status_code in [401, 403]

    def test_container_logs_show_admin_user_creation(self):
        """Test that container logs show admin user was created."""
        logs = self.container.logs().decode('utf-8')

        # Should contain log about admin user creation
        assert 'Admin user' in logs or 'admin' in logs.lower()


class TestDockerE2EWithoutAdminCredentials:
    """Test Docker container behavior when admin credentials are not provided."""

    def test_container_starts_without_admin_env_vars(self, docker_client, docker_image, project_root):
        """Test that container starts even without admin credentials (with warning)."""
        print(f"\n{'='*60}")
        print(f"Testing container without admin credentials...")
        print(f"{'='*60}")

        volumes = {
            str(project_root / "master_key.txt"): {
                'bind': '/run/secrets/flowfile_master_key',
                'mode': 'ro'
            }
        }

        # Environment without admin credentials
        environment = {
            'FLOWFILE_MODE': 'docker',
            # Intentionally omitting FLOWFILE_ADMIN_USER and FLOWFILE_ADMIN_PASSWORD
            'FLOWFILE_STORAGE_DIR': '/app/internal_storage',
            'FLOWFILE_USER_DATA_DIR': '/app/user_data',
            'RUNNING_IN_DOCKER': 'true',
        }

        container = docker_client.containers.run(
            image=docker_image.id,
            detach=True,
            ports={f'{FLOWFILE_CORE_PORT}/tcp': FLOWFILE_CORE_PORT + 1},  # Different port
            environment=environment,
            volumes=volumes,
            name=f"flowfile-e2e-no-admin-{int(time.time())}",
            remove=True
        )

        try:
            # Wait a bit for startup
            time.sleep(5)

            # Check logs for warning message
            logs = container.logs().decode('utf-8')

            # Should contain warning about missing admin credentials
            assert 'FLOWFILE_ADMIN_USER' in logs or 'FLOWFILE_ADMIN_PASSWORD' in logs
            assert 'not set' in logs or 'warning' in logs.lower()

            print("\n✓ Container correctly warns about missing admin credentials")

        finally:
            try:
                container.stop(timeout=5)
            except Exception:
                pass


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
