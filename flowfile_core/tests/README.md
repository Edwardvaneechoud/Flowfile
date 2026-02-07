# Flowfile Core Tests

This directory contains tests for the Flowfile Core package.

## Kernel Integration Tests

Kernel integration tests verify that the Docker-based kernel system works correctly. These tests require Docker to be available and are marked with `@pytest.mark.kernel`.

### Test Fixtures

There are **two session-scoped kernel fixtures** that serve different purposes:

#### `kernel_manager`
- **Used by:** `test_kernel_integration.py`, `test_kernel_persistence_integration.py`
- **What it does:** Builds the kernel Docker image, creates a `KernelManager`, and starts a kernel container
- **Kernel ID:** `integration-test`
- **Use when:** Testing kernel execution, artifacts within a flow, persistence, etc. - anything that doesn't require communication with the Core API

#### `kernel_manager_with_core`
- **Used by:** `test_global_artifacts_kernel_integration.py`
- **What it does:** Same as above, PLUS starts the Core API server and sets up authentication tokens for kernel ↔ Core communication
- **Kernel ID:** `integration-test-core`
- **Use when:** Testing global artifacts (`publish_global()`, `get_global()`, `list_global_artifacts()`, `delete_global_artifact()`) which require the kernel to make HTTP calls to Core

### Why Two Fixtures?

The fixtures use **different kernel IDs** to avoid conflicts when both run in the same test session:

1. **Separation of concerns:** Kernel-only tests don't need Core running, so we avoid the overhead of starting it
2. **Faster feedback:** Tests that only need the kernel can run without waiting for Core to start
3. **Isolation:** Each fixture manages its own kernel instance, preventing test interference

### Running Kernel Tests

```bash
# Run all kernel tests
pytest flowfile_core/tests -m kernel -v

# Run only kernel-only tests (no Core)
pytest flowfile_core/tests/flowfile/test_kernel_integration.py -v
pytest flowfile_core/tests/flowfile/test_kernel_persistence_integration.py -v

# Run only global artifacts tests (requires Core)
pytest flowfile_core/tests/flowfile/test_global_artifacts_kernel_integration.py -v
```

### CI Workflow

The kernel integration tests run in GitHub Actions via `.github/workflows/test-kernel-integration.yml`. The workflow:

1. Sets up Python and Docker
2. Builds the `flowfile-kernel` Docker image
3. Runs tests with `-m kernel` marker
4. Cleans up Docker resources

### Writing New Kernel Tests

1. **For kernel-only tests:** Use the `kernel_manager` fixture
   ```python
   pytestmark = pytest.mark.kernel

   def test_my_kernel_feature(self, kernel_manager: tuple[KernelManager, str]):
       manager, kernel_id = kernel_manager
       # Your test code here
   ```

2. **For tests needing Core API:** Use the `kernel_manager_with_core` fixture
   ```python
   pytestmark = pytest.mark.kernel

   def test_my_global_artifact_feature(self, kernel_manager_with_core: tuple[KernelManager, str]):
       manager, kernel_id = kernel_manager_with_core
       # Your test code here (can use publish_global, get_global, etc.)
   ```

### Troubleshooting

- **Tests skipped locally:** Docker must be available. The fixtures skip tests if Docker isn't running.
- **Tests fail in CI:** The fixtures fail loudly in CI (detected via `CI` or `TEST_MODE` env vars) to surface actual errors instead of silently skipping.
- **"Kernel already exists" error:** This can happen if a previous test run didn't clean up properly. Run `docker rm -f flowfile-kernel-integration-test flowfile-kernel-integration-test-core` to clean up.
