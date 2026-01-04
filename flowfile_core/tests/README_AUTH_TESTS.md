# Authentication Tests Documentation

This document describes the authentication test suite for FlowFile's Docker mode implementation.

## Test Structure

The authentication test suite consists of two main test files:

### 1. `test_auth.py` - Unit Tests
Unit tests that test authentication logic without requiring Docker containers.

**Test Classes:**
- `TestPasswordUtilities` - Password hashing and verification functions
- `TestElectronModeAuth` - Electron mode auto-authentication
- `TestDockerModeAuth` - Docker mode credential validation
- `TestDockerAdminUserCreation` - Admin user bootstrap from environment variables

**Running:**
```bash
# From project root
poetry run pytest flowfile_core/tests/test_auth.py -v

# Run specific test class
poetry run pytest flowfile_core/tests/test_auth.py::TestDockerModeAuth -v

# With coverage
poetry run pytest flowfile_core/tests/test_auth.py --cov=flowfile_core.auth --cov=flowfile_core.routes.auth -v
```

### 2. `test_auth_e2e.py` - End-to-End Docker Tests
Full integration tests that build and run actual Docker containers.

**Test Classes:**
- `TestDockerE2EAuthentication` - Full authentication flow in Docker
- `TestDockerE2EWithoutAdminCredentials` - Behavior without admin credentials

**Requirements:**
- Docker must be installed and running
- Docker Python SDK (`docker` package)
- Sufficient disk space for Docker images

**Running:**
```bash
# From project root
poetry run pytest flowfile_core/tests/test_auth_e2e.py -v -s

# Run specific test
poetry run pytest flowfile_core/tests/test_auth_e2e.py::TestDockerE2EAuthentication::test_login_with_valid_admin_credentials -v -s
```

**Note:** E2E tests take longer (~2-5 minutes) because they:
1. Build the Docker image
2. Start a container
3. Wait for service initialization
4. Run tests
5. Clean up containers and images

## Test Coverage

### Authentication Scenarios Tested

#### ✅ Electron Mode
- Auto-authentication without credentials
- Credentials ignored in Electron mode

#### ✅ Docker Mode - Valid Cases
- Login with correct username and password
- Token-based authentication for protected endpoints
- Admin user creation from environment variables

#### ✅ Docker Mode - Invalid Cases
- Login fails with wrong password (401)
- Login fails with non-existent username (401)
- Login fails without username (401)
- Login fails without password (401)
- Login fails without any credentials (401)
- Protected endpoints reject unauthenticated requests (401)

#### ✅ Admin User Bootstrap
- Admin user created when `FLOWFILE_ADMIN_USER` and `FLOWFILE_ADMIN_PASSWORD` are set
- Admin user NOT created in Electron mode
- Admin user NOT created when env vars missing
- Existing users NOT overwritten
- Warning logged when credentials not provided in Docker mode

#### ✅ Password Security
- Passwords hashed with bcrypt
- Different hashes for same password (salt verification)
- Correct password verification succeeds
- Incorrect password verification fails

## CI/CD Integration

### GitHub Actions Workflow

The authentication tests run automatically in CI/CD via `.github/workflows/test-docker-auth.yml`:

**Triggers:**
- Push to `main` branch (if auth files changed)
- Pull requests to `main` (if auth files changed)
- Manual workflow dispatch

**Jobs:**
1. **Unit Tests** - Fast unit tests run first
2. **Docker E2E Tests** - Full Docker build and test
3. **Summary** - Aggregate results

**Monitored Files:**
- `flowfile_core/flowfile_core/routes/auth.py`
- `flowfile_core/flowfile_core/auth/**`
- `flowfile_core/flowfile_core/database/init_db.py`
- `flowfile_core/tests/test_auth*.py`
- `flowfile_core/Dockerfile`

## Manual Testing with Docker

### Test Docker Authentication Locally

1. **Build the image:**
```bash
docker build -f flowfile_core/Dockerfile -t flowfile-core:test .
```

2. **Run with admin credentials:**
```bash
docker run -d \
  -p 63578:63578 \
  -e FLOWFILE_MODE=docker \
  -e FLOWFILE_ADMIN_USER=admin \
  -e FLOWFILE_ADMIN_PASSWORD=testpass123 \
  -v $(pwd)/master_key.txt:/run/secrets/flowfile_master_key:ro \
  --name flowfile-test \
  flowfile-core:test
```

3. **Test authentication:**
```bash
# Should fail (no credentials)
curl -X POST http://localhost:63578/auth/token

# Should succeed
curl -X POST http://localhost:63578/auth/token \
  -d "username=admin" \
  -d "password=testpass123"

# Use token for authenticated request
TOKEN=$(curl -X POST http://localhost:63578/auth/token \
  -d "username=admin" \
  -d "password=testpass123" | jq -r '.access_token')

curl http://localhost:63578/auth/users/me \
  -H "Authorization: Bearer $TOKEN"
```

4. **Clean up:**
```bash
docker stop flowfile-test
docker rm flowfile-test
docker rmi flowfile-core:test
```

## Troubleshooting

### E2E Tests Timeout
If E2E tests timeout during container startup:
- Check Docker daemon is running: `docker ps`
- Increase timeout: Edit `CONTAINER_STARTUP_TIMEOUT` in `test_auth_e2e.py`
- Check container logs: Look for error messages in test output

### E2E Tests Fail to Build Image
- Ensure you're running from project root
- Check `master_key.txt` exists (or tests will create temporary one)
- Verify Dockerfile is valid: `docker build -f flowfile_core/Dockerfile .`

### Port Already in Use
If port 63578 is in use:
- Stop existing containers: `docker stop $(docker ps -q --filter "publish=63578")`
- Or change test port in `test_auth_e2e.py`

### Docker Permission Denied
On Linux, you may need to:
- Add user to docker group: `sudo usermod -aG docker $USER`
- Or run with sudo: `sudo poetry run pytest ...`

## Environment Variables

### For Docker Mode
- `FLOWFILE_MODE=docker` - Enable Docker authentication mode
- `FLOWFILE_ADMIN_USER` - Admin username to create on startup
- `FLOWFILE_ADMIN_PASSWORD` - Admin password to create on startup

### For Testing
- `TESTING=True` - Use temporary test database (set automatically by conftest.py)

## Test Data

### Test Admin Credentials (E2E)
- Username: `e2e_admin`
- Password: `e2e_test_password_123`

These are hardcoded in `test_auth_e2e.py` and only used for testing.

## Security Notes

- Unit tests use in-memory SQLite database
- E2E tests use isolated Docker containers
- Test credentials are never committed to production
- Master key file is temporary for tests
- All test containers are removed after tests complete

## Contributing

When adding new authentication features:

1. Add unit tests to `test_auth.py`
2. Add E2E tests to `test_auth_e2e.py` if testing Docker behavior
3. Update this README with new test scenarios
4. Ensure all tests pass locally before pushing
5. CI/CD will run full test suite on PR

## References

- [FastAPI Security Documentation](https://fastapi.tiangolo.com/tutorial/security/)
- [Passlib Documentation](https://passlib.readthedocs.io/)
- [Docker Python SDK](https://docker-py.readthedocs.io/)
