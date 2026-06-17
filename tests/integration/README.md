# Integration Tests

Full Docker-based end-to-end tests that exercise the complete Flowfile stack
(core, worker, and kernel containers) via `docker compose`.

## Prerequisites

- Docker Engine
- docker compose v2

No other setup is needed — the tests build all images, start services, and
tear everything down automatically.

## Running

```bash
# Build + test in one command:
pytest -m docker_integration -v

# The test handles all building, starting, and teardown automatically.
```

By default, `pytest` **excludes** these tests (they are slow and require
Docker). Only `pytest -m docker_integration` will run them.

## CI

- Run in a separate job with an extended timeout (10 min).
- Use a unique compose project name to avoid collisions:
  ```bash
  COMPOSE_PROJECT_NAME=flowfile-ci-$RUN_ID pytest -m docker_integration -v
  ```
- Add a post-step that always runs:
  ```bash
  docker compose -p flowfile-ci-$RUN_ID down -v --remove-orphans
  ```

## What the tests do

1. **Pre-flight** — verify Docker & docker compose are available and ports
   63578 / 63579 are free.
2. **Build** — `docker compose build` for core, worker, and kernel images.
3. **Secrets** — generate one-time `FLOWFILE_INTERNAL_TOKEN` and
   `JWT_SECRET_KEY` (proves secret-passing works).
4. **Start** — `docker compose up -d flowfile-core flowfile-worker`.
5. **Auth** — obtain a JWT via `POST /auth/token`.
6. **Kernel** — create and start a kernel container, wait for idle.
7. **Flow** — import a 3-node flow (manual_input → train → predict),
   run it, poll until done.
8. **Validate** — assert success, 3 nodes completed, `predicted_y` column
   present in node 3's output.
9. **Teardown** — stop/delete kernel, `docker compose down -v`.
