# Variables
PYTHON_DIR := .
FRONTEND_DIR := flowfile_frontend
TAURI_DIR := $(FRONTEND_DIR)/src-tauri
KEY_FILE := master_key.txt

# Detect OS
ifeq ($(OS),Windows_NT)
	RMRF := del /f /s /q
	RMDIR := rmdir /s /qg
	CD := cd /d
	POETRY_RUN := poetry run
	NULL_OUTPUT := >NUL 2>NUL
	CHECK_POETRY := poetry check >NUL 2>NUL || (echo Lock file needs updating. Running poetry lock... && poetry lock --no-update)
else
	RMRF := rm -rf
	RMDIR := rm -rf
	CD := cd
	POETRY_RUN := poetry run
	NULL_OUTPUT := 2>/dev/null
	CHECK_POETRY := if ! poetry check 2>/dev/null; then echo "Lock file needs updating. Running poetry lock..."; poetry lock --no-update; fi
endif

# Ports the local servers listen on: core, worker, vite dev, vite preview.
FLOWFILE_PORTS := 63578 63579 8080 4173
# Kills the core/worker/vite processes listening on those ports (POSIX); any other listener, such as
# Docker Desktop publishing the compose stack's ports, is left alone.
STOP_LISTENERS = command -v lsof >/dev/null || command -v ss >/dev/null || echo "Neither lsof nor ss found: nothing stopped."; \
	for port in $(FLOWFILE_PORTS); do \
		for pid in $$(lsof -nP -t -iTCP:$$port -sTCP:LISTEN 2>/dev/null || \
			ss -Hltnp "sport = :$$port" 2>/dev/null | grep -o 'pid=[0-9]*' | cut -d= -f2 | sort -u); do \
			case "$$(ps -p $$pid -o args= 2>/dev/null)" in \
				*flowfile_core*|*flowfile_worker*|*vite*) echo "Stopping PID $$pid (port $$port)"; kill $$pid 2>/dev/null;; \
				*) echo "Leaving PID $$pid on port $$port alone: not a Flowfile or Vite process";; \
			esac; \
		done; \
	done; true

# Kernel image flavour to (re)build locally: base (default), ml, or lite.
KERNEL_FLAVOUR ?= base
ifeq ($(KERNEL_FLAVOUR),ml)
KERNEL_BUILD_ARG := --build-arg EXTRAS=ml
else ifeq ($(KERNEL_FLAVOUR),lite)
KERNEL_BUILD_ARG := --build-arg SLIM_CONSTRAINTS=true
else
KERNEL_BUILD_ARG :=
endif

# Default target: install dependencies, build Python services, stage sidecars, build Tauri app, generate key
all: install_python_deps build_python_services rename_sidecars sign_sidecars build_tauri_app generate_key

# Update Poetry lock file
update_lock:
	@echo "Updating Poetry lock file..."
	poetry lock
	@echo "Lock file updated."

# Force update Poetry lock file without updating dependencies
force_lock:
	@echo "Forcing Poetry lock file update without updating dependencies..."
	poetry lock --no-update
	@echo "Lock file updated without dependency updates."

# Install Python dependencies with Poetry
install_python_deps:
	@echo "Checking Poetry lock file..."
	@$(CHECK_POETRY)
	@echo "Installing Python dependencies with Poetry..."
	poetry install --with build
	@echo "Python dependencies installed."

# Build Python services
build_python_services: install_python_deps
	@echo "Building Python services..."
	$(POETRY_RUN) build_backends
	@echo "Python services built successfully."

# Stage PyInstaller outputs into the Tauri sidecar layout (binaries/<name>-<triple>)
rename_sidecars:
	@echo "Staging sidecars for Tauri..."
	$(POETRY_RUN) python tools/rename_sidecar.py
	@echo "Sidecars staged."

# Convenience: build PyInstaller services AND stage them for Tauri.
# Day-to-day iteration usually wants both — split targets exist for
# diagnostics (test_built_services, measure_bundle, --triple overrides).
services: build_python_services rename_sidecars

# Sign bundled sidecars for macOS notarization. No-op off macOS or when
# APPLE_SIGNING_IDENTITY is unset, so it's safe on every platform/build.
sign_sidecars:
ifeq ($(OS),Windows_NT)
	@echo "sign_sidecars: skipped (Windows)"
else
	@bash tools/sign_macos_sidecars.sh
endif

# Detach stale Flowfile DMG volumes + temp images left by a previous (failed)
# build. Tauri's bundle_dmg.sh mounts the new image at /Volumes/Flowfile; if a
# leftover already occupies that name macOS remaps the mount and the script
# (set -e) fails. Detach by device node so the "Flowfile 1"/space variants are
# caught too. No-op on non-macOS.
clean_dmg_mounts:
ifeq ($(shell uname),Darwin)
	@echo "Cleaning stale Flowfile DMG mounts..."
	-@hdiutil info | awk '/\/Volumes\/Flowfile/ {print $$1}' | while read dev; do hdiutil detach -force "$$dev" >/dev/null 2>&1 || true; done
	-@rm -f "$(FRONTEND_DIR)/src-tauri/target/release/bundle/macos/rw."*.dmg 2>/dev/null || true
endif

# Build Tauri app
build_tauri_app: clean_dmg_mounts sign_sidecars
	@echo "Building Tauri app..."
	$(CD) "$(FRONTEND_DIR)" && npm install
	$(CD) "$(FRONTEND_DIR)" && npm run build
	@echo "Tauri app built successfully."

# Platform-specific Tauri builds
build_tauri_win:
	$(CD) "$(FRONTEND_DIR)" && npx tauri build --target x86_64-pc-windows-msvc

build_tauri_mac: clean_dmg_mounts sign_sidecars
	$(CD) "$(FRONTEND_DIR)" && npx tauri build

build_tauri_mac_arm: clean_dmg_mounts sign_sidecars
	$(CD) "$(FRONTEND_DIR)" && npx tauri build --target aarch64-apple-darwin

build_tauri_mac_intel: clean_dmg_mounts sign_sidecars
	$(CD) "$(FRONTEND_DIR)" && npx tauri build --target x86_64-apple-darwin

build_tauri_linux:
	$(CD) "$(FRONTEND_DIR)" && npx tauri build --target x86_64-unknown-linux-gnu

# Show sizes of bundled outputs — useful to track PyInstaller optimization wins
measure_bundle:
	@echo "Service bundle sizes:"
ifeq ($(OS),Windows_NT)
	@if exist "services_dist" dir services_dist
else
	@du -sh services_dist 2>/dev/null || echo "  (services_dist missing — run build_python_services)"
	@du -sh services_dist/flowfile_core services_dist/flowfile_worker services_dist/_internal 2>/dev/null || true
	@echo ""
	@echo "Tauri sidecar binaries:"
	@du -sh "$(TAURI_DIR)/binaries" 2>/dev/null || echo "  (binaries missing — run rename_sidecars)"
endif

# Smoke-test the built PyInstaller binaries before bundling
test_built_services:
	@echo "Smoke-testing built services..."
	@bash -c '\
		set -e; \
		./services_dist/flowfile_core & CORE_PID=$$!; \
		./services_dist/flowfile_worker & WORKER_PID=$$!; \
		sleep 8; \
		echo "core: $$(curl -s -o /dev/null -w "%{http_code}" http://127.0.0.1:63578/docs)"; \
		echo "worker: $$(curl -s -o /dev/null -w "%{http_code}" http://127.0.0.1:63579/docs)"; \
		curl -s -X POST http://127.0.0.1:63578/shutdown >/dev/null || true; \
		curl -s -X POST http://127.0.0.1:63579/shutdown >/dev/null || true; \
		wait $$CORE_PID $$WORKER_PID 2>/dev/null || true; \
	'

# Clean up build artifacts
clean:
	@echo "Cleaning up build artifacts..."
ifeq ($(OS),Windows_NT)
	@if exist "services_dist" $(RMDIR) "services_dist"
	@if exist "build" $(RMDIR) "build"
	@if exist "$(FRONTEND_DIR)\build" $(RMDIR) "$(FRONTEND_DIR)\build"
	@if exist "$(FRONTEND_DIR)\node_modules" $(RMDIR) "$(FRONTEND_DIR)\node_modules"
	@if exist "$(TAURI_DIR)\target" $(RMDIR) "$(TAURI_DIR)\target"
	@if exist "$(TAURI_DIR)\binaries" $(RMDIR) "$(TAURI_DIR)\binaries"
else
	$(RMRF) services_dist/ build/ $(FRONTEND_DIR)/build/ $(FRONTEND_DIR)/node_modules/ $(TAURI_DIR)/target/ $(TAURI_DIR)/binaries/
endif
	@echo "Clean up done."

generate_key:
	@echo "Checking for master key..."
ifeq ($(OS),Windows_NT)
	@if not exist "$(KEY_FILE)" (echo Generating new master key... && $(POETRY_RUN) python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())" > $(KEY_FILE) && echo Master key generated successfully.)
else
	@if [ ! -f "$(KEY_FILE)" ]; then \
		echo "Generating new master key..."; \
		$(POETRY_RUN) python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())" > $(KEY_FILE); \
		chmod 600 $(KEY_FILE); \
		echo "Master key generated successfully."; \
	else \
		echo "Master key already exists."; \
	fi
endif

# Force regenerate master key
force_key:
	@echo "Regenerating master key..."
	$(POETRY_RUN) python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())" > $(KEY_FILE)
ifeq ($(OS),Windows_NT)
	@echo Master key regenerated successfully.
else
	@chmod 600 $(KEY_FILE)
	@echo "Master key regenerated successfully."
endif

# E2E Testing (web tests only — Tauri E2E via tauri-driver is a follow-up)
install_e2e:
	@echo "Installing E2E test dependencies..."
	$(CD) "$(FRONTEND_DIR)" && npm ci
	$(CD) "$(FRONTEND_DIR)" && npx playwright install chromium
	@echo "E2E dependencies installed."

test_e2e: install_python_deps
	@echo "Running E2E tests..."
	@echo "Building frontend..."
	$(CD) "$(FRONTEND_DIR)" && npm run build:web
	@echo "Starting backend..."
ifeq ($(OS),Windows_NT)
	start /B $(POETRY_RUN) flowfile_core
	start /B $(POETRY_RUN) flowfile_worker
	timeout /t 5 /nobreak >NUL
	@echo "Starting frontend preview..."
	$(CD) "$(FRONTEND_DIR)" && start /B npm run preview:web
	timeout /t 3 /nobreak >NUL
else
	$(POETRY_RUN) flowfile_core &
	$(POETRY_RUN) flowfile_worker &
	sleep 5
	@echo "Starting frontend preview..."
	$(CD) "$(FRONTEND_DIR)" && npm run preview:web &
	sleep 3
endif
	@echo "Running tests..."
ifeq ($(OS),Windows_NT)
	$(CD) "$(FRONTEND_DIR)" && TEST_URL=http://localhost:4173 npx playwright test tests/web-flow.spec.ts tests/csp.spec.ts || true
	@$(MAKE) stop_servers
else
	@status=0; \
	($(CD) "$(FRONTEND_DIR)" && TEST_URL=http://localhost:4173 PLAYWRIGHT_HTML_OPEN=never npx playwright test tests/web-flow.spec.ts tests/csp.spec.ts) || status=$$?; \
	$(STOP_LISTENERS); exit $$status
endif

test_e2e_dev: install_python_deps
	@echo "Running E2E tests (dev mode)..."
	@echo "Starting backend..."
ifeq ($(OS),Windows_NT)
	start /B $(POETRY_RUN) flowfile_core
	start /B $(POETRY_RUN) flowfile_worker
	timeout /t 5 /nobreak >NUL
	@echo "Starting frontend dev server..."
	$(CD) "$(FRONTEND_DIR)" && start /B npm run dev:web
	timeout /t 3 /nobreak >NUL
else
	$(POETRY_RUN) flowfile_core &
	$(POETRY_RUN) flowfile_worker &
	sleep 5
	@echo "Starting frontend dev server..."
	$(CD) "$(FRONTEND_DIR)" && npm run dev:web &
	sleep 3
endif
	@echo "Running tests..."
ifeq ($(OS),Windows_NT)
	$(CD) "$(FRONTEND_DIR)" && npx playwright test tests/web-flow.spec.ts tests/csp.spec.ts || true
	@$(MAKE) stop_servers
else
	@status=0; \
	($(CD) "$(FRONTEND_DIR)" && PLAYWRIGHT_HTML_OPEN=never npx playwright test tests/web-flow.spec.ts tests/csp.spec.ts) || status=$$?; \
	$(STOP_LISTENERS); exit $$status
endif

stop_servers:
	@echo "Stopping servers..."
ifeq ($(OS),Windows_NT)
	-@taskkill /F /IM python.exe $(NULL_OUTPUT)
	-@taskkill /F /IM node.exe $(NULL_OUTPUT)
else
	-@$(STOP_LISTENERS)
endif
	@echo "Servers stopped."

# Cloud storage E2E (macOS/Linux + Docker): MinIO, then an isolated core/worker/preview on free ports
# with a MinIO-only AWS profile; runs tests/cloud_e2e and cloud-storage-flow.spec.ts, kills only its PIDs.
# Servers use `poetry run` from empty cwds: -P (Poetry 2) / -C (1.x) find the project without a chdir.
test_e2e_cloud: SHELL := /bin/bash
test_e2e_cloud:
ifeq ($(OS),Windows_NT)
	@echo "test_e2e_cloud needs bash and Docker; run it on macOS or Linux." && exit 1
else
	@set -uo pipefail; \
	repo=$$(pwd); \
	py=$$($(POETRY_RUN) python -c 'import sys; print(sys.executable)') || exit 1; \
	venv=$$(dirname "$$(dirname "$$py")"); \
	poetry=$$(command -v poetry); \
	if "$$poetry" --version | grep -q 'version 1\.'; then project=(-C "$$repo"); else project=(-P "$$repo"); fi; \
	tmp=$$(mktemp -d "$${TMPDIR:-/tmp}/flowfile-e2e-cloud.XXXXXX"); \
	run_id=make$$(date +%s); \
	pids=""; \
	cleanup() { \
		status=$$?; \
		for pid in $$pids; do kill "$$pid" 2>/dev/null; done; \
		for pid in $$pids; do \
			for _ in $$(seq 50); do kill -0 "$$pid" 2>/dev/null || break; sleep 0.2; done; \
			kill -9 "$$pid" 2>/dev/null; \
		done; \
		(cd "$$repo" && "$$py" -c 'import sys; from test_utils.s3.fixtures import get_minio_client; c = get_minio_client(); pages = c.get_paginator("list_objects_v2").paginate(Bucket="flowfile-test", Prefix=sys.argv[1]); keys = [{"Key": o["Key"]} for p in pages for o in p.get("Contents", [])]; [c.delete_objects(Bucket="flowfile-test", Delete={"Objects": keys[i : i + 1000]}) for i in range(0, len(keys), 1000)]' "cloud-e2e-$$run_id/") \
			|| echo "Could not clean s3://flowfile-test/cloud-e2e-$$run_id/"; \
		if [ $$status -eq 0 ]; then rm -rf "$$tmp"; else echo "Server logs kept in $$tmp"; fi; \
		exit $$status; \
	}; \
	trap cleanup EXIT; trap 'exit 130' INT TERM; \
	wait_for() { \
		for _ in $$(seq 240); do \
			curl -sf -o /dev/null "$$1" && return 0; \
			kill -0 "$$2" 2>/dev/null || { echo "$$3 exited early:"; tail -40 "$$tmp/$$3.log"; return 1; }; \
			sleep 0.5; \
		done; \
		echo "$$3 did not come up at $$1:"; tail -40 "$$tmp/$$3.log"; return 1; \
	}; \
	$(POETRY_RUN) start_minio && $(POETRY_RUN) seed_cloud_e2e || exit 1; \
	mkdir -p "$$tmp"/{home,aws,core_cwd,worker_cwd}; \
	printf '[default]\naws_access_key_id = minioadmin\naws_secret_access_key = minioadmin\n' > "$$tmp/aws/credentials"; \
	printf '[default]\nregion = us-east-1\n' > "$$tmp/aws/config"; \
	read -r core worker web < <("$$py" -c 'import socket; s = [socket.socket() for _ in range(3)]; [x.bind(("127.0.0.1", 0)) for x in s]; print(*[x.getsockname()[1] for x in s])'); \
	server_env=(env -i PATH="$$PATH" HOME="$$tmp/home" VIRTUAL_ENV="$$venv" TMPDIR="$${TMPDIR:-/tmp}" LANG="$${LANG:-en_US.UTF-8}" \
		FLOWFILE_MODE=electron FLOWFILE_DB_PATH="$$tmp/catalog.db" FLOWFILE_STORAGE_DIR="$$tmp/storage" \
		FLOWFILE_SECURE_STORAGE_PATH="$$tmp/secure" FLOWFILE_SHARED_DIR="$$tmp/shared" \
		FLOWFILE_TELEMETRY=0 FLOWFILE_KERNEL_WARMUP=0 FLOWFILE_KERNEL_GC=0 \
		WORKER_HOST=127.0.0.1 CORE_HOST=127.0.0.1 CORE_PORT=$$core FLOWFILE_WORKER_PORT=$$worker \
		AWS_SHARED_CREDENTIALS_FILE="$$tmp/aws/credentials" AWS_CONFIG_FILE="$$tmp/aws/config" \
		AWS_ENDPOINT_URL=http://localhost:9000 AWS_ALLOW_HTTP=true AWS_EC2_METADATA_DISABLED=true); \
	echo "Starting worker :$$worker and core :$$core (logs in $$tmp)"; \
	(cd "$$tmp/worker_cwd" && exec "$${server_env[@]}" "$$poetry" "$${project[@]}" run flowfile_worker --port $$worker --core-port $$core) > "$$tmp/worker.log" 2>&1 & \
	pids="$$pids $$!"; worker_pid=$$!; \
	(cd "$$tmp/core_cwd" && exec "$${server_env[@]}" "$$poetry" "$${project[@]}" run flowfile_core --host 127.0.0.1 --port $$core --worker-port $$worker) > "$$tmp/core.log" 2>&1 & \
	pids="$$pids $$!"; core_pid=$$!; \
	wait_for "http://127.0.0.1:$$worker/docs" $$worker_pid worker || exit 1; \
	wait_for "http://127.0.0.1:$$core/health/status" $$core_pid core || exit 1; \
	echo "Building the web frontend..."; \
	(cd "$(FRONTEND_DIR)" && node_modules/.bin/vite build --config vite.config.mjs --outDir "$$tmp/web" --emptyOutDir --logLevel error) || exit 1; \
	(cd "$(FRONTEND_DIR)" && FLOWFILE_CORE_PORT=$$core exec node_modules/.bin/vite preview --config vite.config.mjs --outDir "$$tmp/web" --host 127.0.0.1 --port $$web --strictPort) > "$$tmp/preview.log" 2>&1 & \
	pids="$$pids $$!"; preview_pid=$$!; \
	wait_for "http://127.0.0.1:$$web/" $$preview_pid preview || exit 1; \
	rc=0; \
	echo "Running the cloud storage pytest suite..."; \
	$(POETRY_RUN) pytest tests/cloud_e2e -m cloud_e2e -p no:cacheprovider || rc=1; \
	echo "Running the cloud storage Playwright spec against http://127.0.0.1:$$web ..."; \
	(cd "$(FRONTEND_DIR)" && TEST_URL=http://127.0.0.1:$$web API_URL=http://127.0.0.1:$$core E2E_RUN_ID=$$run_id \
		E2E_AWS_PROFILE_CONFIGURED=1 PLAYWRIGHT_HTML_OPEN=never \
		npm run test:cloud -- --reporter=list,html) || rc=1; \
	for dir in core_cwd worker_cwd; do \
		if [ -n "$$(ls -A "$$tmp/$$dir")" ]; then echo "A server wrote into its working directory ($$dir):"; ls -A "$$tmp/$$dir"; rc=1; fi; \
	done; \
	exit $$rc
endif

# Remove all local kernels: their Docker containers + per-kernel derived images,
# and (when Core is stopped) their catalog-DB records. See tools/clean_kernels.py.
clean_kernels:
	$(POETRY_RUN) python tools/clean_kernels.py

# Same as clean_kernels, but ALSO removes the kernel flavour images (base/ml/lite,
# local builds + pulled published tags). Core re-pulls/rebuilds them on demand.
clean_kernel_images:
	$(POETRY_RUN) python tools/clean_kernels.py --images

# Remove and rebuild a local kernel image so it picks up current source (e.g. a
# bumped runtime __version__). Flavour via KERNEL_FLAVOUR=base|ml|lite (default base).
# Point core at the result with FLOWFILE_KERNEL_IMAGE_BASE=flowfile-kernel-base:local.
rebuild_kernel:
	@echo "Rebuilding flowfile-kernel-$(KERNEL_FLAVOUR):local ..."
	-@docker rmi -f flowfile-kernel-$(KERNEL_FLAVOUR):local $(NULL_OUTPUT)
	docker build $(KERNEL_BUILD_ARG) -t flowfile-kernel-$(KERNEL_FLAVOUR):local kernel_runtime/
	@echo "Built flowfile-kernel-$(KERNEL_FLAVOUR):local (reports __version__ via /health)."

clean_test:
	@echo "Cleaning test artifacts..."
	$(RMRF) $(FRONTEND_DIR)/test-results/ $(FRONTEND_DIR)/playwright-report/
	@echo "Test artifacts cleaned."

# Coverage - run core and worker tests sequentially to avoid import collisions
test_coverage:
	@echo "Running tests with coverage..."
	$(POETRY_RUN) pytest flowfile_core/tests --cov --cov-report= --disable-warnings
	$(POETRY_RUN) pytest flowfile_worker/tests --cov --cov-append --cov-report= --disable-warnings
	@echo ""
	$(POETRY_RUN) coverage report --show-missing
	@echo ""
	@echo "To generate XML: $(POETRY_RUN) coverage xml"
	@echo "To generate HTML: $(POETRY_RUN) coverage html"

# Regenerate the flowfile_frame .pyi stubs (Expr, FlowFrame, and the thin
# submodules). Run this after changing any public API on FlowFrame, Expr, or a
# top-level helper exported via flowfile_frame/__init__.py. The Python source
# is the source of truth; stubs are introspected from it. The final ruff pass
# auto-removes any unused imports the generators (especially the older
# hardcoded ones) emit.
stubs:
	@echo "Regenerating flowfile_frame stubs..."
	$(POETRY_RUN) python flowfile_frame/expr_stub_generator.py
	$(POETRY_RUN) python flowfile_frame/flow_frame_stub_generator.py
	$(POETRY_RUN) python flowfile_frame/submodule_stub_generator.py
	@echo "Pruning unused imports from generated stubs..."
	@$(POETRY_RUN) ruff check $$(find flowfile_frame/flowfile_frame -name '*.pyi') --select F401 --fix --quiet || true
	@echo "Stubs regenerated."

# Drift check for CI: regenerate and fail if anything changed.
check_stubs: stubs
	@echo "Checking for stub drift..."
	@if ! git diff --exit-code -- 'flowfile_frame/flowfile_frame/*.pyi' 'flowfile_frame/flowfile_frame/**/*.pyi'; then \
		echo "ERROR: stubs are out of sync with the source. Run 'make stubs' and commit the result."; \
		exit 1; \
	fi
	@echo "Stubs are in sync."

# Regenerate the formula function reference (docs/users/formulas/functions.md)
# and the counts snippet index.md includes, from the polars-expr-transformer
# docstrings. Run after bumping the polars-expr-transformer pin.
formula_docs:
	@echo "Generating formula function reference..."
	$(POETRY_RUN) python tools/generate_formula_docs.py

# Drift check: regenerate and fail if the committed page or the counts snippet
# that index.md includes changed.
check_formula_docs: formula_docs
	@if ! git diff --exit-code -- docs/users/formulas/functions.md docs/users/formulas/function_summary.snippet; then \
		echo "ERROR: formula docs are out of sync. Run 'make formula_docs' and commit the result."; \
		exit 1; \
	fi
	@echo "Formula docs are in sync."

# Regenerate the kernel image manifest that kernel dependency matching reads.
# Run after changing kernel_runtime's pyproject/lockfile or its Dockerfile.
kernel_manifest:
	@echo "Generating kernel image manifest..."
	$(POETRY_RUN) python tools/generate_kernel_manifest.py

# Drift check: regenerate and fail if the committed manifest changed.
check_kernel_manifest: kernel_manifest
	@if ! git diff --exit-code -- flowfile_core/flowfile_core/kernel/kernel_image_manifest.json; then \
		echo "ERROR: the kernel image manifest is out of sync with kernel_runtime. Run 'make kernel_manifest' and commit the result."; \
		exit 1; \
	fi
	@echo "Kernel image manifest is in sync."

# Regenerate + verify the manifest actually ships in every packaging manifest.
check_kernel_data: check_kernel_manifest
	$(POETRY_RUN) pytest flowfile_core/tests/test_kernel_packaging_gate.py -q

# Regenerate the WASM node-support manifest the share-link builder reads.
# Run after changing flowfile_wasm's node palette, its core dialect map, or the
# aggregations its Pyodide engine implements.
wasm_node_manifest:
	@echo "Generating WASM node support manifest..."
	$(POETRY_RUN) python tools/generate_wasm_node_manifest.py

# Drift check: regenerate and fail if the committed manifest changed.
check_wasm_node_manifest: wasm_node_manifest
	@if ! git diff --exit-code -- flowfile_core/flowfile_core/flowfile/share/wasm_node_support.json; then \
		echo "ERROR: the WASM node support manifest is out of sync with flowfile_wasm. Run 'make wasm_node_manifest' and commit the result."; \
		exit 1; \
	fi
	@echo "WASM node support manifest is in sync."

# Regenerate + verify the manifest actually ships in every packaging manifest.
check_share_data: check_wasm_node_manifest
	$(POETRY_RUN) pytest flowfile_core/tests/test_share_packaging_gate.py -q

# Bump the app version everywhere (pyproject / package.json / tauri.conf.json / Cargo.toml).
# Usage: make bump-version VERSION=X.Y.Z
bump-version:
	@if [ -z "$(VERSION)" ]; then echo "Usage: make bump-version VERSION=X.Y.Z"; exit 1; fi
	$(POETRY_RUN) python tools/bump_version.py $(VERSION)
	@if command -v cargo >/dev/null 2>&1; then \
		cd flowfile_frontend/src-tauri && cargo update -p flowfile; \
	else echo "cargo not found: refresh flowfile_frontend/src-tauri/Cargo.lock before committing"; fi

# Drift check for CI: fail if the version is out of sync across manifests.
check-version:
	$(POETRY_RUN) python tools/check_version_sync.py

# Bump the kernel runtime version (kernel_runtime pyproject + __init__ __version__),
# independent of the app version. The kernel_runtime test_version_sync test guards
# the two stay in sync. Usage: make bump-version-kernel VERSION=X.Y.Z
bump-version-kernel:
	@if [ -z "$(VERSION)" ]; then echo "Usage: make bump-version-kernel VERSION=X.Y.Z"; exit 1; fi
	$(POETRY_RUN) python tools/bump_kernel_version.py $(VERSION)
	@$(MAKE) kernel_manifest

# Phony targets
.PHONY: all update_lock force_lock install_python_deps build_python_services rename_sidecars services sign_sidecars clean_dmg_mounts build_tauri_app build_tauri_win build_tauri_mac build_tauri_mac_arm build_tauri_mac_intel build_tauri_linux measure_bundle test_built_services clean generate_key force_key install_e2e test_e2e test_e2e_dev test_e2e_cloud stop_servers clean_kernels clean_kernel_images rebuild_kernel clean_test test_coverage stubs check_stubs formula_docs check_formula_docs kernel_manifest check_kernel_manifest check_kernel_data wasm_node_manifest check_wasm_node_manifest check_share_data bump-version check-version bump-version-kernel
