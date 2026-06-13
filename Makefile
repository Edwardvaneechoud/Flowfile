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
	timeout /t 5 /nobreak >NUL
	@echo "Starting frontend preview..."
	$(CD) "$(FRONTEND_DIR)" && start /B npm run preview:web
	timeout /t 3 /nobreak >NUL
else
	$(POETRY_RUN) flowfile_core &
	sleep 5
	@echo "Starting frontend preview..."
	$(CD) "$(FRONTEND_DIR)" && npm run preview:web &
	sleep 3
endif
	@echo "Running tests..."
	$(CD) "$(FRONTEND_DIR)" && TEST_URL=http://localhost:4173 npx playwright test tests/web-flow.spec.ts || true
	@$(MAKE) stop_servers

test_e2e_dev: install_python_deps
	@echo "Running E2E tests (dev mode)..."
	@echo "Starting backend..."
ifeq ($(OS),Windows_NT)
	start /B $(POETRY_RUN) flowfile_core
	timeout /t 5 /nobreak >NUL
	@echo "Starting frontend dev server..."
	$(CD) "$(FRONTEND_DIR)" && start /B npm run dev:web
	timeout /t 3 /nobreak >NUL
else
	$(POETRY_RUN) flowfile_core &
	sleep 5
	@echo "Starting frontend dev server..."
	$(CD) "$(FRONTEND_DIR)" && npm run dev:web &
	sleep 3
endif
	@echo "Running tests..."
	$(CD) "$(FRONTEND_DIR)" && npx playwright test tests/web-flow.spec.ts || true
	@$(MAKE) stop_servers

stop_servers:
	@echo "Stopping servers..."
ifeq ($(OS),Windows_NT)
	-@taskkill /F /IM python.exe $(NULL_OUTPUT)
	-@taskkill /F /IM node.exe $(NULL_OUTPUT)
else
	-@pkill -f "flowfile_core" 2>/dev/null || true
	-@pkill -f "vite" 2>/dev/null || true
endif
	@echo "Servers stopped."

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
# from the polars-expr-transformer docstrings. Run after bumping the
# polars-expr-transformer pin.
formula_docs:
	@echo "Generating formula function reference..."
	$(POETRY_RUN) python tools/generate_formula_docs.py

# Drift check: regenerate and fail if the committed page changed.
check_formula_docs: formula_docs
	@if ! git diff --exit-code -- docs/users/formulas/functions.md; then \
		echo "ERROR: formula docs are out of sync. Run 'make formula_docs' and commit the result."; \
		exit 1; \
	fi
	@echo "Formula docs are in sync."

# Phony targets
.PHONY: all update_lock force_lock install_python_deps build_python_services rename_sidecars services sign_sidecars clean_dmg_mounts build_tauri_app build_tauri_win build_tauri_mac build_tauri_mac_arm build_tauri_mac_intel build_tauri_linux measure_bundle test_built_services clean generate_key force_key install_e2e test_e2e test_e2e_dev stop_servers clean_test test_coverage stubs check_stubs formula_docs check_formula_docs
