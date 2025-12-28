# Variables
PYTHON_DIR := .
ELECTRON_DIR := flowfile_frontend
KEY_FILE := master_key.txt

# Detect OS
ifeq ($(OS),Windows_NT)
	RMRF := del /f /s /q
	RMDIR := rmdir /s /q
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

# Default target: install dependencies and build both Python services and Electron app
all: install_python_deps build_python_services build_electron_app generate_key

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
	poetry install
	@echo "Python dependencies installed."

# Build Python services
build_python_services: install_python_deps
	@echo "Building Python services..."
	$(POETRY_RUN) build_backends
	@echo "Python services built successfully."

# Build Electron app
build_electron_app:
	@echo "Building Electron app..."
	$(CD) "$(ELECTRON_DIR)" && npm install
	$(CD) "$(ELECTRON_DIR)" && npm run build
	@echo "Electron app built successfully."

# Platform-specific Electron builds
build_electron_win:
	$(CD) "$(ELECTRON_DIR)" && npm run build:win

build_electron_mac:
	$(CD) "$(ELECTRON_DIR)" && npm run build:mac

build_electron_linux:
	$(CD) "$(ELECTRON_DIR)" && npm run build:linux

# Clean up build artifacts
clean:
	@echo "Cleaning up build artifacts..."
ifeq ($(OS),Windows_NT)
	@if exist "services_dist" $(RMDIR) "services_dist"
	@if exist "build" $(RMDIR) "build"
	@if exist "$(ELECTRON_DIR)\dist" $(RMDIR) "$(ELECTRON_DIR)\dist"
	@if exist "$(ELECTRON_DIR)\build" $(RMDIR) "$(ELECTRON_DIR)\build"
	@if exist "$(ELECTRON_DIR)\node_modules" $(RMDIR) "$(ELECTRON_DIR)\node_modules"
else
	$(RMRF) services_dist/ build/ $(ELECTRON_DIR)/dist/ $(ELECTRON_DIR)/build/ $(ELECTRON_DIR)/node_modules/
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

# Force regenerate Docker master key
force_key:
	@echo "Regenerating master key..."
	$(POETRY_RUN) python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())" > $(KEY_FILE)
ifeq ($(OS),Windows_NT)
	@echo Master key regenerated successfully.
else
	@chmod 600 $(KEY_FILE)
	@echo "Master key regenerated successfully."
endif

# E2E Testing
install_e2e:
	@echo "Installing E2E test dependencies..."
	$(CD) "$(ELECTRON_DIR)" && npm ci
	$(CD) "$(ELECTRON_DIR)" && npx playwright install chromium
	@echo "E2E dependencies installed."

test_e2e: install_python_deps
	@echo "Running E2E tests..."
	@echo "Building frontend..."
	$(CD) "$(ELECTRON_DIR)" && npm run build:web
	@echo "Starting backend..."
ifeq ($(OS),Windows_NT)
	start /B $(POETRY_RUN) flowfile_core
	timeout /t 5 /nobreak >NUL
	@echo "Starting frontend preview..."
	$(CD) "$(ELECTRON_DIR)" && start /B npm run preview:web
	timeout /t 3 /nobreak >NUL
else
	$(POETRY_RUN) flowfile_core &
	sleep 5
	@echo "Starting frontend preview..."
	$(CD) "$(ELECTRON_DIR)" && npm run preview:web &
	sleep 3
endif
	@echo "Running tests..."
	$(CD) "$(ELECTRON_DIR)" && TEST_URL=http://localhost:4173 npx playwright test tests/web-flow.spec.ts || true
	@$(MAKE) stop_servers

test_e2e_dev: install_python_deps
	@echo "Running E2E tests (dev mode)..."
	@echo "Starting backend..."
ifeq ($(OS),Windows_NT)
	start /B $(POETRY_RUN) flowfile_core
	timeout /t 5 /nobreak >NUL
	@echo "Starting frontend dev server..."
	$(CD) "$(ELECTRON_DIR)" && start /B npm run dev:web
	timeout /t 3 /nobreak >NUL
else
	$(POETRY_RUN) flowfile_core &
	sleep 5
	@echo "Starting frontend dev server..."
	$(CD) "$(ELECTRON_DIR)" && npm run dev:web &
	sleep 3
endif
	@echo "Running tests..."
	$(CD) "$(ELECTRON_DIR)" && npx playwright test tests/web-flow.spec.ts || true
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
	$(RMRF) $(ELECTRON_DIR)/test-results/ $(ELECTRON_DIR)/playwright-report/
	@echo "Test artifacts cleaned."

# Electron E2E Testing (requires built app)
build_for_electron_test: install_python_deps build_python_services build_electron_app
	@echo "Electron app built successfully for E2E testing."

test_e2e_electron: build_for_electron_test
	@echo "Running Electron E2E tests..."
	$(CD) "$(ELECTRON_DIR)" && npx playwright test tests/app.spec.ts tests/complex-flow.spec.ts --reporter=html
	@echo "Electron E2E tests completed."

# Phony targets
.PHONY: all update_lock force_lock install_python_deps build_python_services build_electron_app build_electron_win build_electron_mac build_electron_linux clean generate_key force_key install_e2e test_e2e test_e2e_dev stop_servers clean_test build_for_electron_test test_e2e_electron
