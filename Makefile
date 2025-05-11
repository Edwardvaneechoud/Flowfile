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

# Phony targets
.PHONY: all update_lock force_lock install_python_deps build_python_services build_electron_app build_electron_win build_electron_mac build_electron_linux clean generate_key force_key
