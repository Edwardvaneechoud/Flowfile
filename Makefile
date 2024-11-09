# Variables
PYTHON_DIR := .
ELECTRON_DIR := ./flowfile_frontend

# Default target: imake cleannstall dependencies and build both Python services and Electron app
all: install_python_deps build_python_services build_electron_app

# Install Python dependencies with Poetry
install_python_deps:
	@echo "Installing Python dependencies with Poetry..."
	poetry install
	@echo "Python dependencies installed."

# Build Python services
build_python_services: install_python_deps
	@echo "Building Python services..."
	poetry run build_backends
	@echo "Python services built successfully."

# Build Electron app
build_electron_app:
	@echo "Building Electron app..."
	cd $(ELECTRON_DIR) && npm install
	cd $(ELECTRON_DIR) && npm run build
	@echo "Electron app built successfully."

# Platform-specific Electron builds
build_electron_win:
	cd $(ELECTRON_DIR) && npm run build:win

build_electron_mac:
	cd $(ELECTRON_DIR) && npm run build:mac

build_electron_linux:
	cd $(ELECTRON_DIR) && npm run build:linux

# Clean up build artifacts
clean:
	@echo "Cleaning up build artifacts..."
	rm -rf dist/ build/ $(ELECTRON_DIR)/dist/ $(ELECTRON_DIR)/build/ $(ELECTRON_DIR)/node_modules/
	@echo "Clean up done."

# Phony targets
.PHONY: all install_python_deps build_python_services build_electron_app build_electron_win build_electron_mac build_electron_linux clean