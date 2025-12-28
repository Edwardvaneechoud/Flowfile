# E2E Testing Makefile
# Run: make test-e2e

.PHONY: install test-e2e test-e2e-dev start-backend start-frontend stop-all clean

# Install all dependencies
install:
	@echo "Installing backend dependencies..."
	cd .. && poetry install
	@echo "Installing frontend dependencies..."
	npm ci
	@echo "Installing Playwright browsers..."
	npx playwright install chromium

# Run E2E tests against production build
test-e2e: stop-all
	@echo "Building frontend..."
	npm run build:web
	@echo "Starting backend..."
	cd .. && poetry run flowfile_core &
	@sleep 5
	@echo "Starting frontend preview..."
	npm run preview:web &
	@sleep 3
	@echo "Running E2E tests..."
	TEST_URL=http://localhost:4173 npx playwright test tests/web-flow.spec.ts || true
	@$(MAKE) stop-all

# Run E2E tests against dev server (faster, no build)
test-e2e-dev: stop-all
	@echo "Starting backend..."
	cd .. && poetry run flowfile_core &
	@sleep 5
	@echo "Starting frontend dev server..."
	npm run dev:web &
	@sleep 3
	@echo "Running E2E tests..."
	npx playwright test tests/web-flow.spec.ts || true
	@$(MAKE) stop-all

# Start backend only (for manual testing)
start-backend:
	cd .. && poetry run flowfile_core

# Start frontend only (for manual testing)
start-frontend:
	npm run dev:web

# Start frontend preview (production build)
start-frontend-preview:
	npm run build:web
	npm run preview:web

# Stop all background processes
stop-all:
	@echo "Stopping any running servers..."
	-@pkill -f "flowfile_core" 2>/dev/null || true
	-@pkill -f "vite" 2>/dev/null || true
	-@pkill -f "preview" 2>/dev/null || true

# Clean test artifacts
clean:
	rm -rf test-results/
	rm -rf playwright-report/

# Show help
help:
	@echo "Available commands:"
	@echo "  make install          - Install all dependencies"
	@echo "  make test-e2e         - Run E2E tests (production build)"
	@echo "  make test-e2e-dev     - Run E2E tests (dev server, faster)"
	@echo "  make start-backend    - Start backend server only"
	@echo "  make start-frontend   - Start frontend dev server only"
	@echo "  make stop-all         - Stop all background servers"
	@echo "  make clean            - Clean test artifacts"
