# Contributing to Flowfile

Thank you for your interest in contributing to Flowfile! This guide will help you set up your development environment and understand our code quality standards.

## Development Setup

### Prerequisites

- Python 3.10 or higher (but less than 3.14)
- [Poetry](https://python-poetry.org/docs/#installation) for dependency management
- Git

### Initial Setup

1. **Clone the repository**
   ```bash
   git clone https://github.com/Edwardvaneechoud/Flowfile.git
   cd Flowfile
   ```

2. **Install dependencies**
   ```bash
   poetry install
   ```

3. **Install pre-commit hooks** (recommended)
   ```bash
   poetry run pre-commit install
   ```

   This will automatically run linting and formatting checks before each commit.

## Code Quality

### Linting with Ruff

We use [Ruff](https://docs.astral.sh/ruff/) for linting and code formatting. Ruff is configured in `pyproject.toml`.

**Run linting manually:**
```bash
# Check for linting issues
poetry run ruff check .

# Auto-fix linting issues
poetry run ruff check --fix .

# Check code formatting
poetry run ruff format --check .

# Format code
poetry run ruff format .
```

**Configuration:**
- Target: Python 3.10+
- Line length: 120 characters
- Rules: F (Pyflakes), E/W (pycodestyle), I (isort), UP (pyupgrade), B (flake8-bugbear)

### Pre-commit Hooks

Pre-commit hooks automatically run before each commit to ensure code quality. They will:

1. **Ruff linting** - Check and auto-fix Python code issues
2. **Ruff formatting** - Format Python code consistently
3. **File checks** - Validate YAML, JSON, TOML, and Python syntax
4. **Trailing whitespace** - Remove unnecessary whitespace
5. **End of file** - Ensure files end with a newline
6. **Merge conflicts** - Detect merge conflict markers
7. **Large files** - Prevent committing large files (>1MB)

**Skip pre-commit hooks** (not recommended):
```bash
git commit --no-verify -m "Your commit message"
```

**Run pre-commit manually on all files:**
```bash
poetry run pre-commit run --all-files
```

### Continuous Integration

Our GitHub Actions workflows automatically run:

- **Linting** (`lint.yml`) - Runs ruff check and format validation on all PRs
- **Tests** (`test-docker-auth.yml`, `e2e-tests.yml`) - Runs test suites
- **Documentation** (`documentation.yml`) - Builds and deploys docs

All checks must pass before a PR can be merged.

## Running Tests

```bash
# Run all tests
poetry run pytest

# Run tests for a specific module
poetry run pytest flowfile_core/tests/
poetry run pytest flowfile_worker/tests/

# Run tests with coverage
poetry run pytest --cov=flowfile_core --cov=flowfile_worker
```

## Code Style Guidelines

- Follow [PEP 8](https://pep8.org/) style guidelines (enforced by Ruff)
- Use type hints where appropriate
- Write descriptive variable and function names
- Keep functions focused and modular
- Add docstrings for public functions and classes
- Keep line length under 120 characters

## Submitting Changes

1. **Create a new branch** for your feature or fix:
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **Make your changes** and ensure all tests pass

3. **Commit your changes** (pre-commit hooks will run automatically):
   ```bash
   git add .
   git commit -m "Add your descriptive commit message"
   ```

4. **Push to your fork**:
   ```bash
   git push origin feature/your-feature-name
   ```

5. **Create a Pull Request** on GitHub

## Getting Help

- Check the [documentation](https://edwardvaneechoud.github.io/Flowfile/)
- Open an issue on GitHub
- Read the [architecture documentation](docs/for-developers/architecture.md)

Thank you for contributing to Flowfile! 🚀
