# Contributing to Flowfile

Thanks for your interest in contributing! This guide covers how to set up a dev environment, the conventions we follow, and how to get a change merged.

Flowfile is MIT-licensed. By contributing, you agree that your contribution will be released under the same license.

## Before you start

- **Small fixes** (typos, clear bugs, small doc improvements) — just open a PR.
- **Anything bigger** — open a [GitHub Discussion](https://github.com/edwardvaneechoud/Flowfile/discussions) or an issue first so we can agree on the shape before you write code. This saves everyone time.
- **Questions, not contributions** — head to [Discussions](https://github.com/edwardvaneechoud/Flowfile/discussions). See the [Community page](https://edwardvaneechoud.github.io/Flowfile/community.html).

## Repository layout

This is a monorepo managed by Poetry (Python) and npm (frontend):

- `flowfile_core/` — FastAPI backend (ETL engine, flow execution, auth, catalog) — port `63578`
- `flowfile_worker/` — FastAPI compute worker for heavy data processing — port `63579`
- `flowfile_frame/` — Python API library (Polars-like interface for programmatic flows)
- `flowfile_frontend/` — Electron + Vue 3 UI (VueFlow graph editor)
- `flowfile_wasm/` — Browser-only WASM version (Pyodide)
- `flowfile/` — CLI entry point
- `kernel_runtime/` — Docker-based sandbox for user Python code
- `docs/` — MkDocs site

See [`CLAUDE.md`](./CLAUDE.md) for a more detailed tour.

## Dev setup

### Prerequisites

- Python `>=3.10, <3.14`
- Node.js `20+`
- [Poetry](https://python-poetry.org/)
- Docker (only required for the `kernel` test marker)

### Backend

```bash
poetry install

# Run the services in two terminals
poetry run flowfile_worker   # :63579
poetry run flowfile_core     # :63578
```

### Frontend

```bash
cd flowfile_frontend
npm install
npm run dev:web              # web dev server, :5173 → backend on :63578
# or: npm run dev            # full Electron dev mode
```

### Full stack via Docker

```bash
cp .env.example .env
docker compose up -d
# Frontend: http://localhost:8080  Core: :63578  Worker: :63579
```

## Code style

### Python — Ruff

- Line length **120**, target **Python 3.10**
- Rules: Pyflakes (F), pycodestyle (E/W), isort (I), pyupgrade (UP), flake8-bugbear (B)
- Double quotes, space indentation
- Use **Polars**, not pandas
- Import order: stdlib, third-party, then first-party (`flowfile`, `flowfile_core`, `flowfile_worker`, `flowfile_frame`, `shared`, `test_utils`, `tools`, `build_backends`)

```bash
poetry run ruff check .
poetry run ruff check --fix .
poetry run ruff format .
```

### Frontend — ESLint + Prettier

- Semicolons, 2-space tabs, double quotes, 100-char width, trailing commas, LF line endings
- Vue 3 Composition API + TypeScript, Pinia for state, Element Plus for UI
- Path aliases: `@` → `src/renderer/app/`

```bash
cd flowfile_frontend
npm run lint
```

## Tests

Any code change that can be tested should come with a test.

### Python (pytest)

```bash
poetry run pytest flowfile_core/tests
poetry run pytest flowfile_worker/tests
poetry run pytest flowfile_frame/tests

# With coverage (core + worker)
make test_coverage

# Kernel integration (Docker required)
poetry run pytest -m kernel
```

Markers: `worker`, `core`, `kernel`.

### Frontend E2E (Playwright)

```bash
cd flowfile_frontend
npx playwright install --with-deps chromium
npm run test:web         # needs backend + preview server running
npm run test:electron    # needs built app
```

Or via Make:

```bash
make test_e2e            # web E2E, builds frontend and starts servers
make test_e2e_electron   # full Electron E2E
```

### WASM (Vitest)

```bash
cd flowfile_wasm
npm run test
```

## Commits and PRs

- **Branch naming:** `fix/...`, `feat/...`, `docs/...` is fine — nothing strict.
- **Commit messages:** short imperative subject (e.g. `fix flow parameter serialization on reload`). Explain the *why* in the body if it isn't obvious from the diff.
- **One logical change per PR.** Smaller PRs get reviewed faster.
- **Fill in the PR description:** what changed, why, and how you tested it. Screenshots or short clips help for UI changes.
- **CI must be green** before merge. If a check is flaky, say so in the PR — don't just re-run silently.
- **Don't force-push to `main`.** Releases build from it.

## Things to avoid

- Don't commit `master_key.txt`, `.env`, or any credentials.
- Don't introduce pandas — the project uses Polars throughout.
- On Windows, Polars is pinned to `<=1.25.2` (build constraint) — don't bump it unilaterally.
- Don't add features or abstractions "for later." Keep changes scoped to the task.

## Reporting security issues

Please **do not** open a public issue for security vulnerabilities. Instead, report them privately via GitHub's ["Report a vulnerability"](https://github.com/edwardvaneechoud/Flowfile/security/advisories/new) form.

## Code of conduct

Be kind and assume good intent. Flowfile follows the [Contributor Covenant v2.1](https://www.contributor-covenant.org/version/2/1/code_of_conduct/).
