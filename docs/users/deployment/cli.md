# Headless runs and the CLI

This page is for anyone driving Flowfile from a terminal — launching the app, running a saved flow without opening the UI, seeding the demo catalog, or scripting project version control. It documents every `flowfile` command and its flags, and walks through a real headless run of the bundled sales pipeline.

Installing Flowfile with `pip install flowfile` puts a `flowfile` command on your path. Run it with no arguments to print the version and a usage summary.

## Command overview

| Command | What it does |
|---|---|
| `flowfile run ui` | Start the web UI with the bundled services (the default way to use Flowfile). |
| `flowfile run flow <path>` | Run a saved flow headlessly and exit. |
| `flowfile run core` | Start only the core API service. |
| `flowfile run worker` | Start only the compute worker service. |
| `flowfile seed-demo` | Populate the optional demo catalog. |
| `flowfile remove-demo` | Remove the demo catalog. |
| `flowfile project init/open/save` | Git version control for your work (see [Projects](../projects.md)). |

## Start the app

```bash
flowfile run ui
```

This launches the core API with the worker co-hosted on the same port and opens the web UI in your browser at `http://127.0.0.1:63578/ui`.

Flags:

- `--no-browser` — start the services but don't open a browser tab.

!!! warning "The web UI is fixed to 127.0.0.1:63578"
    `run ui` always binds the bundled UI to `127.0.0.1` on port `63578` — the server refuses to start on any other host or port. The `--host` and `--port` flags exist for `run core` and `run worker` (below); they do not move the bundled UI. To expose Flowfile on another address, run it behind a reverse proxy or use the [Docker deployment](docker.md).

## Run a flow headlessly

```bash
flowfile run flow <path-to-flow-file>
```

Runs a saved flow to completion and exits with code `0` on success or `1` on failure. This is the command to use in cron jobs, CI, or any non-interactive context.

Accepted file formats are `.yaml`, `.yml`, and `.json`. (The legacy `.flowfile` pickle format is open-only in the app and is not accepted here.)

What a headless run does:

- **Forces local execution.** The flow runs entirely in-process — no worker service needs to be running.
- **Skips UI-only nodes.** `explore_data` nodes are dropped before the run (they require the interactive UI); the command reports how many it skipped.
- **Prints progress.** It shows the flow name, node count, the node tree, and a per-node error summary if anything fails.

Flags:

- `--param KEY=VALUE` — override a flow parameter. Repeat the flag for several overrides. An override that names a parameter the flow doesn't have is added as a new one.
- `--run-id N` — report the run's result back to a pre-created run record (used by the scheduler). You normally don't set this by hand.

```bash
# Override two parameters
flowfile run flow pipeline.yaml --param input_dir=/data --param threshold=100
```

## Worked example: run the sales pipeline

The sales pipeline that ships with Flowfile is a runnable artifact you can execute headlessly.

Download [`sales_pipeline.yaml`](../../assets/flows/sales_pipeline.yaml) and run it:

```bash
flowfile run flow sales_pipeline.yaml
```

The downloaded flow reads the sample CSV from its public GitHub URL, so it runs as-is with an internet connection — expect a five-city income summary and exit code `0`. To run fully offline, edit the read node's path to a local copy of [`supermarket_sales.csv`](https://raw.githubusercontent.com/edwardvaneechoud/flowfile/main/data/templates/supermarket_sales.csv), or create the flow from the in-app template browser (which provisions the data locally) and save it to a file.

## Run individual services

For advanced or split deployments you can start each service on its own. These accept `--host` and `--port`:

```bash
flowfile run core                       # core API, default 127.0.0.1:63578
flowfile run core --host 0.0.0.0 --port 63578
flowfile run worker                     # compute worker, default 127.0.0.1:63579
```

When you run the UI the two are combined into one process via single-file mode (`FLOWFILE_SINGLE_FILE_MODE`), which co-hosts the worker on the core port. When you run `core` and `worker` separately, core reaches the worker at `WORKER_HOST:FLOWFILE_WORKER_PORT`.

## The PyInstaller / packaged path

The desktop app and packaged builds run a saved flow through the core module directly rather than the `flowfile` wrapper:

```bash
python -m flowfile_core.main --run-flow <path> --run-id <N>
```

This forces local execution and drops `explore_data` nodes, exactly like `flowfile run flow`. Here `--run-id` is required — the packaged path always reports back to a run record. Prefer `flowfile run flow` for your own headless runs; this form exists for the scheduler and installed builds.

## Demo catalog

```bash
flowfile seed-demo      # create the demo catalog (idempotent)
flowfile remove-demo    # remove everything under the demo catalog
```

`seed-demo` populates a `Demo` catalog with sample tables and two registered flows so you can explore the catalog with real content. It's safe to run more than once — it skips what already exists. See [Start with a populated catalog](../visual-editor/catalog/index.md#start-with-a-populated-catalog) for what it creates.

## Projects

```bash
flowfile project init <folder>       # create a project in <folder>
flowfile project open <folder>       # open a project and rebuild it into the database
flowfile project save "<message>"    # save a version with a commit message
```

These are the headless equivalents of the app's project controls. Opening a project reports what it imported and how many secret values still need to be set. See [Projects](../projects.md) for the full workflow, and note that in Docker mode the project commands require `FLOWFILE_ENABLE_PROJECTS` to be set.

## Scheduling

To run flows on a timer rather than on demand, use the scheduler. In the desktop app and pip package it runs inside the app; for a standalone service the `flowfile_scheduler` console script polls for due runs and executes each one through the same headless path documented above. See [Schedules](../visual-editor/catalog/schedules.md) for setup, cron syntax, and the standalone-service mode.

## Related

- [Schedules](../visual-editor/catalog/schedules.md) — run flows automatically on a timer.
- [Projects](../projects.md) — git version control from the CLI.
- [Docker deployment](docker.md) — multi-user and networked deployments.
- [Python API](../python-api/index.md) — build and run flows programmatically instead of from files.
