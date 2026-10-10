# Flowfile Roadmap

**Flowfile 0.22 · updated 2026-10-08 · [discuss this roadmap](https://github.com/edwardvaneechoud/Flowfile/discussions/categories/announcements)**

Flowfile lets you build the same data pipeline on a canvas or in Python.
The priority is to make that everyday workflow dependable: edit it, run it,
save it and export it without losing work or getting different results.

The canvas, Python API, Polars export, Delta storage and separate worker
already exist. There are gaps to close before adding more infrastructure.
This is the current order of work. Scope and priorities can change as we
learn more; there are no delivery dates attached.

## Priorities

The status column describes what exists today. Detailed implementation
notes are in the [roadmap background](docs/roadmap-details.md).

| Order | Work | Current status | Next milestone |
|---|---|---|---|
| 1 | [Canvas, Python and export](docs/roadmap-details.md#canvas-python-and-export) | Working, with coverage gaps | Close the remaining export and expression gaps; verify matching results and preserved node settings. |
| 2 | [Everyday reliability](docs/roadmap-details.md#everyday-reliability) | Release criteria to define and verify | Check install, save, reopen, upgrade and error recovery against a documented set of everyday flows. |
| 3 | [Workers on separate machines](docs/roadmap-details.md#remote-workers) | Separate service; shared filesystem still required | Run core and worker on separate hosts without a shared disk. |
| 4 | [Open formats and catalogs](docs/roadmap-details.md#open-formats-and-catalogs) | Iceberg reader still a stub | Read an Iceberg table written by another engine, through the canvas, Python API and export. |
| 5 | [Shared catalogs and team deployment](docs/roadmap-details.md#shared-catalogs-and-team-deployment) | Combined setup still to verify | Two instances share tables safely; provide a reproducible PostgreSQL and MinIO setup. |
| 6 | [AI planner reliability](docs/roadmap-details.md#ai-planner-reliability) | Reviewable changes exist; benchmark planned | Measure task success and retries against agreed thresholds. |

The first two priorities set the scope for 1.0. Remote workers come next so
you can run a flow where your data lives. Catalog and deployment work builds
on that foundation. AI evaluation can progress independently when someone
takes it on.

## What we want from 1.0

For 1.0, the focus is a reliable local workflow. The release checks should
cover:

- **Consistent results.** Canvas and Python edits preserve node settings.
  Exported flows produce the same results with Flowfile uninstalled and
  their documented dependencies installed. Export coverage and any
  limitations are clear before you export.
- **Work you can keep.** Saving and reopening preserves the flow. Upgrades
  are checked against saved flows from supported versions, with a documented
  compatibility policy and migration instructions where needed.
- **Useful failures.** A failed run points to the affected node and gives
  enough information to fix the problem. Correcting it and rerunning doesn't
  require rebuilding the flow.
- **A straightforward start.** A new user can install Flowfile, build a flow,
  inspect its results and export it by following the local quickstart.
  The supported setup and workflow are covered by automated checks.

The exact supported versions and test cases need to be agreed and recorded
before release. External catalogs, organization login and cluster backends
are separate milestones, so they don't hold up the core 1.0 release.

## After the core work

The longer-term aim is to let you choose your compute, storage and catalog,
and keep running your pipelines if you stop using Flowfile.

Possible follow-ups include a Polars Cloud backend, more Iceberg REST
catalogs, OIDC login, components shared through the catalog and kernels on a
remote Docker host. The full reference deployment would add Lakekeeper and
a walkthrough that checks the services together.

The [architecture and technology notes](docs/roadmap-details.md) explain the
options, dependencies and how we'd check each one. They also cover what
Flowfile leaves to other tools, including cluster management, storage
services and full BI.

## License

Flowfile is [MIT-licensed](https://github.com/edwardvaneechoud/Flowfile/blob/main/LICENSE).

## Get involved

Work on a milestone should have an issue or
[Discussion](https://github.com/edwardvaneechoud/Flowfile/discussions/categories/announcements)
with its scope, open questions and completion checks. Link it here when work
starts so progress is easy to follow.

For anything larger than a bug fix, discuss it before writing code. To
suggest a roadmap change, open a Discussion titled `RFC: <topic>` and explain
the problem, who it helps, the smallest useful first step and how we'd check
that it works.

For a smaller contribution, have a look at
[`good first issue`](https://github.com/edwardvaneechoud/Flowfile/issues?q=is%3Aopen+label%3A%22good+first+issue%22)
issues and [CONTRIBUTING.md](https://github.com/edwardvaneechoud/Flowfile/blob/main/CONTRIBUTING.md).
Suggestions for this file are welcome as pull requests too.
