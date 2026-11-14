# Project kinds

A project folder holds an install's objects as files: flows, connections, schedules, catalog tables and the rest. Each sort of object is a **kind**, and `flowfile_core/project/kinds.py` keeps them in one registry. Export, status, the change preview, the files git stages, an apply and its deletes all read that registry, so a kind registered there is handled everywhere without further code. Core registers its twelve kinds in `core_kinds.py`. A Python package can add its own through an entry point.

## What a kind is

A kind is a `kinds.Kind`, a frozen dataclass:

| Field | What it holds |
|---|---|
| `name` | The kind's identifier, written in `project.yaml`'s `kinds:` list. |
| `label` | How a change summary names one of its objects. |
| `paths` | One-level patterns of the files it owns, such as `widgets/*.yaml` or `widgets.yaml`. |
| `export(ctx)` | The kind's files as `{path: text}`, read from the database through `ctx.db` for `ctx.owner_id`. The same database must always give the same text. |
| `entries(files)` | Splits the kind's files into objects, `{key: Entry(label, path, text)}`; two entries are the same object when their keys are equal and unchanged when their texts are. |
| `delete(ctx, keys)` | Deletes the objects of those keys from the database. |
| `delete_label` | What a preview says a delete does. With none, removals are not reported. |
| `apply(ctx, entries)` | Writes the objects `entries` returned for the kind's files into the database, adding and changing rows. Deletes go through `delete`, so `apply` never removes an object. |
| `after`, `before` | Names of kinds this one is applied after, and before. |
| `cap` | The most files or entries an import accepts, or none. |

Project files stay secret-free: an export writes no secret value and no ciphertext. A kind that needs a secret writes a reference to it by name.

## Registering a kind

A package names its kind in the `flowfile.project_kinds` entry-point group. The entry point loads to the `Kind` itself:

```toml
[project.entry-points."flowfile.project_kinds"]
widget = "my_package.project:WIDGET"
```

The group is scanned once, the first time the registry is read, after core's kinds are registered.

## Order

An import applies the kinds in `kinds.ordered()` order, and deletes them in reverse order. A kind is placed after every kind its `after` names, and before every kind its `before` names. `before=("flow",)` means the same as `flow` itself listing the kind in its `after`. A name that is not registered is ignored.

## Rejection

A package's kind is left out when:

- its name is already registered;
- one of its patterns matches, or is matched by, a pattern of another kind;
- adding it would put the order in a cycle;
- the entry point does not load, or loads to something that is not a `Kind`.

The reason is logged as a warning and `kinds.rejected()` returns it by kind name. Every other kind keeps working. A project that lists a rejected kind reports it as an unknown kind on import and keeps its files.

## The transaction

A restore or reload applies the whole folder in one database transaction. For a package's kind the importer reads the files its patterns match, calls `apply(ctx, kind.entries(files))` in a session that joins that transaction, then commits the session. `apply` need not commit itself. When `apply` raises, the import fails with `Could not apply <label>: <error>` and nothing it or any other kind wrote is kept. A failing `delete` rolls the apply back in the same way.

## Testing a kind

`flowfile_core.project.testing.assert_kind_round_trips(kind, owner_id)` checks a kind against a test database holding some of its objects:

- two exports are equal, and every path written is one of the kind's;
- `entries` of the export returns equal entries each time it is called;
- deleting every key leaves no object in the export;
- applying the entries again gives back the first export, byte for byte.

It deletes and re-creates the owner's objects of the kind, so run it on test data only.
