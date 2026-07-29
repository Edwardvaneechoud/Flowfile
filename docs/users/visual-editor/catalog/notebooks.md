# Notebooks

Notebooks are the catalog's exploration console: Jupyter-style cells that live *next to your tables*, in the catalog rather than in one person's private folder. Open one from the catalog tree, point it at a kernel, and analyze catalog data with any Python library — with the environment pinned by the kernel, so the notebook runs the same for everyone who opens it.

![A catalog notebook open in the notebook panel: the catalog tree on the left with the notebook selected, cells with execution counters, the kernel picker in the toolbar, and an interactive table rendered under a cell.](../../../assets/images/guides/notebooks/notebook-panel.png)

## Creating and organizing

Notebooks are catalog residents: create one from the notebook panel (**New notebook**) and it takes a place in a [namespace](index.md#namespaces) like any table or flow — same tree, same organization, and in Docker mode the same [group-based sharing](../../deployment/sharing.md) (a grant on the namespace reaches the notebooks inside it).

## Cells

Two cell types, mixable freely:

- **Python** — executes on a [kernel](../kernels.md), which you pick in the toolbar. Any library the kernel image carries (or that you added to the kernel) is available. The notebook remembers your last-used kernel.
- **Markdown** — renders in place, no kernel needed. Use it for the narrative between the code.

Keybindings match Jupyter: **Shift+Enter** runs a cell (or renders a Markdown cell), **Cmd/Ctrl+Enter** runs it and moves to the next. In cell mode the last expression displays automatically, Jupyter-style, and the editor gives you code completions as you type.

## Talking to the catalog

Cells see the catalog through the same `flowfile_ctx` API that Python Script nodes use:

```python
lf = flowfile_ctx.read_catalog_table("sales_by_city")   # by name; schema= / namespace_id= to disambiguate
flowfile_ctx.display(lf)                                 # interactive table
flowfile_ctx.explore(lf)                                 # Graphic Walker explorer
```

Reads come back as LazyFrames; `flowfile_ctx.write_catalog_table` persists a result as a real catalog table, and `flowfile_ctx.publish_global` saves a Python object (a trained model, a config) as a [global artifact](index.md#global-artifacts) that survives across sessions and flows. The full cell-side API — inputs, outputs, display, artifacts — is documented in [The flowfile_ctx API](../kernel-api.md).

<details markdown="1">
<summary>See it: a catalog notebook cell in action</summary>

![A catalog notebook cell running flowfile_ctx.read_catalog_table, then display and explore, with the interactive table and Graphic Walker explorer rendering live beneath the cell](../../../assets/images/guides/notebooks/notebook-in-action.gif)

</details>

## Versioned like flows

A notebook's cells are stored as one clean YAML file on disk — code as readable text, not an opaque blob — which is what lets [Projects](../../projects.md) version notebooks in git exactly like flows: diffs read like code review, and a teammate pulling the project gets your notebooks along with everything else.

## Related

- [Kernels](../kernels.md) — creating kernels, adding packages, the full `flowfile_ctx` API.
- [SQL Editor](sql-editor.md) — for pure-SQL exploration without a kernel.
- [Catalog Architecture](../../../for-developers/catalog-architecture.md#notebooks) — how notebooks are stored and routed, for contributors.
