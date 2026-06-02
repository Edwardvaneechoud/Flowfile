# Formula syntax

Flowfile supports a simplified **formula syntax** for expressions: Excel-like
strings that use bracket notation (`[column]`) to reference columns. It is an
alternative to the Polars-style [`ff.col(...)` API](../reference/flowframe-operations.md),
designed to lower the learning curve for users coming from Excel, Power BI,
Alteryx, and Tableau — and it renders natively in the visual editor's formula bar.

```python
import flowfile as ff

# These two are equivalent:
ff.col("price") * ff.col("quantity")   # Polars-style expression
"[price] * [quantity]"                  # Flowfile formula syntax
```

This page is the canonical reference for the formula language. For the complete
list of built-in functions, see the
[formula function reference](../reference/formula-functions.md).

## Try it live

Edit a formula and press **Run** — it executes right here in your browser via
Pyodide (WebAssembly Python), no install or backend required. The first run
downloads the Python runtime, so give it a few seconds.

<div class="formula-playground" data-formula="[price] * [quantity]"></div>

Prefer a standalone version? The hosted
[Flowfile Formula Playground](https://polars-expr-transformer-playground-whuwbghlymon84t5ciewp3.streamlit.app/)
works too.

## Where you can use it

Two FlowFrame operations accept formula strings instead of (or alongside)
Polars expressions:

=== "with_columns"

    Pass `flowfile_formulas` together with matching `output_column_names`
    (one name per formula):

    ```python
    df = df.with_columns(
        flowfile_formulas=[
            "[price] * [quantity]",
            "if [quantity] > 75 then 'High' else 'Low' endif",
        ],
        output_column_names=["revenue", "volume_category"],
    )
    ```

=== "filter"

    Pass a single `flowfile_formula` that evaluates to a boolean:

    ```python
    df = df.filter(flowfile_formula="[price] > 15 and [quantity] > 0")
    ```

You can freely mix formula columns with Polars-style expressions across chained
calls:

```python
df = (
    df.with_columns(ff.col("price").round(0).alias("price_rounded"))   # Polars style
      .with_columns(
          flowfile_formulas=["[price_rounded] * [quantity]"],          # formula style
          output_column_names=["estimated_revenue"],
      )
)
```

## Column references

Wrap a column name in square brackets to reference it. `[price]` is exactly
equivalent to `ff.col("price")`. Names with spaces work too: `[unit price]`.

## Operators

| Type | Operators | Example |
| --- | --- | --- |
| Arithmetic | `+`  `-`  `*`  `/` | `[price] * [quantity]` |
| Comparison | `<`  `<=`  `>`  `>=`  `=` (or `==`)  `!=` | `[price] >= 15` |
| Boolean | `and`  `or`  `not(...)` (case-insensitive: `AND`/`OR` also work) | `[price] > 15 and [qty] > 0` |

!!! warning "Not supported"
    Use `!=` for "not equal" — the SQL-style `<>` operator is **not** supported.
    Combine multiple conditions with `and` / `or`, and negate with `not(...)`.

Use parentheses to control precedence: `[price] * (1 - [discount])`.

## Conditional logic

Use `if … then … else … endif`. Conditions can use any comparison/boolean
operators, and branches can be values, columns, or nested formulas:

```text
if [quantity] > 75 then 'High' else 'Low' endif
if [score] >= 90 then 'A' else if [score] >= 80 then 'B' else 'C' endif endif
```

String literals may use single or double quotes (`'High'` or `"High"`).

## Functions

The formula language ships **95 built-in functions** across five categories —
logic, string, math, date/time, and type conversion:

```text
uppercase([name])
round([price] * [discount], 2)
coalesce([nickname], [name], 'Unknown')
add_days([order_date], 30)
to_integer([code])
```

See the [formula function reference](../reference/formula-functions.md) for the
full catalog with descriptions and examples.

## Formula syntax vs. the Polars API

Both compile to the same Polars expressions, so there is no performance
difference. Choose based on context:

| Use formula syntax when… | Use the Polars `ff.col(...)` API when… |
| --- | --- |
| You want the expression to render in the visual editor's formula bar | You are writing pure code and prefer IDE autocomplete / type checking |
| Your audience knows Excel / Alteryx / Power BI more than Polars | You need Polars features with no formula equivalent (windows, structs, lists) |
| The logic reads more clearly as a string (`if … then … endif`) | You are composing expressions programmatically |

!!! tip
    Formula columns appear as a **Formula** node in the visual editor, while
    Polars expressions appear as their corresponding node type. Both round-trip
    losslessly between code and the editor.

---

*Looking for the full API? See the [reference documentation](../reference/index.md).*
