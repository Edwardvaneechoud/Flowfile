# Formula Language

Flowfile formulas are a simple, Excel-like expression language for transforming data. Write `[column]` to reference a column, call functions like `round()` or `concat()`, and use `if ... then ... else ... endif` for conditional logic — the same way you would in a spreadsheet formula or a SQL `CASE` statement.

Every formula compiles to a native [Polars](https://pola.rs) expression before it runs, so there is no row-by-row Python overhead: formulas are as fast as hand-written Polars code. The same language is used everywhere in Flowfile — the visual editor and the Python API share it.

!!! tip "Try it in your browser"
    The [interactive formula playground](https://edwardvaneechoud.github.io/polars_expr_transformer/). Pick a sample dataset, type a formula, and watch the result, the generated Polars code, and the FlowFrame code update as you type.

---

## Where formulas are used

| Surface | How |
|---------|-----|
| [Formula node](../visual-editor/nodes/transform.md#formula) | Create or replace a column with a formula. |
| [Filter node](../visual-editor/nodes/transform.md#filter-data) (advanced mode) | Keep rows where a formula evaluates to `true`; split mode routes passing and failing rows to separate outputs. |
| [Python API](../python-api/concepts/formulas.md) | Pass formula strings to `with_columns(flowfile_formulas=...)`, `filter(flowfile_formula=...)`, and `filter_split(flowfile_formula=...)`. |

---

## Syntax

### Column references

Wrap column names in square brackets. Spaces in names are fine.

```text
[salary]
[Order Date]
[first_name]
```

### Literals

Strings use single or double quotes; numbers and booleans are written bare.

```text
"hello"   'world'
42   3.14   -7
true   false
```

### Operators

| Operators | Meaning |
|-----------|---------|
| `+` `-` `*` `/` `%` | Arithmetic (`%` is modulo) |
| `=` `==` `!=` | Equality (`=` and `==` are equivalent) |
| `>` `>=` `<` `<=` | Comparison |
| `and` `or` | Boolean logic |
| `( )` | Grouping |

Use parentheses to control evaluation order:

```text
[price] * (1 - [discount])
```

!!! warning "Use `and` / `or`, not `&&` / `||`"
    Boolean logic uses the keywords `and` and `or` (case-insensitive). The C-style operators `&&` and `||` are not part of the language.

### Conditionals

`if ... then ... else ... endif`, with optional `elseif` branches:

```text
if [score] >= 90 then "A"
elseif [score] >= 80 then "B"
else "C" endif
```

### Functions

There are 95 built-in functions for logic, strings, math, dates, and type conversion — see the [function reference](functions.md). Calls can be nested.

```text
uppercase(left([last_name], 3))
round([price] * [qty], 2)
coalesce([nickname], [name], "n/a")
```

### Comments

Everything after `//` on a line is ignored.

```text
[price] * [quantity]        // subtotal
  - ifnull([discount], 0)   // minus discount
```

---

## Worked examples

Classify order volume and compute a discounted price:

```text
if [quantity] > 75 then 'High' else 'Low' endif
```

```text
round([price] * (1 - [discount]), 2)
```

Build a display name and compute tenure in days:

```text
concat(titlecase([first_name]), " ", titlecase([last_name]))
```

```text
date_diff_days(today(), [hire_date])
```

---

## Function reference

| Category | Functions | Examples |
|----------|-----------|----------|
| [Logic & Nulls](functions.md#logic-nulls) | 13 | `coalesce`, `ifnull`, `between`, `is_empty` |
| [String](functions.md#string) | 23 | `concat`, `uppercase`, `trim`, `replace`, `split` |
| [Math](functions.md#math) | 23 | `round`, `abs`, `floor`, `power`, `log` |
| [Date & Time](functions.md#date-time) | 28 | `year`, `add_days`, `date_diff_days`, `format_date` |
| [Type Conversion](functions.md#type-conversion) | 8 | `to_string`, `to_integer`, `to_date`, `to_boolean` |

---

## How it works

1. **Parse** — the formula is tokenized and each token is classified: columns, literals, operators, functions, and `if`/`then` keywords.
2. **Build** — the tokens are arranged into a function tree, applying operator precedence.
3. **Convert** — the tree is turned into a single Polars expression; execution is handled by Polars itself.

For example:

```text
if [score] >= 90 then "A" elseif [score] >= 80 then "B" else "C" endif
```

becomes:

```python
pl.when(pl.col("score") >= pl.lit(90)).then(pl.lit("A"))
  .when(pl.col("score") >= pl.lit(80)).then(pl.lit("B"))
  .otherwise(pl.lit("C"))
```

This is also why flows that use formulas [export to clean Python code](../visual-editor/tutorials/code-generator.md): each formula has a direct Polars equivalent.

---

*Next: browse the [function reference](functions.md), use formulas in the [Formula node](../visual-editor/nodes/transform.md#formula), or call them from the [Python API](../python-api/concepts/formulas.md).*
