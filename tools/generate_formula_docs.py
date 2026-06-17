"""Generate the formula function reference page from polars-expr-transformer docstrings.

Writes docs/users/formulas/functions.md. The library is the source of truth;
rerun via `make formula_docs` after bumping the polars-expr-transformer pin.

The page mirrors the interactive playground's function reference
(https://edwardvaneechoud.github.io/polars_expr_transformer/): a search box,
category filter chips, and a card grid with per-function "Try it" deep links
into the playground. The example grammar, sample datasets, and try-expression
overrides are vendored from the playground's own docs generator so both stay
in sync. Styling and filtering behavior live in
docs/stylesheets/formula-reference.css and docs/javascripts/formula-reference.js.

Output is deterministic: no timestamps, no annotations (their reprs are not
stable across runs), functions sorted alphabetically per category.
"""

import html
import inspect
import re
import urllib.parse
from pathlib import Path

from polars_expr_transformer.funcs import (
    date_functions,
    logic_functions,
    math_functions,
    string_functions,
    type_conversions,
)

CATEGORIES = [
    ("Logic & Nulls", logic_functions),
    ("String", string_functions),
    ("Math", math_functions),
    ("Date & Time", date_functions),
    ("Type Conversion", type_conversions),
]

PLAYGROUND_URL = "https://edwardvaneechoud.github.io/polars_expr_transformer/"
OUTPUT_PATH = Path(__file__).resolve().parents[1] / "docs" / "users" / "formulas" / "functions.md"

CALL_START_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\(")

# Same structured-example grammar the playground's docs generator uses:
# "For example, <call> would return <result> [when <context>]."
EXAMPLE_RE = re.compile(
    r"For example,?\s+(?P<call>.+?)\s+(?:would|will|might|returns?)\s+"
    r"(?:return\s+)?(?P<result>.+?)"
    r"(?:\s+when\s+(?P<context>.+?))?\.?\s*$",
    re.IGNORECASE,
)

# Curated runnable expressions for functions whose docstring example does not
# run against the playground's sample datasets (vendored from the playground's
# docs generator; the dataset key selects the playground dataset).
TRY_OVERRIDES = {
    "year": ("year([hire_date])", "employees"),
    "month": ("month([hire_date])", "employees"),
    "day": ("day([hire_date])", "employees"),
    "week": ("week([hire_date])", "employees"),
    "weekday": ("weekday([hire_date])", "employees"),
    "dayofweek": ("dayofweek([hire_date])", "employees"),
    "dayofyear": ("dayofyear([hire_date])", "employees"),
    "quarter": ("quarter([hire_date])", "employees"),
    "add_days": ("add_days([hire_date], 30)", "employees"),
    "add_weeks": ("add_weeks([hire_date], 2)", "employees"),
    "add_months": ("add_months([hire_date], 6)", "employees"),
    "add_years": ("add_years([hire_date], 1)", "employees"),
    "date_diff_days": ("date_diff_days(today(), [hire_date])", "employees"),
    "start_of_month": ("start_of_month([hire_date])", "employees"),
    "end_of_month": ("end_of_month([hire_date])", "employees"),
    "format_date": ('format_date([hire_date], "%B %d, %Y")', "employees"),
    "date_trim": ('date_trim([order_date], "day")', "orders"),
    "date_truncate": ('date_truncate([order_date], "1d")', "orders"),
    "hour": ("hour([order_date])", "orders"),
    "minute": ("minute([order_date])", "orders"),
    "second": ("second([order_date])", "orders"),
    "add_hours": ("add_hours([order_date], 3)", "orders"),
    "add_minutes": ("add_minutes([order_date], 30)", "orders"),
    "add_seconds": ("add_seconds([order_date], 45)", "orders"),
    "datetime_diff_seconds": ("datetime_diff_seconds(now(), [order_date])", "orders"),
    "datetime_diff_nanoseconds": ("datetime_diff_nanoseconds(now(), [order_date])", "orders"),
    "sin": ("sin([discount])", "orders"),
    "cos": ("cos([discount])", "orders"),
    "tan": ("tan([discount])", "orders"),
    "asin": ("asin([discount])", "orders"),
    "acos": ("acos([discount])", "orders"),
    "atan": ("atan([discount])", "orders"),
    "coalesce": ('coalesce([email], "no email")', "employees"),
    "ifnull": ("ifnull([discount], 0)", "orders"),
    "nvl": ("nvl([discount], 0)", "orders"),
    "is_empty": ("is_empty([email])", "employees"),
    "is_not_empty": ("is_not_empty([email])", "employees"),
    "nullif": ('nullif([status], "cancelled")', "orders"),
    "to_boolean": ("to_boolean(1)", "employees"),
    "to_datetime": ('to_datetime("2023-05-15 14:30:00")', "employees"),
}

# Functions that work locally but not in the browser playground (no
# WebAssembly build for their native dependencies).
BROWSER_UNSUPPORTED = {"string_similarity"}

# Minimal stand-ins for the playground datasets, used to verify that every
# "Try it" expression actually runs. Columns mirror the playground's app.js.
_VALIDATION_FRAMES = {
    "employees": {
        "first_name": ("str", ["John", "Jane"]),
        "last_name": ("str", ["Doe", "Smith"]),
        "age": ("int", [30, 25]),
        "salary": ("float", [50000.0, 60000.0]),
        "department": ("str", ["Sales", "Engineering"]),
        "hire_date": ("date", ["2021-03-15", "2019-07-01"]),
        "email": ("str", ["john.doe@acme.com", None]),
    },
    "orders": {
        "order_id": ("str", ["ORD-0001", "ORD-0002"]),
        "product": ("str", ["Laptop Pro", "Wireless Mouse"]),
        "category": ("str", ["Computers", "Accessories"]),
        "price": ("float", [1299.99, 24.5]),
        "quantity": ("int", [1, 4]),
        "discount": ("float", [0.1, None]),
        "order_date": ("datetime", ["2024-01-15 10:30:00", "2024-01-17 14:05:12"]),
        "status": ("str", ["shipped", "pending"]),
    },
    "events": {
        "event": ("str", ["Kickoff Meeting", "Tech Conference"]),
        "city": ("str", ["Amsterdam", "Lisbon"]),
        "start": ("datetime", ["2024-05-06 09:00:00", "2024-06-18 08:00:00"]),
        "end": ("datetime", ["2024-05-06 10:30:00", "2024-06-20 18:00:00"]),
        "attendees": ("int", [12, None]),
    },
}


def _validation_df(dataset_key: str):
    import polars as pl

    series = []
    for name, (dtype, values) in _VALIDATION_FRAMES[dataset_key].items():
        s = pl.Series(name, values)
        if dtype == "int":
            s = s.cast(pl.Int64)
        elif dtype == "float":
            s = s.cast(pl.Float64)
        elif dtype == "date":
            s = s.str.to_date("%Y-%m-%d")
        elif dtype == "datetime":
            s = s.str.to_datetime("%Y-%m-%d %H:%M:%S")
        series.append(s)
    return pl.DataFrame(series)


def _expression_runs(expression: str, dataset_key: str) -> bool:
    import warnings

    from polars_expr_transformer import simple_function_to_expr

    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            expr = simple_function_to_expr(expression)
            _validation_df(dataset_key).with_columns(expr.alias("result"))
        return True
    except Exception:
        return False


def _find_runnable_dataset(expression: str):
    for dataset_key in _VALIDATION_FRAMES:
        if _expression_runs(expression, dataset_key):
            return dataset_key
    return None


def try_link(name: str, example_call: str | None) -> str | None:
    """Return a playground deep link for a runnable expression, or None."""
    if name in BROWSER_UNSUPPORTED:
        return None
    expression, dataset = None, None
    if example_call:
        dataset = _find_runnable_dataset(example_call)
        if dataset:
            expression = example_call
    if expression is None and name in TRY_OVERRIDES:
        candidate, candidate_dataset = TRY_OVERRIDES[name]
        if _expression_runs(candidate, candidate_dataset):
            expression, dataset = candidate, candidate_dataset
    if expression is None:
        return None
    return f"{PLAYGROUND_URL}#ds={dataset}&expr={urllib.parse.quote(expression, safe='')}"


def collect_functions(module):
    funcs = [
        (name, func)
        for name, func in vars(module).items()
        if callable(func) and not name.startswith("_") and inspect.getmodule(func) is module
    ]
    return sorted(funcs)


def render_signature(name: str, func) -> str:
    parts = []
    for param in inspect.signature(func).parameters.values():
        if param.kind is inspect.Parameter.VAR_POSITIONAL:
            parts.append(f"*{param.name}")
        elif param.kind is inspect.Parameter.VAR_KEYWORD:
            parts.append(f"**{param.name}")
        elif param.default is not inspect.Parameter.empty:
            parts.append(f"{param.name}={param.default!r}")
        else:
            parts.append(param.name)
    return f"{name}({', '.join(parts)})"


def parse_docstring(doc: str) -> dict:
    """Split a docstring into description, example parts, params, and returns."""
    parsed = {
        "description": "",
        "example_call": None,
        "example_result": None,
        "example_context": None,
        "example_text": None,
        "params": [],
        "returns": None,
    }
    paragraphs = [p.strip() for p in re.split(r"\n\s*\n", inspect.cleandoc(doc or "")) if p.strip()]
    description_parts = []
    for paragraph in paragraphs:
        collapsed = re.sub(r"\s+", " ", paragraph)
        lowered = collapsed.lower()
        if lowered.startswith("parameters:"):
            for line in paragraph.splitlines():
                line = line.strip()
                if line.startswith("-"):
                    name, _, desc = line.lstrip("- ").partition(":")
                    parsed["params"].append((name.split(" (")[0].strip(), desc.strip()))
        elif lowered.startswith("returns:"):
            for line in paragraph.splitlines():
                line = line.strip()
                if line.startswith("-"):
                    parsed["returns"] = line.lstrip("- ").strip()
                    break
        elif lowered.startswith(("note:", "raises:")):
            continue
        elif lowered.startswith("for example"):
            match = EXAMPLE_RE.match(collapsed)
            if match:
                parsed["example_call"] = match.group("call")
                parsed["example_result"] = match.group("result")
                parsed["example_context"] = match.group("context")
            else:
                parsed["example_text"] = re.sub(r"^For example[,:]?\s*", "", collapsed)
        else:
            description_parts.append(collapsed)
    parsed["description"] = " ".join(description_parts)
    return parsed


def render_example(parsed: dict) -> str:
    if parsed["example_call"]:
        result = html.escape(parsed["example_result"] or "")
        if " " not in result or (parsed["example_result"] or "")[:1] in "\"'":
            result = f'<code class="fn-result">{result}</code>'
        else:
            result = f'<span class="fn-result">{result}</span>'
        parts = [f"<code>{html.escape(parsed['example_call'])}</code> " f'<span class="fn-arrow">→</span> {result}']
        if parsed["example_context"]:
            parts.append(f'<span class="fn-ctx">when {html.escape(parsed["example_context"])}</span>')
        return f'<div class="fn-example">{"".join(parts)}</div>'
    if parsed["example_text"]:
        return f'<div class="fn-example"><code>{html.escape(parsed["example_text"])}</code></div>'
    return ""


def render_params(func, parsed: dict) -> str:
    documented = dict(parsed["params"])
    items = []
    for param in inspect.signature(func).parameters.values():
        display = f"*{param.name}" if param.kind is inspect.Parameter.VAR_POSITIONAL else param.name
        description = documented.get(param.name, "")
        entry = f"<li><code>{html.escape(display)}</code>"
        if description:
            entry += f" — {html.escape(description)}"
        items.append(entry + "</li>")
    if parsed["returns"]:
        items.append(f"<li><strong>Returns</strong> — {html.escape(parsed['returns'])}</li>")
    if not items:
        return ""
    return (
        '<details class="fn-params"><summary><span class="fn-toggle">▸</span> '
        "Parameters &amp; return value</summary><ul>" + "".join(items) + "</ul></details>"
    )


def render_card(name: str, func, category_title: str) -> str:
    parsed = parse_docstring(func.__doc__)
    signature = render_signature(name, func)
    search_text = html.escape(f"{signature} {parsed['description']} {category_title}".lower(), quote=True)
    lines = [f'<article class="fn-card" id="{name}" data-text="{search_text}">']
    header = f'<header><code class="fn-sig">{html.escape(signature)}</code>'
    link = try_link(name, parsed["example_call"])
    if link:
        header += f'<a class="fn-try" href="{html.escape(link)}" target="_blank" rel="noopener">Try it ▸</a>'
    header += "</header>"
    lines.append(header)
    if parsed["description"]:
        lines.append(f'<p class="fn-desc">{html.escape(parsed["description"])}</p>')
    example = render_example(parsed)
    if example:
        lines.append(example)
    params = render_params(func, parsed)
    if params:
        lines.append(params)
    lines.append("</article>")
    return "\n".join(lines)


def slugify(title: str) -> str:
    """Mirror python-markdown's toc slugify: drop symbols, collapse spaces/dashes."""
    cleaned = re.sub(r"[^a-z0-9\s-]", "", title.lower()).strip()
    return re.sub(r"[\s-]+", "-", cleaned)


def build_page() -> str:
    total = sum(len(collect_functions(module)) for _, module in CATEGORIES)
    chips = [f'<button class="fn-chip active" data-cat="all">All <span>{total}</span></button>']
    sections = []
    for title, module in CATEGORIES:
        functions = collect_functions(module)
        if not functions:
            continue
        slug = slugify(title)
        chips.append(
            f'<button class="fn-chip" data-cat="{slug}">{html.escape(title)} <span>{len(functions)}</span></button>'
        )
        cards = "\n".join(render_card(name, func, title) for name, func in functions)
        sections.append(
            f'<section class="fn-category" id="{slug}" data-cat="{slug}">\n'
            f'<h2>{html.escape(title)} <span class="fn-count">({len(functions)})</span></h2>\n'
            f'<div class="fn-grid">\n{cards}\n</div>\n'
            "</section>"
        )

    return "\n".join(
        [
            "<!-- AUTO-GENERATED by tools/generate_formula_docs.py — do not edit. Run 'make formula_docs'. -->",
            "",
            "# Function Reference",
            "",
            f"All {total} built-in functions of the [Flowfile formula language](index.md). "
            "This reference is generated from the library's docstrings. Unless noted, every argument accepts a "
            "literal value, a `[column]` reference or a nested expression. Click *Try it ▸* to load an example "
            f"into the [interactive playground]({PLAYGROUND_URL}).",
            "",
            '<input class="fn-search" type="search" '
            "placeholder=\"Search functions… (e.g. 'date', 'trim', 'null')\" "
            'aria-label="Search functions">',
            "",
            f'<div class="fn-chips">{"".join(chips)}</div>',
            "",
            "\n\n".join(sections),
            "",
            '<p class="fn-no-results" hidden>No functions match your search.</p>',
            "",
        ]
    )


def main() -> None:
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(build_page(), encoding="utf-8")
    print(f"Wrote {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
