"""Node emitters for the plain-Python (teaching) export.

Every table is a ``list[dict]`` — one dict per row, keyed by column name. That
is what ``csv.DictReader`` hands you and what a beginner draws on paper, and it
is what makes the group-by lesson (an accumulator dict) land.

These emitters write things out longhand instead of factoring out helpers: the
repetition *is* the lesson. What they do factor out is the prose — a long
explanation is emitted only the first time a pattern appears (see ``_teach``).

Matching the engine matters more than looking tidy, and three Polars behaviours
drive most of the awkward-looking code here:

* a null sorts **first**, in both directions;
* a null join key matches **nothing**;
* an aggregate over an all-null group is **null**, except ``sum`` which is 0.
"""

from flowfile_core.flowfile.code_generator.base import ConverterMixinBase
from flowfile_core.flowfile.flow_data_engine.flow_file_column.main import FlowfileColumn
from flowfile_core.schemas import input_schema, transform_schema

# Aggregations over `values`, the group's non-null values for that column. An agg
# outside this map (and _AGG_RAW / _AGG_STATISTICS) turns the group_by into an
# exercise stub rather than emitting something subtly wrong.
_AGG_OVER_VALUES: dict[str, str] = {
    "sum": "sum({v})",
    "min": "min({v}, default=None)",
    "max": "max({v}, default=None)",
    "count": "len({v})",
    "concat": '",".join(str(x) for x in {v})',
    "mean": "sum({v}) / len({v}) if {v} else None",
}

# Aggregations that need the raw rows (nulls included) rather than the values list.
_AGG_RAW: dict[str, str] = {
    "first": "{rows}[0][{col}]",
    "last": "{rows}[-1][{col}]",
    "n_unique": "len({{r[{col}] for r in {rows}}})",
}

# statistics-module aggregations, with the minimum sample size Polars needs
# before it stops returning null.
_AGG_STATISTICS: dict[str, tuple[str, int]] = {
    "median": ("statistics.median", 1),
    "std": ("statistics.stdev", 2),
    "var": ("statistics.variance", 2),
}

_COMPARISON_OPS: dict[str, str] = {
    "equals": "==",
    "not_equals": "!=",
    "greater_than": ">",
    "greater_than_or_equals": ">=",
    "less_than": "<",
    "less_than_or_equals": "<=",
}

# Only these join flavours are emitted. A right/full/outer join needs the engine's
# column-collision bookkeeping to be replayed in a second direction; getting that
# subtly wrong is worse than saying so, and "a right join is a left join with the
# tables swapped" is the lesson anyway.
_JOIN_HOWS_SUPPORTED = frozenset({"inner", "left", "semi", "anti"})

# Loop variables the emitters bind. Uniquified against user-pinned node references
# so a node called `row` can't collide with `for row in ...`.
_TEMP_NAMES = (
    "row",
    "key",
    "values",
    "groups",
    "rows_in_group",
    "seen",
    "counts",
    "by_key",
    "right_index",
    "matches",
    "left_row",
    "right_row",
    "part",
    "columns",
    "handle",
)


class PlainPythonUnsupported(Exception):
    """Raised by an emitter that cannot express this node's settings in plain Python.

    The converter catches it and emits an exercise stub instead, so one awkward
    node never costs you the whole script.
    """

    def __init__(self, reason: str):
        self.reason = reason
        super().__init__(reason)


class PlainPythonHandlersMixin(ConverterMixinBase):
    """Emitters that render each supported node type as plain Python loops."""

    # ---------------------------------------------------------------- helpers

    def _tmp(self, name: str) -> str:
        return self._temp_names[name]

    def _section(self, title: str) -> None:
        self._add_code(f"# --- {title} " + "-" * max(3, 62 - len(title)))

    def _teach(self, key: str, *lines: str, indent: int = 0) -> None:
        """Emit an explanatory comment block the first time a pattern shows up."""
        if key in self._taught:
            return
        self._taught.add(key)
        pad = " " * indent
        for line in lines:
            self._add_code(f"{pad}# {line}" if line else f"{pad}#")

    def _columns_of(self, node_id: int | None) -> list[FlowfileColumn]:
        """Predicted output schema of a node, or [] when it can't be resolved."""
        if node_id is None or node_id < 0:
            return []
        try:
            node = self.flow_graph.get_node(node_id)
            return list(node.get_predicted_schema() or []) if node is not None else []
        except Exception:
            return []

    def _column_names_of(self, node_id: int | None) -> list[str]:
        return [c.column_name for c in self._columns_of(node_id)]

    def _row(self, column: str, var: str | None = None) -> str:
        return f"{var or self._tmp('row')}[{self._py_str(column)}]"

    @staticmethod
    def _normalize_dtype(data_type: str | None) -> str | None:
        if not data_type:
            return None
        name = str(data_type).strip()
        if name.startswith(("Int", "UInt")) or name.lower() in ("integer", "int"):
            return "int"
        if name.startswith(("Float", "Decimal")) or name.lower() in ("double", "float"):
            return "float"
        if name == "Boolean" or name.lower() == "bool":
            return "bool"
        if name == "Date":
            return "date"
        if name.startswith("Datetime"):
            return "datetime"
        if name in ("String", "Utf8") or name.lower() in ("str", "string"):
            return "str"
        return None

    def _cast_expr(self, value: str, data_type: str | None) -> str | None:
        """Null-safe Python expression casting ``value`` to a Polars dtype, or None."""
        kind = self._normalize_dtype(data_type)
        if kind is None:
            return None
        if kind == "bool":
            # Reached from CSV text ("true") and from numbers (1/0) alike.
            inner = f'str({value}).strip().lower() in ("true", "1", "yes")'
        elif kind == "date":
            self.imports.add("import datetime")
            inner = f"datetime.date.fromisoformat(str({value}))"
        elif kind == "datetime":
            self.imports.add("import datetime")
            inner = f"datetime.datetime.fromisoformat(str({value}))"
        else:
            inner = f"{kind}({value})"
        return f"{inner} if {value} is not None else None"

    def _literal(self, node_id: int, field: str, raw: str) -> str:
        """Render a filter's string-typed setting value as a Python literal.

        The schema stores every basic-filter value as text, so the column's dtype
        decides whether it means the number 100 or the string "100".
        """
        kind = self._normalize_dtype(self._column_dtype(node_id, field))
        try:
            if kind == "int":
                return repr(int(raw))
            if kind == "float":
                return repr(float(raw))
        except (TypeError, ValueError):
            return self._py_str(raw)
        if kind == "bool":
            return "True" if str(raw).strip().lower() in ("true", "1", "yes") else "False"
        return self._py_str(raw)

    def _key_tuple(self, columns: list[str], var: str | None = None) -> str:
        inner = ", ".join(self._row(c, var) for c in columns)
        return f"({inner},)" if len(columns) == 1 else f"({inner})"

    def _sort_key(self, columns: list[str], descending: bool) -> str:
        """Sort key placing nulls first in *both* directions, matching Polars.

        Under ``reverse=True`` the largest key comes first, so a descending sort
        flips the null flag to keep missing values at the front.
        """
        parts: list[str] = []
        for column in columns:
            target = self._row(column)
            parts.append(f"{target} is None" if descending else f"{target} is not None")
            parts.append(target)
        return "(" + ", ".join(parts) + ")"

    @staticmethod
    def _safe_ident(name: str, fallback: str) -> str:
        cleaned = "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in name).strip("_")
        if not cleaned or cleaned[0].isdigit():
            cleaned = f"{fallback}_{cleaned}" if cleaned else fallback
        return cleaned.lower()

    # ---------------------------------------------------------------- sources

    def _handle_manual_input(
        self, settings: input_schema.NodeManualInput, var_name: str, input_vars: dict[str, str]
    ) -> None:
        raw = settings.raw_data_format
        self._section("Manual input")
        self._teach(
            "row_model",
            "A table is a list of dicts: one dict per row, keyed by column name.",
            "That is the only data structure this whole script uses.",
        )
        if not raw.columns or not raw.data or not raw.data[0]:
            self._add_code(f"{var_name} = []")
            self._add_code("")
            return

        # The engine casts the stored values to each column's declared dtype, so the
        # literals have to be cast here too or a downstream `> 100` compares strings.
        casters = {c.name: self._normalize_dtype(c.data_type) for c in raw.columns}
        rows: list[dict] = []
        for row_index in range(len(raw.data[0])):
            row: dict = {}
            for column_index, column in enumerate(raw.columns):
                row[column.name] = self._coerce_literal(raw.data[column_index][row_index], casters[column.name])
            rows.append(row)

        self._add_code(f"{var_name} = [")
        for row in rows:
            self._add_code(f"    {row!r},")
        self._add_code("]")
        self._add_code("")

    def _coerce_literal(self, value, kind: str | None):
        """Apply a declared dtype to a stored literal, mirroring the engine's cast."""
        if value is None or kind is None:
            self._note_literal_imports(value)
            return value
        try:
            if kind == "int":
                converted = int(value)
            elif kind == "float":
                converted = float(value)
            elif kind == "bool":
                converted = str(value).strip().lower() in ("true", "1", "yes")
            elif kind == "str":
                converted = str(value)
            else:
                converted = value
        except (TypeError, ValueError):
            converted = value
        self._note_literal_imports(converted)
        return converted

    def _note_literal_imports(self, value) -> None:
        """repr() of a date or Decimal names a module the script must import."""
        module = type(value).__module__
        if module == "datetime":
            self.imports.add("import datetime")
        elif module == "decimal":
            self.imports.add("import decimal")

    def _handle_flow_input(
        self, settings: input_schema.NodeFlowInput, var_name: str, input_vars: dict[str, str]
    ) -> None:
        if settings.raw_data_format is not None and settings.raw_data_format.columns:
            self._handle_manual_input(settings, var_name, input_vars)
        else:
            self._add_code(f"{var_name} = []  # subflow input '{settings.input_name}'")
            self._add_code("")

    def _handle_flow_output(
        self, settings: input_schema.NodeFlowOutput, var_name: str, input_vars: dict[str, str]
    ) -> None:
        self._add_code(f"{var_name} = {input_vars.get('main', 'rows')}")
        self._add_code("")

    def _handle_explore_data(
        self, settings, var_name: str, input_vars: dict[str, str]
    ) -> None:
        """A canvas-only preview node: emit nothing and pass the rows through."""
        main = input_vars.get("main")
        if main:
            self.node_var_mapping[settings.node_id] = main

    def _handle_read(self, settings: input_schema.NodeRead, var_name: str, input_vars: dict[str, str]) -> None:
        received = settings.received_file
        if received.file_type != "csv":
            raise PlainPythonUnsupported(
                f"reading {received.file_type} files needs a library; only CSV is plain-Python friendly"
            )
        table = received.table_settings
        self._section(f"Read {received.name or received.abs_file_path}")
        self._teach(
            "csv_reader",
            "csv.DictReader turns each line into a dict keyed by the header row —",
            "the same shape the manual-input node produces.",
        )
        self.imports.add("import csv")

        encoding = table.encoding or "utf-8"
        path = self._py_str(received.abs_file_path)
        self._add_code(f'with open({path}, newline="", encoding={self._py_str(encoding)}) as {self._tmp("handle")}:')
        if table.starting_from_line:
            self._add_code(f'    for _ in range({table.starting_from_line}):')
            self._add_code(f'        next({self._tmp("handle")})')
        reader_args = f'{self._tmp("handle")}, delimiter={self._py_str(table.delimiter)}'
        if table.quote_char:
            reader_args += f", quotechar={self._py_str(table.quote_char)}"
        if not table.has_headers:
            names = self._column_names_of(settings.node_id)
            if not names:
                raise PlainPythonUnsupported("a headerless CSV needs a known schema to name the columns")
            reader_args += f", fieldnames={names!r}"
        self._add_code(f"    {var_name} = list(csv.DictReader({reader_args}))")
        self._add_code("")
        self._emit_csv_typing(settings.node_id, var_name)

    def _emit_csv_typing(self, node_id: int, var_name: str) -> None:
        """Turn blank cells into None and text into the types Polars inferred.

        This is the step a dataframe library performs invisibly at read time, and
        it is why `amount > 100` works on a DataFrame and raises TypeError on a
        freshly-parsed CSV.
        """
        row = self._tmp("row")
        casts = [
            (column.column_name, cast)
            for column in self._columns_of(node_id)
            if (cast := self._cast_expr(f"{row}[{self._py_str(column.column_name)}]", column.data_type))
            and self._normalize_dtype(column.data_type) != "str"
        ]
        self._teach(
            "csv_types",
            "A CSV file is text: every value arrives as a string and a blank cell",
            "arrives as an empty string, not as 'missing'. Polars works both out",
            "while reading; here you do it yourself.",
        )
        self._add_code(f"for {row} in {var_name}:")
        self._add_code(f"    for {self._tmp('key')} in {row}:")
        self._add_code(f'        if {row}[{self._tmp("key")}] == "":')
        self._add_code(f'            {row}[{self._tmp("key")}] = None')
        for column, cast in casts:
            self._add_code(f"    {row}[{self._py_str(column)}] = {cast}")
        self._add_code("")

    # ------------------------------------------------------------- transforms

    def _handle_record_count(
        self, settings: input_schema.NodeRecordCount, var_name: str, input_vars: dict[str, str]
    ) -> None:
        rows = input_vars.get("main", "rows")
        self._section("Record count")
        self._add_code(f'{var_name} = [{{"number_of_records": len({rows})}}]')
        self._add_code("")

    def _predicate_for(self, basic: transform_schema.BasicFilter, node_id: int) -> tuple[str, bool]:
        """Return (python expression over the row, needs_null_guard)."""
        field = basic.field
        operator = basic.operator.value if hasattr(basic.operator, "value") else str(basic.operator)
        target = self._row(field)

        if operator == "is_null":
            return f"{target} is None", False
        if operator == "is_not_null":
            return f"{target} is not None", False

        if operator in _COMPARISON_OPS:
            return f"{target} {_COMPARISON_OPS[operator]} {self._literal(node_id, field, basic.value)}", True
        if operator in ("contains", "not_contains"):
            # Polars str.contains takes a regex, not a literal substring.
            self.imports.add("import re")
            search = f"re.search({self._py_str(str(basic.value))}, str({target}))"
            return (f"{search} is not None" if operator == "contains" else f"{search} is None"), True
        if operator == "starts_with":
            return f"{target}.startswith({self._literal(node_id, field, basic.value)})", True
        if operator == "ends_with":
            return f"{target}.endswith({self._literal(node_id, field, basic.value)})", True
        if operator in ("in", "not_in"):
            members = [self._literal(node_id, field, part.strip()) for part in str(basic.value).split(",")]
            joined = ", ".join(members)
            keyword = "in" if operator == "in" else "not in"
            suffix = f"({joined},)" if len(members) == 1 else f"({joined})"
            return f"{target} {keyword} {suffix}", True
        if operator == "between":
            if basic.value2 is None or str(basic.value2) == "":
                raise PlainPythonUnsupported("this BETWEEN filter has no upper bound, so the flow itself fails")
            low = self._literal(node_id, field, basic.value)
            high = self._literal(node_id, field, basic.value2)
            return f"{low} <= {target} <= {high}", True

        raise PlainPythonUnsupported(f"filter operator '{operator}' has no plain-Python equivalent yet")

    def _handle_filter(self, settings: input_schema.NodeFilter, var_name: str, input_vars: dict[str, str]) -> None:
        rows = input_vars.get("main", "rows")
        if settings.filter_input.is_advanced():
            raise PlainPythonUnsupported(
                "this filter uses an advanced expression, which Flowfile evaluates with its "
                "expression engine rather than a plain comparison"
            )
        basic = settings.filter_input.basic_filter
        if basic is None or not basic.field:
            self._add_code(f"{var_name} = {rows}  # no filter configured")
            self._add_code("")
            return

        predicate, needs_guard = self._predicate_for(basic, settings.node_id)
        row = self._tmp("row")
        self._section(f"Filter on {basic.field}")
        self._teach(
            "filter_loop",
            "Walk the rows one at a time and keep the ones that pass the test.",
            "This is the loop a dataframe's .filter() runs for you.",
        )
        if needs_guard:
            self._teach(
                "null_guard",
                "The `is not None` guard matches Polars: a comparison against a missing",
                "value is neither true nor false there, and the row is dropped.",
            )
        guard = f"{self._row(basic.field)} is not None and " if needs_guard else ""

        if settings.split_mode:
            pass_var, fail_var = f"{var_name}_pass", f"{var_name}_fail"
            self._add_code(f"{pass_var} = []")
            self._add_code(f"{fail_var} = []")
            self._add_code(f"for {row} in {rows}:")
            if needs_guard:
                self._add_code(f"    if {self._row(basic.field)} is None:")
                self._add_code("        continue")
            self._add_code(f"    if {predicate}:")
            self._add_code(f"        {pass_var}.append({row})")
            self._add_code("    else:")
            self._add_code(f"        {fail_var}.append({row})")
            self._add_code("")
            self.node_handle_var_mapping[(settings.node_id, "output-0")] = pass_var
            self.node_handle_var_mapping[(settings.node_id, "output-1")] = fail_var
            self.node_var_mapping[settings.node_id] = pass_var
            return

        self._add_code(f"{var_name} = []")
        self._add_code(f"for {row} in {rows}:")
        self._add_code(f"    if {guard}{predicate}:")
        self._add_code(f"        {var_name}.append({row})")
        self._add_code("")

    def _handle_select(self, settings: input_schema.NodeSelect, var_name: str, input_vars: dict[str, str]) -> None:
        rows = input_vars.get("main", "rows")
        row = self._tmp("row")
        selects = [s for s in settings.select_input if s.is_available]
        kept = [s for s in selects if s.keep]
        mentioned = {s.old_name for s in selects}

        incoming = self._column_names_of(settings.depending_on_id)
        extras = [c for c in incoming if c not in mentioned] if settings.keep_missing else []

        ordered = sorted(kept, key=lambda s: (s.position is not None, s.position or 0))
        pairs: list[tuple[str, str, str | None]] = [
            (s.new_name or s.old_name, s.old_name, s.data_type) for s in ordered
        ]
        pairs.extend((c, c, None) for c in extras)
        if not pairs:
            raise PlainPythonUnsupported("this select keeps no columns")

        self._section("Select columns")
        self._teach(
            "select_loop",
            "Build a fresh dict per row containing only the columns you want,",
            "under the names you want. Renaming is just a different key.",
        )
        self._add_code(f"{var_name} = []")
        self._add_code(f"for {row} in {rows}:")
        self._add_code(f"    {var_name}.append({{")
        for new_name, old_name, cast_to in pairs:
            value = self._row(old_name)
            # The engine casts whenever a dtype is set, not only when the UI flagged a change.
            current = self._column_dtype(settings.depending_on_id, old_name)
            if cast_to and self._normalize_dtype(cast_to) != self._normalize_dtype(current):
                value = self._cast_expr(value, cast_to) or value
            self._add_code(f"        {self._py_str(new_name)}: {value},")
        self._add_code("    })")
        self._add_code("")

    def _handle_sort(self, settings: input_schema.NodeSort, var_name: str, input_vars: dict[str, str]) -> None:
        rows = input_vars.get("main", "rows")
        row = self._tmp("row")
        sorts = [s for s in settings.sort_input if s.column]
        if not sorts:
            self._add_code(f"{var_name} = list({rows})  # no sort configured")
            self._add_code("")
            return

        self._section("Sort")
        self._teach(
            "sort_key",
            "sorted() takes a `key` function saying what to order by. The",
            "`is not None` term keeps missing values first, which is where",
            "Polars puts them — and it stops Python comparing None with a number.",
        )
        directions = {bool(s.descending) for s in sorts}
        if len(directions) == 1:
            descending = directions.pop()
            key = self._sort_key([s.column for s in sorts], descending)
            suffix = ", reverse=True" if descending else ""
            self._add_code(f"{var_name} = sorted({rows}, key=lambda {row}: {key}{suffix})")
        else:
            self._teach(
                "sort_stable",
                "Python's sort is *stable*: equal rows keep their relative order. So",
                "sorting by the least important key first and the most important key",
                "last gives a multi-key sort with mixed directions.",
            )
            self._add_code(f"{var_name} = list({rows})")
            for sort in reversed(sorts):
                key = self._sort_key([sort.column], bool(sort.descending))
                suffix = ", reverse=True" if sort.descending else ""
                self._add_code(f"{var_name}.sort(key=lambda {row}: {key}{suffix})")
        self._add_code("")

    def _handle_unique(self, settings: input_schema.NodeUnique, var_name: str, input_vars: dict[str, str]) -> None:
        rows = input_vars.get("main", "rows")
        row, key = self._tmp("row"), self._tmp("key")
        columns = settings.unique_input.columns or self._column_names_of(settings.depending_on_id)
        if not columns:
            raise PlainPythonUnsupported("cannot resolve which columns make a row unique")
        strategy = settings.unique_input.strategy or "any"

        self._section("Unique")
        if strategy in ("any", "first"):
            seen = self._tmp("seen")
            self._teach(
                "seen_set",
                "A set remembers what you have already seen, and checking membership in",
                "a set stays fast however big it gets. The key is a tuple because sets",
                "can only hold immutable values (a list would not work).",
            )
            self._add_code(f"{seen} = set()")
            self._add_code(f"{var_name} = []")
            self._add_code(f"for {row} in {rows}:")
            self._add_code(f"    {key} = {self._key_tuple(columns)}")
            self._add_code(f"    if {key} not in {seen}:")
            self._add_code(f"        {seen}.add({key})")
            self._add_code(f"        {var_name}.append({row})")
        elif strategy == "last":
            by_key = self._tmp("by_key")
            self._add_code(f"{by_key} = {{}}")
            self._add_code(f"for {row} in {rows}:")
            self._add_code(f"    {by_key}[{self._key_tuple(columns)}] = {row}  # a later row overwrites an earlier one")
            self._add_code(f"{var_name} = list({by_key}.values())")
        else:  # "none" -> drop every row that has a duplicate
            counts = self._tmp("counts")
            self._add_code(f"{counts} = {{}}")
            self._add_code(f"for {row} in {rows}:")
            self._add_code(f"    {key} = {self._key_tuple(columns)}")
            self._add_code(f"    {counts}[{key}] = {counts}.get({key}, 0) + 1")
            self._add_code(f"{var_name} = []")
            self._add_code(f"for {row} in {rows}:")
            self._add_code(f"    if {counts}[{self._key_tuple(columns)}] == 1:")
            self._add_code(f"        {var_name}.append({row})")
        self._add_code("")

    def _handle_group_by(self, settings: input_schema.NodeGroupBy, var_name: str, input_vars: dict[str, str]) -> None:
        rows = input_vars.get("main", "rows")
        row, key = self._tmp("row"), self._tmp("key")
        group_of = self._tmp("rows_in_group")
        agg_cols = settings.groupby_input.agg_cols
        group_cols = [a for a in agg_cols if a.agg == "groupby"]
        aggregations = [a for a in agg_cols if a.agg != "groupby"]

        for agg in aggregations:
            if agg.agg not in _AGG_OVER_VALUES and agg.agg not in _AGG_RAW and agg.agg not in _AGG_STATISTICS:
                raise PlainPythonUnsupported(f"the '{agg.agg}' aggregation has no plain-Python equivalent yet")

        self._section("Group by")
        if not group_cols:
            self._teach("whole_frame_agg", "No grouping columns, so the whole table is one group.")
            self._add_code(f"{group_of} = {rows}")
            self._emit_value_lists(aggregations, group_of, indent=0)
            self._add_code(f"{var_name} = [{{")
            for agg in aggregations:
                self._add_code(f"    {self._py_str(agg.new_name)}: {self._agg_expression(agg, group_of)},")
            self._add_code("}]")
            self._add_code("")
            return

        groups = self._tmp("groups")
        self._teach(
            "accumulator",
            "The accumulator-dict pattern: walk the rows once and file each one",
            "under its key. Then walk the groups and summarise each one.",
            "This is the most reusable loop in programming — it is how you would",
            "count words, bucket log lines, or tally votes in any language.",
        )
        self._add_code(f"{groups} = {{}}")
        self._add_code(f"for {row} in {rows}:")
        self._add_code(f"    {key} = {self._key_tuple([a.old_name for a in group_cols])}")
        self._add_code(f"    if {key} not in {groups}:")
        self._add_code(f"        {groups}[{key}] = []")
        self._add_code(f"    {groups}[{key}].append({row})")
        self._add_code("")

        self._add_code(f"{var_name} = []")
        self._add_code(f"for {key}, {group_of} in {groups}.items():")
        self._emit_value_lists(aggregations, group_of, indent=4)
        self._add_code(f"    {var_name}.append({{")
        for index, agg in enumerate(group_cols):
            self._add_code(f"        {self._py_str(agg.new_name or agg.old_name)}: {key}[{index}],")
        for agg in aggregations:
            self._add_code(f"        {self._py_str(agg.new_name)}: {self._agg_expression(agg, group_of)},")
        self._add_code("    })")
        self._add_code("")

    def _plan_value_list_names(self, columns: list[str]) -> None:
        """Bind one identifier per aggregated column, deduped.

        Sanitising is lossy — "my col" and "my-col" both become ``my_col`` — so the
        names are uniquified or one column's values would silently shadow another's.
        """
        self._value_names = {}
        used: set[str] = set(self._temp_names.values())
        for column in columns:
            base = f"{self._safe_ident(column, 'column')}_{self._tmp('values')}"
            candidate, bump = base, 2
            while candidate in used:
                candidate, bump = f"{base}_{bump}", bump + 1
            used.add(candidate)
            self._value_names[column] = candidate

    def _value_list_name(self, column: str) -> str:
        return self._value_names[column]

    def _emit_value_lists(self, aggregations: list, group_of: str, indent: int) -> None:
        """Bind one non-null value list per aggregated column.

        Doing this before the dict literal is what lets min/max/mean/std return
        None on an empty or all-null group instead of raising, which is what
        Polars does — and it reads better than inlining the same filter N times.
        """
        needed = sorted(
            {agg.old_name for agg in aggregations if agg.agg in _AGG_OVER_VALUES or agg.agg in _AGG_STATISTICS}
        )
        self._plan_value_list_names(needed)
        if not needed:
            return
        pad = " " * indent
        self._teach(
            "agg_values",
            "Pull out the non-empty values first: an aggregate over nothing is",
            "empty in Polars, not an error, and this is what makes that possible.",
            indent=indent,
        )
        for column in sorted(needed):
            name = self._value_list_name(column)
            quoted = self._py_str(column)
            self._add_code(f"{pad}{name} = [r[{quoted}] for r in {group_of} if r[{quoted}] is not None]")

    def _agg_expression(self, agg: transform_schema.AggColl, group_of: str) -> str:
        column = self._py_str(agg.old_name)
        if agg.agg in _AGG_RAW:
            return _AGG_RAW[agg.agg].format(rows=group_of, col=column)
        values = self._value_list_name(agg.old_name)
        if agg.agg in _AGG_OVER_VALUES:
            return _AGG_OVER_VALUES[agg.agg].format(v=values)
        function, minimum = _AGG_STATISTICS[agg.agg]
        self.imports.add("import statistics")
        return f"{function}({values}) if len({values}) >= {minimum} else None"

    def _handle_union(self, settings: input_schema.NodeUnion, var_name: str, input_vars: dict[str, str]) -> None:
        parts = [value for key, value in input_vars.items() if key.startswith("main")]
        if not parts:
            raise PlainPythonUnsupported("union has no inputs")
        row, part = self._tmp("row"), self._tmp("part")

        # Column order comes from the input schemas, not the rows: an input that
        # contributes zero rows still contributes its columns.
        columns: list[str] = []
        for upstream_id in getattr(settings, "depending_on_ids", []) or []:
            for name in self._column_names_of(upstream_id):
                if name not in columns:
                    columns.append(name)

        self._section("Union")
        self._teach(
            "union_concat",
            "Stacking tables is adding lists together. Flowfile fills in any column",
            "a table is missing with None, so line the keys up afterwards.",
        )
        self._add_code(f"{var_name} = []")
        self._add_code(f"for {part} in [{', '.join(parts)}]:")
        self._add_code(f"    {var_name}.extend({part})")
        if columns:
            self._add_code(f"{var_name} = [{{c: {row}.get(c) for c in {columns!r}}} for {row} in {var_name}]")
        else:
            all_columns = self._tmp("columns")
            self._add_code(f"{all_columns} = []")
            self._add_code(f"for {row} in {var_name}:")
            self._add_code(f"    for c in {row}:")
            self._add_code(f"        if c not in {all_columns}:")
            self._add_code(f"            {all_columns}.append(c)")
            self._add_code(
                f"{var_name} = [{{c: {row}.get(c) for c in {all_columns}}} for {row} in {var_name}]"
            )
        self._add_code("")

    # ------------------------------------------------------------------ joins

    def _handle_join(self, settings: input_schema.NodeJoin, var_name: str, input_vars: dict[str, str]) -> None:
        left_rows = input_vars.get("main", "left_rows")
        right_rows = input_vars.get("right", "right_rows")
        how = settings.join_input.how
        how = how.value if hasattr(how, "value") else str(how)
        if how not in _JOIN_HOWS_SUPPORTED:
            raise PlainPythonUnsupported(
                f"'{how}' joins are not covered yet — a right join is a left join with the two "
                "tables swapped, which is worth trying by hand"
            )
        if not settings.join_input.join_mapping:
            raise PlainPythonUnsupported("this join has no join keys")

        # The engine suffixes colliding right-hand columns with _right at run time;
        # replay that here or the emitted dict literal repeats a key and the left
        # value is silently overwritten.
        manager = transform_schema.JoinInputManager(settings.join_input)
        manager.auto_rename()
        join_input = manager.input

        left_keys = [m.left_col for m in join_input.join_mapping]
        right_keys = [m.right_col for m in join_input.join_mapping]

        row, key = self._tmp("row"), self._tmp("key")
        index, matches = self._tmp("right_index"), self._tmp("matches")
        left_row, right_row = self._tmp("left_row"), self._tmp("right_row")

        self._section(f"Join ({how})")
        self._teach(
            "hash_join",
            "Index the right-hand table first, then look each left row up in it.",
            "The obvious version — a loop inside a loop — compares every left row",
            "against every right row: len(left) x len(right) comparisons. Indexing",
            "first makes it len(left) + len(right). That difference is the whole",
            "reason databases and dataframe libraries build indexes.",
        )
        self._add_code(f"{index} = {{}}")
        self._add_code(f"for {row} in {right_rows}:")
        self._add_code(f"    {key} = {self._key_tuple(right_keys)}")
        self._teach(
            "null_join_key",
            "A missing key never matches in Polars, not even another missing key.",
            indent=4,
        )
        self._add_code(f"    if any(v is None for v in {key}):")
        self._add_code("        continue")
        self._add_code(f"    if {key} not in {index}:")
        self._add_code(f"        {index}[{key}] = []")
        self._add_code(f"    {index}[{key}].append({row})")
        self._add_code("")

        self._add_code(f"{var_name} = []")
        self._add_code(f"for {left_row} in {left_rows}:")
        self._add_code(f"    {key} = {self._key_tuple(left_keys, left_row)}")
        self._add_code(
            f"    {matches} = [] if any(v is None for v in {key}) else {index}.get({key}, [])"
        )

        if how in ("semi", "anti"):
            test = matches if how == "semi" else f"not {matches}"
            self._add_code(f"    if {test}:")
            self._add_code(f"        {var_name}.append({left_row})")
            self._add_code("")
            return

        left_pairs = self._join_output_pairs(join_input.left_select, left_row)
        right_pairs = self._join_output_pairs(join_input.right_select, right_row)

        self._add_code(f"    for {right_row} in {matches}:")
        self._add_code(f"        {var_name}.append({{")
        for new_name, expression in left_pairs + right_pairs:
            self._add_code(f"            {self._py_str(new_name)}: {expression},")
        self._add_code("        })")

        if how == "left":
            self._teach(
                "left_join_fill",
                "An unmatched left row still comes through, with the right side empty.",
                indent=4,
            )
            self._add_code(f"    if not {matches}:")
            self._add_code(f"        {var_name}.append({{")
            for new_name, expression in left_pairs:
                self._add_code(f"            {self._py_str(new_name)}: {expression},")
            for new_name, _ in right_pairs:
                self._add_code(f"            {self._py_str(new_name)}: None,")
            self._add_code("        })")
        self._add_code("")

    def _join_output_pairs(self, selects, row_var: str) -> list[tuple[str, str]]:
        """(output column name, python expression) for one side of a join."""
        renames = getattr(selects, "renames", None) or []
        return [
            (rename.new_name or rename.old_name, self._row(rename.old_name, row_var))
            for rename in renames
            if rename.keep and rename.is_available
        ]
