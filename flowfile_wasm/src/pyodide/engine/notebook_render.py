"""Render a flow as notebook cells: ``import flowfile as ff`` code, split at its statements.

A port of flowfile_core's FlowFrame export (``FlowGraphToFlowFrameConverter`` with
``placeholders=True, deterministic_names=True``) and ``notebook/render.py`` for the node types
the browser build has. The input is the flow in flowfile_core's dialect, exactly what the
download writes, so every emitter reads the settings core reads and writes the text core writes:
a notebook made here opens unchanged in the full app, and the other way around.

Linear runs fuse into one piped statement, one cell each; a node that cannot be written as code
is a ``ff.canvas_node`` placeholder cell that keeps its place in the flow. Nothing is executed
and no data is read.
"""

from __future__ import annotations

import ast
import datetime
import heapq
import io
import json
import re
import textwrap
import tokenize
from dataclasses import replace

from .notebook_formulas import strip_outer_parens, translate_advanced_filter, translate_to_ff_code
from .notebook_fusion import NodeEmission, render_pipeline

IMPORTS_CELL_ID = "imports"
FRAMEWORK = "ff"
AUTO_DATA_TYPE = "Auto"
STRING_CONCAT_DELIMITER = ","

NODE_TYPE_VAR_LABEL: dict[str, str] = {
    "read": "source",
    "manual_input": "source",
    "catalog_reader": "source",
    "filter": "filtered",
    "formula": "computed",
    "select": "selected",
    "dynamic_rename": "renamed",
    "sort": "ordered",
    "group_by": "grouped",
    "pivot": "pivoted",
    "unpivot": "unpivoted",
    "join": "joined",
    "cross_join": "joined",
    "union": "combined",
    "unique": "deduped",
    "record_id": "with_record_id",
    "record_count": "counted",
    "sample": "sampled",
    "polars_code": "transformed",
}

# The name the full app's palette gives each node type, used in a placeholder's comment.
NODE_TEMPLATE_NAMES: dict[str, str] = {
    "read": "Read data",
    "manual_input": "Manual input",
    "filter": "Filter data",
    "select": "Select data",
    "sort": "Sort data",
    "group_by": "Group by",
    "unique": "Drop duplicates",
    "formula": "Formula",
    "record_id": "Add record Id",
    "record_count": "Count records",
    "dynamic_rename": "Rename columns",
    "sample": "Take Sample",
    "polars_code": "Polars code",
    "pivot": "Pivot data",
    "unpivot": "Unpivot data",
    "join": "Join",
    "cross_join": "Cross join",
    "union": "Union data",
    "explore_data": "Explore data",
    "output": "Write data",
    "flow_input": "Flow Input",
    "flow_output": "Flow Output",
    "catalog_reader": "Read from Catalog",
    "catalog_writer": "Write to Catalog",
}

_SOURCE_TYPES = frozenset({"read", "manual_input", "flow_input", "catalog_reader"})
_TWO_INPUT_TYPES = frozenset({"join", "cross_join"})
_ANY_INPUT_TYPES = frozenset({"union", "polars_code"})
_NUMBER = re.compile(r"-?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)")
_DTYPE_MAP = {
    "String": "Utf8",
    "Integer": "Int64",
    "Double": "Float64",
    "Boolean": "Boolean",
    "Date": "Date",
    "Datetime": "Datetime",
    "Float32": "Float32",
    "Float64": "Float64",
    "Int32": "Int32",
    "Int64": "Int64",
    "Utf8": "Utf8",
}


def node_label(node_type: str, node_id: int) -> str:
    """The deterministic variable name of an unnamed node: ``<type_label>_<id>`` (``filtered_12``)."""
    label = NODE_TYPE_VAR_LABEL.get(node_type) or re.sub(r"\W", "_", node_type)
    return f"{label}_{node_id}"


def _py_str(value) -> str:
    """``value`` as a double-quoted Python string literal."""
    return json.dumps(value, ensure_ascii=False)


def _py_path(value) -> str:
    return json.dumps(str(value), ensure_ascii=False)


def _is_number(value: str | None) -> bool:
    """Whether a basic filter value is a plain decimal number, so it renders bare (``1-2`` stays text)."""
    return bool(value) and _NUMBER.fullmatch(value) is not None


def _temporal_base(field_dtype: str | None) -> str | None:
    """``"Date"`` or ``"Datetime"`` for a (possibly parametrized) temporal dtype string, else None."""
    base = (field_dtype or "").split("(", 1)[0]
    return base if base in ("Date", "Datetime") else None


def _temporal_literal(value: str, base: str) -> str | None:
    """A ``datetime.date(...)``/``datetime.datetime(...)`` source literal for an ISO value, or None."""
    text = value.strip().replace("T", " ", 1)
    try:
        if base == "Date":
            d = datetime.date.fromisoformat(text[:10])
            return f"datetime.date({d.year}, {d.month}, {d.day})"
        dt = datetime.datetime.fromisoformat(text)
        parts = [dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second]
        if dt.microsecond:
            parts.append(dt.microsecond)
        return f"datetime.datetime({', '.join(str(p) for p in parts)})"
    except ValueError:
        return None


def _with_description(code: str, description: str) -> str | None:
    """``description=`` added to the call the last statement assigns (after defs and imports), else None."""
    try:
        *head, last = ast.parse(code).body or [None]
    except SyntaxError:
        return None
    if not isinstance(last, ast.Assign) or not isinstance(last.value, ast.Call):
        return None
    if not all(isinstance(stmt, ast.FunctionDef | ast.Import | ast.ImportFrom) for stmt in head):
        return None
    call = last.value
    while isinstance(call.func, ast.Attribute) and call.func.attr == "agg" and isinstance(call.func.value, ast.Call):
        call = call.func.value
    if any(kw.arg == "description" for kw in call.keywords):
        return code
    lines = code.split("\n")
    line = lines[call.end_lineno - 1]
    col = len(line.encode("utf-8")[: call.end_col_offset - 1].decode("utf-8", errors="ignore"))
    before = code[: sum(len(prior) + 1 for prior in lines[: call.end_lineno - 1]) + col]
    rest, before = code[len(before) :], before.rstrip()
    separator = "" if before.endswith("(") else " " if before.endswith(",") else ", "
    return f"{before}{separator}description={json.dumps(description, ensure_ascii=False)}{rest}"


def _legacy_polars_code_body(code: str) -> tuple[list[str], str | None]:
    """Text heuristics for Polars code that does not parse, so a node with broken code still renders."""
    if "output_df" not in code:
        return [], code
    lines = [line for line in code.split("\n") if line.strip()]
    if "return" in code:
        return lines, None
    assignments = [line.strip() for line in lines if "=" in line]
    return lines, (assignments[-1].split("=")[0].strip() if assignments else None)


def _assigns_name(statements: list[ast.stmt], name: str) -> bool:
    return any(
        isinstance(node, ast.Name) and node.id == name and isinstance(node.ctx, ast.Store)
        for statement in statements
        for node in ast.walk(statement)
    )


def _polars_code_function_body(code: str) -> tuple[list[str], str | None]:
    """Split a Polars-code node's source into function body lines and the expression to return."""
    code = textwrap.dedent(code).strip()
    try:
        tree = ast.parse("def _f():\n" + textwrap.indent(code, "    "))
    except SyntaxError:
        return _legacy_polars_code_body(code)
    statements = tree.body[0].body
    lines = [line for line in code.split("\n") if line.strip()]
    if any(isinstance(statement, ast.Return) for statement in statements):
        return lines, None
    if _assigns_name(statements, "output_df"):
        return lines, "output_df"
    if len(statements) == 1 and isinstance(statements[0], ast.Expr):
        # Line 1 of the parsed source is the synthetic def; leading comment lines stay in the body.
        source_lines = code.split("\n")
        start = statements[0].lineno - 2
        return [line for line in source_lines[:start] if line.strip()], "\n".join(source_lines[start:])
    for statement in reversed(statements):
        if isinstance(statement, ast.Assign) and isinstance(statement.targets[0], ast.Name):
            return lines, statement.targets[0].id
    return lines, None


def _rename_tokens(body: list[str], rename: dict[str, str]) -> list[str] | None:
    """Rename NAME tokens only, leaving string and comment tokens untouched; None when not tokenizable."""
    source = "\n".join(body)
    try:
        tokens = list(tokenize.generate_tokens(io.StringIO(source).readline))
    except (tokenize.TokenError, IndentationError, SyntaxError, ValueError):
        return None
    physical = source.split("\n")
    edits: dict[int, list[tuple[int, int, str]]] = {}
    for tok in tokens:
        if tok.type != tokenize.NAME:
            continue
        new = rename.get(tok.string)
        if new is None:
            continue
        (srow, scol), (_erow, ecol) = tok.start, tok.end
        line = physical[srow - 1]
        # Skip ``param=`` (a keyword argument's name) while still renaming assignment targets.
        if ecol < len(line) and line[ecol] == "=":
            continue
        edits.setdefault(srow - 1, []).append((scol, ecol, new))
    for idx, line_edits in edits.items():
        line = physical[idx]
        for scol, ecol, new in sorted(line_edits, reverse=True):
            line = line[:scol] + new + line[ecol:]
        physical[idx] = line
    out: list[str] = []
    pos = 0
    for element in body:
        span = element.count("\n") + 1
        out.append("\n".join(physical[pos : pos + span]))
        pos += span
    return out


def _rename_regex(body: list[str], rename: dict[str, str]) -> list[str]:
    patterns = [(re.compile(r"\b" + re.escape(old) + r"\b(?!=)"), new) for old, new in rename.items()]
    out = []
    for line in body:
        for pattern, new in patterns:
            line = pattern.sub(new, line)
        out.append(line)
    return out


def _fill_names(cells: list[dict]) -> None:
    """``defines`` (top-level bindings) and ``uses`` (loaded names another cell defines) from each cell's AST."""
    loads: list[set[str]] = []
    for cell in cells:
        try:
            tree = ast.parse(cell["code"])
        except SyntaxError:
            loads.append(set())
            continue
        defined: list[str] = []
        for stmt in tree.body:
            if isinstance(stmt, ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef):
                found = [stmt.name]
            elif isinstance(stmt, ast.Import | ast.ImportFrom):
                found = [alias.asname or alias.name.split(".")[0] for alias in stmt.names]
            else:
                targets = getattr(stmt, "targets", None) or [getattr(stmt, "target", None)]
                found = [n.id for t in targets if t is not None for n in ast.walk(t) if isinstance(n, ast.Name)]
            defined += [name for name in found if name not in defined]
        cell["defines"] = defined
        loads.append({n.id for n in ast.walk(tree) if isinstance(n, ast.Name) and isinstance(n.ctx, ast.Load)})
    known = {name for cell in cells for name in cell["defines"]} | {"flow"}
    for cell, loaded in zip(cells, loads, strict=True):
        cell["uses"] = sorted((loaded - set(cell["defines"])) & known)


class _Unsupported(Exception):
    """A node this render has no code for; it becomes a placeholder carrying the reason."""


class _Node:
    """One node of the flow file, with the inputs the way core's graph holds them."""

    def __init__(self, raw: dict) -> None:
        self.id = int(raw["id"])
        self.type = str(raw.get("type"))
        self.settings: dict = raw.get("setting_input") or {}
        self.main_inputs = [int(i) for i in raw.get("input_ids") or []]
        right = raw.get("right_input_id")
        left = raw.get("left_input_id")
        self.right_input = None if right is None else int(right)
        self.left_input = None if left is None else int(left)
        self.description = str(raw.get("description") or self.settings.get("description") or "")
        self.reference = raw.get("node_reference") or self.settings.get("node_reference") or None

    def producer_ids(self) -> list[int]:
        ids = list(self.main_inputs)
        if self.left_input is not None:
            ids.append(self.left_input)
        if self.right_input is not None:
            ids.append(self.right_input)
        return ids


def _rows(select: object) -> list[dict]:
    """The rename rows of a join side, with core's defaults filled in."""
    # The flow file spells the list ``select``; the in-memory model calls it ``renames``.
    renames = (select.get("renames") or select.get("select")) if isinstance(select, dict) else select
    rows = []
    for row in renames or []:
        old = row.get("old_name")
        new = row.get("new_name")
        rows.append(
            {
                "old_name": old,
                "new_name": old if new in (None, "") else new,
                "keep": row.get("keep", True),
                "is_available": row.get("is_available", True),
                "join_key": False,
            }
        )
    return rows


class _JoinManager:
    """``transform_schema.JoinInputManager`` over plain dicts: which side renames and keeps what."""

    def __init__(self, join_input: dict) -> None:
        self.how = join_input.get("how") or "inner"
        self.mapping = [(m.get("left_col"), m.get("right_col")) for m in join_input.get("join_mapping") or []]
        self.left = _rows(join_input.get("left_select"))
        self.right = _rows(join_input.get("right_select"))
        self.set_join_keys()

    def set_join_keys(self) -> None:
        left_keys = {left for left, _ in self.mapping}
        right_keys = {right for _, right in self.mapping}
        for row in self.left:
            row["join_key"] = row["old_name"] in left_keys
        for row in self.right:
            row["join_key"] = row["old_name"] in right_keys

    def auto_rename(self) -> None:
        """Rename right-side columns until no kept name collides with a kept left name."""
        self.set_join_keys()
        while True:
            overlapping = {r["new_name"] for r in self.left if r["keep"]} & {
                r["new_name"] for r in self.right if r["keep"]
            }
            if not overlapping:
                return
            for row in self.right:
                if row["new_name"] in overlapping:
                    row["new_name"] = row["new_name"] + "_right"

    @staticmethod
    def rename_table(rows: list[dict]) -> dict[str, str]:
        return {r["old_name"]: r["new_name"] for r in rows if r["is_available"] and (r["keep"] or r["join_key"])}

    def names_for_table_rename(self) -> list[tuple[str, str]]:
        left, right = self.rename_table(self.left), self.rename_table(self.right)
        return [(left.get(lc, lc), right.get(rc, rc)) for lc, rc in self.mapping]

    @staticmethod
    def on_new_name(rows: list[dict], name: str) -> dict | None:
        return next((r for r in rows if r["new_name"] == name), None)

    @staticmethod
    def key_rows(rows: list[dict]) -> list[dict]:
        return [r for r in rows if r["join_key"]]


class NotebookRenderer:
    """The FlowFrame export of one flow, with a placeholder for every node it cannot write as code."""

    def __init__(self, flow: dict, schemas: dict | None = None, locked: dict | None = None) -> None:
        self.nodes: dict[int, _Node] = {}
        for raw in flow.get("nodes") or []:
            node = _Node(raw)
            self.nodes[node.id] = node
        self.schemas = {int(key): value or [] for key, value in (schemas or {}).items()}
        self.locked = {int(key): str(value) for key, value in (locked or {}).items()}
        self.code_lines: list[str] = []
        self.imports: set[str] = {"import flowfile as ff"}
        self.node_var_mapping: dict[int, str] = {}
        self.unsupported: list[tuple[int, str, str]] = []
        self.warnings: list[str] = []
        self._node_spans: list[tuple[_Node, str, int, int]] = []
        self._passthrough: dict[int, int] = {}
        self._placeholder_reasons: dict[int, str] = {}
        self._blocked: set[int] = set()

    def render(self) -> dict:
        for node in self._topological_order():
            self._generate_node_code(node)
        fused = self._fuse()
        imports = ["import flowfile as ff", *(line for line in sorted(self.imports) if line != "import flowfile as ff")]
        cells = [self._cell(IMPORTS_CELL_ID, [], "imports", "\n".join(imports))]
        for em in fused:
            ids = em.node_ids or [em.node_id]
            cell = self._cell(f"cell-{ids[0]}", ids, "node", em.code)
            if em.placeholder_reason:
                cell["status"], cell["reason"] = "placeholder", em.placeholder_reason
            cells.append(cell)
        _fill_names(cells)
        return {
            "cells": cells,
            "warnings": self.warnings,
            "var_by_node": {str(em.node_id): em.var_name for em in fused},
        }

    @staticmethod
    def _cell(cell_id: str, node_ids: list[int], kind: str, code: str) -> dict:
        return {
            "cell_id": cell_id,
            "node_ids": node_ids,
            "kind": kind,
            "code": code,
            "defines": [],
            "uses": [],
            "status": "code",
            "reason": None,
        }

    def _topological_order(self) -> list[_Node]:
        """Kahn's algorithm with a min-heap on node id; nodes left in a cycle follow in id order."""
        leads_to: dict[int, list[int]] = {node_id: [] for node_id in self.nodes}
        in_degree = dict.fromkeys(self.nodes, 0)
        for node in self.nodes.values():
            for producer in node.producer_ids():
                if producer in leads_to:
                    leads_to[producer].append(node.id)
                    in_degree[node.id] += 1
        heap = [node_id for node_id, degree in in_degree.items() if degree == 0]
        heapq.heapify(heap)
        order: list[_Node] = []
        while heap:
            node = self.nodes[heapq.heappop(heap)]
            order.append(node)
            for downstream in leads_to[node.id]:
                in_degree[downstream] -= 1
                if in_degree[downstream] == 0:
                    heapq.heappush(heap, downstream)
        seen = {node.id for node in order}
        order.extend(self.nodes[node_id] for node_id in sorted(set(self.nodes) - seen))
        return order

    def _is_correct(self, node: _Node) -> bool:
        """Whether the node's inputs are all connected, as its node type needs them."""
        connected = len(node.producer_ids())
        if node.type in _SOURCE_TYPES:
            return connected == 0
        if node.type in _ANY_INPUT_TYPES:
            return connected > 0 or node.type == "polars_code"
        if node.type in _TWO_INPUT_TYPES:
            return connected == 2
        return connected == 1

    def _static_placeholder_reason(self, node: _Node) -> str | None:
        """Why ``node`` is a placeholder before its emitter runs; an unconnected node blocks its downstream."""
        if node.id in self.locked:
            return self.locked[node.id]
        upstream = [pid for pid in node.producer_ids() if pid in self._blocked]
        if not self._is_correct(node):
            reason = "inputs not fully connected"
        elif upstream:
            reason = f"downstream of node {min(upstream)}, which is not editable as code"
        else:
            if node.type == "explore_data":
                return "explore data is interactive only"
            return None
        self._blocked.add(node.id)
        return reason

    def _generate_node_code(self, node: _Node) -> None:
        reason = self._static_placeholder_reason(node)
        if reason is None:
            mark = (len(self.code_lines), len(self.unsupported), len(self._node_spans))
            saved_imports = set(self.imports)
            try:
                self._emit_node(node)
            except Exception as exc:
                reason = f"could not render: {exc}"
            else:
                if len(self.unsupported) > mark[1]:
                    reason = self.unsupported[mark[1]][2]
                elif len(self._node_spans) == mark[2]:
                    reason = "renders no code"
            if reason is None:
                return self._describe(node)
            del self.code_lines[mark[0] :], self.unsupported[mark[1] :], self._node_spans[mark[2] :]
            self.imports = saved_imports
            self._passthrough.pop(node.id, None)
        var = node.reference or f"df_{node.id}"
        args = [str(node.id), *self._get_input_vars(node).values()]
        self.node_var_mapping[node.id] = var
        comment = " ".join(f"{NODE_TEMPLATE_NAMES.get(node.type) or node.type}: {reason}".split())
        self._placeholder_reasons[node.id] = reason
        self._node_spans.append((node, var, len(self.code_lines), len(self.code_lines) + 1))
        self._add_code(f"{var} = ff.canvas_node({', '.join(args)})  # {comment}")

    def _emit_node(self, node: _Node) -> None:
        var_name = node.reference or f"df_{node.id}"
        self.node_var_mapping[node.id] = var_name
        input_vars = self._get_input_vars(node)
        start = len(self.code_lines)
        handler = getattr(self, f"_handle_{node.type}", None)
        if handler is None:
            self.unsupported.append((node.id, node.type, f"No code generator implemented for node type '{node.type}'"))
            return
        handler(node, var_name, input_vars)
        end = len(self.code_lines)
        if end == start:
            if len(node.main_inputs) == 1:
                self._passthrough[node.id] = node.main_inputs[0]
            return
        self._node_spans.append((node, self.node_var_mapping[node.id], start, end))

    def _get_input_vars(self, node: _Node) -> dict[str, str]:
        input_vars: dict[str, str] = {}
        mains = node.main_inputs
        if len(mains) == 1:
            input_vars["main"] = self.node_var_mapping.get(mains[0], "df")
        else:
            for index, producer in enumerate(mains):
                input_vars[f"main_{index}"] = self.node_var_mapping.get(producer, f"df_{index}")
        if node.right_input is not None:
            input_vars["right"] = self.node_var_mapping.get(node.right_input, "df_right")
        if node.left_input is not None:
            input_vars["left"] = self.node_var_mapping.get(node.left_input, "df_left")
        return input_vars

    def _describe(self, node: _Node) -> None:
        """Add ``description=`` to the call the node's span assigns, when it has one to carry."""
        description = node.description
        if not description or not self._node_spans or self._node_spans[-1][0] is not node:
            return
        _, var, start, end = self._node_spans[-1]
        described = _with_description("\n".join(self.code_lines[start:end]).rstrip(), description)
        if described is None:
            self.warnings.append(f"Node {node.id}: its description has no frame call to attach to")
            return
        self.code_lines[start:end] = [*described.split("\n"), ""]
        self._node_spans[-1] = (node, var, start, len(self.code_lines))

    def _resolve_producer(self, node_id: int) -> int:
        seen: set[int] = set()
        while node_id in self._passthrough and node_id not in seen:
            seen.add(node_id)
            node_id = self._passthrough[node_id]
        return node_id

    def _fuse(self) -> list[NodeEmission]:
        """Fuse linear single-use chains and give the surviving statements their names."""
        emissions: list[NodeEmission] = []
        node_by_id: dict[int, _Node] = {}
        for node, effective_var, start, end in self._node_spans:
            lines = self.code_lines[start:end]
            while lines and lines[-1] == "":
                lines = lines[:-1]
            if not lines:
                continue
            resolved = {self._resolve_producer(pid) for pid in node.producer_ids()}
            num_inputs = len(resolved)
            main_producer_id = next(iter(resolved)) if num_inputs == 1 else None
            emissions.append(
                NodeEmission(
                    node.id, effective_var, lines, main_producer_id, num_inputs,
                    bool(node.description), bool(node.reference), self._placeholder_reasons.get(node.id),
                )
            )  # fmt: skip
            node_by_id[node.id] = node

        consumers: dict[int, list[int]] = {em.node_id: [] for em in emissions}
        for em in emissions:
            for pid in {self._resolve_producer(p) for p in node_by_id[em.node_id].producer_ids()}:
                if pid in consumers:
                    consumers[pid].append(em.node_id)

        fused = render_pipeline(emissions, consumers)
        survivors = {em.node_id for em in fused}
        rename: dict[str, str] = {}
        for em in emissions:
            if em.node_id not in survivors or em.pinned:
                continue
            prefix, label = f"df_{em.node_id}", node_label(node_by_id[em.node_id].type, em.node_id)
            if em.var_name == prefix or em.var_name.startswith(f"{prefix}_"):
                rename[em.var_name] = label + em.var_name[len(prefix) :]
        body = [line for em in fused for line in em.lines]
        if rename:
            body = _rename_tokens(body, rename) or _rename_regex(body, rename)
        out: list[NodeEmission] = []
        for em in fused:
            lines, body = body[: len(em.lines)], body[len(em.lines) :]
            out.append(replace(em, lines=lines, var_name=rename.get(em.var_name, em.var_name)))
        return out

    def _add_code(self, line: str) -> None:
        self.code_lines.append(line)

    def _column_dtype(self, node_id: int, field: str) -> str | None:
        for column in self.schemas.get(node_id) or []:
            if column.get("name") == field:
                return column.get("data_type")
        return None

    def _input_column_names(self, node: _Node) -> list[str] | None:
        if len(node.main_inputs) != 1:
            return None
        schema = self.schemas.get(node.main_inputs[0])
        return [column.get("name") for column in schema] if schema else None

    def _translate_to_ff_code(self, formula: str) -> str | None:
        code = translate_to_ff_code(formula)
        if code:
            for module in ("datetime", "hashlib"):
                if re.search(rf"\b{module}\.", code):
                    self.imports.add(f"import {module}")
            if re.search(r"\bpl\.", code):
                self.imports.add("import polars as pl")
        return code

    # Sources

    def _handle_manual_input(self, node: _Node, var_name: str, input_vars: dict[str, str]) -> None:
        raw = node.settings.get("raw_data_format") or {}
        dumped = {
            "columns": [
                {"name": column.get("name"), "data_type": column.get("data_type", "String")}
                for column in raw.get("columns") or []
            ],
            "data": raw.get("data") or [],
        }
        self._add_code(f"{var_name} = ff.from_raw_data({dumped})")
        self._add_code("")

    def _handle_read(self, node: _Node, var_name: str, input_vars: dict[str, str]) -> None:
        received = node.settings.get("received_file") or {}
        table = received.get("table_settings") or {}
        file_type = received.get("file_type") or table.get("file_type") or "csv"
        path = _py_path(received.get("abs_file_path") or received.get("path"))
        if received.get("scan_mode") == "directory":
            raise _Unsupported("a folder of files cannot be read in the browser")
        if file_type == "csv":
            encoding = str(table.get("encoding", "utf-8"))
            if encoding.lower() not in ("utf-8", "utf8"):
                raise _Unsupported(f"the {encoding} encoding")
            self._add_code(f"{var_name} = ff.scan_csv(")
            self._add_code(f"    {path},")
            for line in self._csv_scan_kwarg_lines(table):
                self._add_code(line)
            self._add_code(")")
        elif file_type == "parquet":
            self._add_code(f"{var_name} = ff.scan_parquet({path})")
        elif file_type in ("xlsx", "excel"):
            self._add_code(f"{var_name} = ff.read_excel(")
            self._add_code(f"    {path},")
            if table.get("sheet_name"):
                self._add_code(f'    sheet_name="{table["sheet_name"]}",')
            self._add_code(")")
        else:
            self.unsupported.append((node.id, "read", f"No code generation for file type '{file_type}'."))
            return
        self._add_code("")

    @staticmethod
    def _csv_scan_kwarg_lines(table: dict) -> list[str]:
        """The stored UTF-8 variant and each option that differs from ``scan_csv``'s default."""
        encoding = "utf8-lossy" if "LOSSY" in str(table.get("encoding") or "").upper() else "utf8"
        lines = [
            f'    separator="{table.get("delimiter", ",")}",',
            f"    has_header={table.get('has_headers', True)},",
            f"    ignore_errors={table.get('ignore_errors', False)},",
            f'    encoding="{encoding}",',
            f"    skip_rows={table.get('starting_from_line', 0)},",
        ]
        infer_schema_length = table.get("infer_schema_length", 10_000)
        if infer_schema_length != 100:
            lines.append(f"    infer_schema_length={infer_schema_length},")
        if not table.get("infer_schema", True):
            lines.append("    infer_schema=False,")
        quote_char = table.get("quote_char", '"')
        if quote_char != '"':
            lines.append(f"    quote_char={_py_str(quote_char) if quote_char else None},")
        if table.get("truncate_ragged_lines", False):
            lines.append("    truncate_ragged_lines=True,")
        return lines

    # Single-input transforms

    def _handle_filter(self, node: _Node, var_name: str, input_vars: dict[str, str]) -> None:
        """Filter nodes; an advanced filter that is one basic comparison renders as the basic one."""
        input_df = input_vars.get("main", "df")
        settings = node.settings
        if settings.get("split_mode"):
            raise _Unsupported("two-output filters are not available in the browser")
        filter_input = settings.get("filter_input") or {}
        advanced = filter_input.get("advanced_filter") or ""
        if filter_input.get("mode") == "advanced":
            basic = translate_advanced_filter(strip_outer_parens(advanced))
            if basic is not None:
                filter_input = basic

        if filter_input.get("mode") == "advanced":
            ff_code = self._translate_to_ff_code(advanced)
            if ff_code:
                self._add_code(f"{var_name} = {input_df}.filter({ff_code})")
            else:
                self._add_code(f"{var_name} = {input_df}.filter(flowfile_formula={advanced!r})")
        else:
            basic_filter = filter_input.get("basic_filter") or {}
            if basic_filter.get("field"):
                field_dtype = self._column_dtype(node.id, basic_filter["field"])
                self._add_code(f"{var_name} = {input_df}.filter({self._basic_filter_expr(basic_filter, field_dtype)})")
            else:
                self._add_code(f"{var_name} = {input_df}  # No filter applied")
        self._add_code("")

    def _basic_filter_expr(self, basic: dict, field_dtype: str | None) -> str:
        """The predicate a basic filter spells; a value renders by the column's dtype."""
        col = f"{FRAMEWORK}.col({_py_str(basic.get('field'))})"
        value = "" if basic.get("value") is None else str(basic.get("value"))
        value2 = basic.get("value2")
        is_boolean = field_dtype == "Boolean"
        temporal = _temporal_base(field_dtype)

        def render(v: str) -> str:
            if is_boolean:
                return "True" if v.strip().lower() in ("true", "1") else "False"
            if temporal:
                literal = _temporal_literal(v, temporal)
                if literal:
                    self.imports.add("import datetime")
                    return literal
            if _is_number(v):
                return v
            return _py_str(v)

        def members() -> str:
            values = [v.strip() for v in value.split(",")]
            if temporal:
                return ", ".join(render(v) for v in values)
            if all(_is_number(v) for v in values):
                return ", ".join(values)
            return ", ".join(_py_str(v) for v in values)

        operator = _OPERATOR_ALIASES.get(str(basic.get("operator", "equals")), str(basic.get("operator", "equals")))
        comparisons = {
            "equals": "==",
            "not_equals": "!=",
            "greater_than": ">",
            "greater_than_or_equals": ">=",
            "less_than": "<",
            "less_than_or_equals": "<=",
        }
        if operator in comparisons:
            return f"{col} {comparisons[operator]} {render(value)}"
        if operator == "contains":
            return f"{col}.str.contains({_py_str(value)})"
        if operator == "not_contains":
            return f"{col}.str.contains({_py_str(value)}).not_()"
        if operator == "starts_with":
            return f"{col}.str.starts_with({_py_str(value)})"
        if operator == "ends_with":
            return f"{col}.str.ends_with({_py_str(value)})"
        if operator == "is_null":
            return f"{col}.is_null()"
        if operator == "is_not_null":
            return f"{col}.is_not_null()"
        if operator == "in":
            return f"{col}.is_in([{members()}])"
        if operator == "not_in":
            return f"{col}.is_in([{members()}]).not_()"
        if operator == "between":
            if value2 is None:
                return f"{col}  # BETWEEN requires two values"
            value2 = str(value2)
            if temporal:
                return f"({col} >= {render(value)}) & ({col} <= {render(value2)})"
            if _is_number(value) and _is_number(value2):
                return f"({col} >= {value}) & ({col} <= {value2})"
            return f"({col} >= {_py_str(value)}) & ({col} <= {_py_str(value2)})"
        return col

    def _handle_select(self, node: _Node, var_name: str, input_vars: dict[str, str]) -> None:
        input_df = input_vars.get("main", "df")
        settings = node.settings
        rows = [_select_row(row) for row in settings.get("select_input") or []]
        keep_missing = settings.get("keep_missing", True)
        drop_cols = self._drop_shaped_select(node, rows, keep_missing)
        if drop_cols:
            self._add_code(f"{var_name} = {input_df}.drop([{', '.join(_py_str(c) for c in drop_cols)}])")
            self._add_code("")
            return
        unlisted: list[str] = []
        if keep_missing and rows:
            input_names = self._input_column_names(node)
            if input_names is None:
                self._emit_select_chain(node, rows, var_name, input_df)
                return
            listed = {row["old_name"] for row in rows}
            unlisted = [name for name in input_names if name not in listed]
        select_exprs = []
        for row in rows:
            if not (row["keep"] and row["is_available"]):
                continue
            expr = f"{FRAMEWORK}.col({_py_str(row['old_name'])})"
            if row["old_name"] != row["new_name"]:
                expr = f"{expr}.alias({_py_str(row['new_name'])})"
            if (row["data_type_change"] or row["is_altered"]) and row["data_type"]:
                expr = f"{expr}.cast({_polars_dtype(row['data_type'])})"
            select_exprs.append(expr)
        select_exprs += [f"{FRAMEWORK}.col({_py_str(name)})" for name in unlisted]
        if not select_exprs:
            self.node_var_mapping[node.id] = input_df
            return
        self._add_code(f"{var_name} = {input_df}.select([")
        for expr in select_exprs:
            self._add_code(f"    {expr},")
        self._add_code("])")
        self._add_code("")

    def _drop_shaped_select(self, node: _Node, rows: list[dict], keep_missing: bool) -> list[str] | None:
        """Columns to drop when a select only unchecks columns, so it renders as ``.drop([...])``; else None."""
        if not keep_missing or node.settings.get("sorted_by") not in (None, "none"):
            return None
        kept: list[str] = []
        dropped: list[str] = []
        for row in sorted(rows, key=lambda r: 0 if r["position"] is None else r["position"]):
            if row["is_altered"] or row["data_type_change"] or row["new_name"] not in (None, row["old_name"]):
                return None
            (kept if row["keep"] else dropped).append(row["old_name"])
        if not dropped:
            return None
        input_names = self._input_column_names(node)
        if input_names is None:
            return None
        dropped = [c for c in dropped if c in input_names]
        kept = [c for c in kept if c in input_names]
        listed = set(kept) | set(dropped)
        if kept + [c for c in input_names if c not in listed] != [c for c in input_names if c not in dropped]:
            return None
        return dropped or None

    def _emit_select_chain(self, node: _Node, rows: list[dict], var_name: str, input_df: str) -> None:
        """A ``keep_missing`` select over an unknown input as ``.drop()``, ``.rename()`` and a cast, in that order."""
        rows = [row for row in rows if row["is_available"]]
        renames = {
            row["old_name"]: row["new_name"] for row in rows if row["keep"] and row["new_name"] != row["old_name"]
        }
        drops = [row["old_name"] for row in rows if not row["keep"]]
        casts = [
            f"{FRAMEWORK}.col({_py_str(row['new_name'])}).cast({_polars_dtype(row['data_type'])})"
            for row in rows
            if row["keep"] and (row["data_type_change"] or row["is_altered"]) and row["data_type"]
        ]
        chain = ""
        if drops:
            chain += f".drop([{', '.join(_py_str(name) for name in drops)}])"
        if renames:
            chain += ".rename({" + ", ".join(f"{_py_str(k)}: {_py_str(v)}" for k, v in renames.items()) + "})"
        if casts:
            chain += f".with_columns([{', '.join(casts)}])"
        if not chain:
            self.node_var_mapping[node.id] = input_df
            return
        self._add_code(f"{var_name} = {input_df}{chain}")
        self._add_code("")

    def _handle_formula(self, node: _Node, var_name: str, input_vars: dict[str, str]) -> None:
        """One chained ``with_columns`` per entry, in order: a later entry may read an earlier one's column."""
        input_df = input_vars.get("main", "df")
        entries = [entry for entry in _formula_entries(node.settings) if (entry.get("function") or "").strip()]
        if not entries:
            self._add_code(f"{var_name} = {input_df}")
        elif len(entries) == 1:
            self._add_code(f"{var_name} = {input_df}{self._formula_entry_call(entries[0])}")
        else:
            self._add_code(f"{var_name} = ({input_df}")
            for entry in entries:
                self._add_code(f"    {self._formula_entry_call(entry)}")
            self._add_code(")")
        self._add_code("")

    def _formula_entry_call(self, entry: dict) -> str:
        """The ``.with_columns(...)`` call one formula entry emits: native code when untyped and translatable."""
        field = entry.get("field") or {}
        formula, name, data_type = entry.get("function"), field.get("name"), field.get("data_type")
        if data_type in (None, AUTO_DATA_TYPE):
            ff_code = self._translate_to_ff_code(formula)
            if ff_code:
                return f'.with_columns(({ff_code}).alias("{name}"))'
        return (
            f".with_columns(flowfile_formulas=[{formula!r}], output_column_names=[{name!r}], "
            f"output_column_datatypes=[{data_type!r}])"
        )

    def _handle_group_by(self, node: _Node, var_name: str, input_vars: dict[str, str]) -> None:
        input_df = input_vars.get("main", "df")
        group_cols, group_items, agg_exprs = [], [], []
        has_renamed_key = False
        for agg_col in (node.settings.get("groupby_input") or {}).get("agg_cols") or []:
            old_name, new_name, agg = agg_col.get("old_name"), agg_col.get("new_name"), agg_col.get("agg")
            if agg == "groupby":
                group_cols.append(old_name)
                if new_name and new_name != old_name:
                    has_renamed_key = True
                    group_items.append(f"{FRAMEWORK}.col({_py_str(old_name)}).alias({_py_str(new_name)})")
                else:
                    group_items.append(_py_str(old_name))
            else:
                agg_exprs.append(
                    f"{FRAMEWORK}.col({_py_str(old_name)}).{_agg_function(agg)}.alias({_py_str(new_name)})"
                )
        if has_renamed_key:
            self._add_code(f"{var_name} = {input_df}.group_by([{', '.join(group_items)}]).agg([")
        else:
            self._add_code(f"{var_name} = {input_df}.group_by({group_cols}).agg([")
        for expr in agg_exprs:
            self._add_code(f"    {expr},")
        self._add_code("])")
        self._add_code("")

    def _handle_sort(self, node: _Node, var_name: str, input_vars: dict[str, str]) -> None:
        input_df = input_vars.get("main", "df")
        sort_input = node.settings.get("sort_input") or []
        columns = [_py_str(entry.get("column")) for entry in sort_input]
        descending = [entry.get("how") in ("desc", "descending") for entry in sort_input]
        self._add_code(f"{var_name} = {input_df}.sort([{', '.join(columns)}], descending={descending})")
        self._add_code("")

    def _handle_unique(self, node: _Node, var_name: str, input_vars: dict[str, str]) -> None:
        input_df = input_vars.get("main", "df")
        unique_input = node.settings.get("unique_input") or {}
        columns, strategy = unique_input.get("columns"), unique_input.get("strategy", "any")
        if columns:
            self._add_code(f"{var_name} = {input_df}.unique(subset={columns}, keep='{strategy}')")
        else:
            self._add_code(f"{var_name} = {input_df}.unique(keep='{strategy}')")
        self._add_code("")

    def _handle_record_id(self, node: _Node, var_name: str, input_vars: dict[str, str]) -> None:
        record = node.settings.get("record_id_input") or {}
        args = [_py_str(record.get("output_column_name", "record_id")), f"offset={record.get('offset', 1)}"]
        if record.get("group_by") and record.get("group_by_columns"):
            args.append(f"group_by={[str(column) for column in record['group_by_columns']]}")
        self._add_code(f"{var_name} = {input_vars.get('main', 'df')}.with_row_index({', '.join(args)})")
        self._add_code("")

    def _handle_record_count(self, node: _Node, var_name: str, input_vars: dict[str, str]) -> None:
        input_df = input_vars.get("main", "df")
        self._add_code(f"{var_name} = {input_df}.select({FRAMEWORK}.len().alias('number_of_records'))")

    def _handle_sample(self, node: _Node, var_name: str, input_vars: dict[str, str]) -> None:
        input_df = input_vars.get("main", "df")
        settings = node.settings
        method = settings.get("sample_method", "first")
        if method == "first":
            self._add_code(f"{var_name} = {input_df}.head({settings.get('sample_size', 1000)})")
            self._add_code("")
            return
        raise _Unsupported("random samples are not available in the browser")

    def _handle_dynamic_rename(self, node: _Node, var_name: str, input_vars: dict[str, str]) -> None:
        input_df = input_vars.get("main", "df")
        s = node.settings.get("dynamic_rename_input") or {}
        args = [f"mode={s.get('rename_mode', 'prefix')!r}"]
        if s.get("prefix"):
            args.append(f"prefix={s['prefix']!r}")
        if s.get("suffix"):
            args.append(f"suffix={s['suffix']!r}")
        if s.get("formula"):
            args.append(f"formula={s['formula']!r}")
        if s.get("selection_mode", "all") == "list":
            args.append(f"columns={list(s.get('selected_columns') or [])!r}")
        elif s.get("selection_mode") == "data_type" and s.get("selected_data_type") is not None:
            args.append(f"data_type={s['selected_data_type']!r}")
        self._add_code(f"{var_name} = {input_df}.dynamic_rename({', '.join(args)})")
        self._add_code("")

    def _handle_pivot(self, node: _Node, var_name: str, input_vars: dict[str, str]) -> None:
        input_df = input_vars.get("main", "df")
        pivot = node.settings.get("pivot_input") or {}
        aggregations = pivot.get("aggregations") or []
        agg_func = aggregations[0] if aggregations else "first"
        index_columns = pivot.get("index_columns") or []
        if not index_columns:
            self._add_code(f"{var_name} = ({input_df}")
            self._add_code(f'    .with_columns({FRAMEWORK}.lit(1).alias("_temp_index_"))')
            self._add_code("    .pivot(")
            self._add_code(f'        values="{pivot.get("value_col")}",')
            self._add_code('        index=["_temp_index_"],')
            self._add_code(f'        on="{pivot.get("pivot_column")}",')
            self._add_code(f'        aggregate_function="{agg_func}"')
            self._add_code("    )")
            self._add_code('    .drop("_temp_index_")')
            self._add_code(")")
        else:
            self._add_code(f"{var_name} = {input_df}.pivot(")
            self._add_code(f"    values='{pivot.get('value_col')}',")
            self._add_code(f"    index={index_columns},")
            self._add_code(f"    on='{pivot.get('pivot_column')}',")
            self._add_code(f"    aggregate_function='{agg_func}'")
            self._add_code(")")
        self._add_code("")

    def _handle_unpivot(self, node: _Node, var_name: str, input_vars: dict[str, str]) -> None:
        input_df = input_vars.get("main", "df")
        unpivot = node.settings.get("unpivot_input") or {}
        self._add_code(f"{var_name} = {input_df}.unpivot(")
        if unpivot.get("index_columns"):
            self._add_code(f"    index={unpivot['index_columns']},")
        if unpivot.get("value_columns"):
            self._add_code(f"    on={unpivot['value_columns']},")
        self._add_code("    variable_name='variable',")
        self._add_code("    value_name='value'")
        self._add_code(")")
        self._add_code("")

    def _handle_polars_code(self, node: _Node, var_name: str, input_vars: dict[str, str]) -> None:
        """``<input>.polars_code(fn, *others)`` (``ff.polars_code(fn)`` without input) over the stored code verbatim."""
        inputs = [input_vars[key] for key in sorted(input_vars) if key.startswith("main")]
        code = textwrap.dedent((node.settings.get("polars_code_input") or {}).get("polars_code") or "").strip()
        names = [f"input_df_{i}" for i in range(1, len(inputs) + 1)] if len(inputs) > 1 else ["input_df"][: len(inputs)]
        function = f"_polars_code_{node.id}"
        if re.search(r"\bpl\.", code):
            self.imports.add("import polars as pl")
        body, returned = _polars_code_function_body(code)
        if returned not in (None, "output_df") and re.search(rf"^{re.escape(returned)}\s*=[^=]", "\n".join(body), re.M):
            returned = None
        self._add_code(f"def {function}({', '.join(f'{name}: ff.FlowFrame' for name in names)}):")
        for line in [*body, *([f"return {returned}"] if returned else [] if body else ["pass"])]:
            self._add_code(f"    {line}")
        self._add_code("")
        self._add_code("")
        call = f"{inputs[0]}.polars_code({', '.join([function, *inputs[1:]])})" if inputs else None
        self._add_code(f"{var_name} = {call or f'ff.polars_code({function})'}")
        self._add_code("")

    def _handle_output(self, node: _Node, var_name: str, input_vars: dict[str, str]) -> None:
        output = node.settings.get("output_settings") or {}
        source = input_vars.get("main", "df")
        table = output.get("table_settings") or {}
        file_type = output.get("file_type")
        abs_path = output.get("abs_file_path")
        if not abs_path:
            directory, name = output.get("directory") or "", output.get("name") or ""
            abs_path = f"{directory.rstrip('/')}/{name}" if directory else name
        path = _py_path(abs_path)
        if file_type == "csv":
            kwargs = [f"separator={_py_str(table.get('delimiter', ','))}"]
            if table.get("encoding", "utf-8") != "utf-8":
                kwargs.append(f"encoding={_py_str(table['encoding'])}")
            self._add_code(f"{var_name} = {source}.write_csv({path}, {', '.join(kwargs)})")
        elif file_type == "parquet":
            compression = table.get("compression")
            extra = "" if compression in (None, "zstd") else f", compression={_py_str(compression)}"
            self._add_code(f"{var_name} = {source}.write_parquet({path}{extra})")
        elif file_type == "excel":
            write_mode = output.get("write_mode") or "overwrite"
            sheet = table.get("sheet_name", "Sheet1")
            self._add_code(f"{source}.write_excel(")
            self._add_code(f"    {path},")
            if write_mode in ("overwrite", "new file"):
                self._add_code(f'    worksheet="{sheet}"')
            else:
                self._add_code(f'    worksheet="{sheet}",')
                self._add_code(f'    write_mode="{write_mode}"')
            self._add_code(")")
        else:
            return
        self._add_code("")

    # Multi-input nodes

    def _handle_union(self, node: _Node, var_name: str, input_vars: dict[str, str]) -> None:
        dfs = (
            [input_vars["main"]] if "main" in input_vars else [v for k, v in input_vars.items() if k.startswith("main")]
        )
        mode = (node.settings.get("union_input") or {}).get("mode", "relaxed")
        how = "diagonal_relaxed" if mode == "relaxed" else "diagonal"
        self._add_code(f"{var_name} = {FRAMEWORK}.concat([")
        for df in dfs:
            self._add_code(f"    {df},")
        self._add_code(f"], how='{how}')")
        self._add_code("")

    def _handle_cross_join(self, node: _Node, var_name: str, input_vars: dict[str, str]) -> None:
        left_df = input_vars.get("main", input_vars.get("main_0", "df_left"))
        right_df = input_vars.get("right", input_vars.get("main_1", "df_right"))
        self._add_code(f"{var_name} = {left_df}.join({right_df}, how='cross')")
        self._add_code("")

    def _handle_join(self, node: _Node, var_name: str, input_vars: dict[str, str]) -> None:
        left_df = input_vars.get("main", input_vars.get("main_0", "df_left"))
        right_df = input_vars.get("right", input_vars.get("main_1", "df_right"))
        if left_df == right_df:
            right_df = "df_right"
            self._add_code(f"{right_df} = {left_df}")
        join_input = node.settings.get("join_input") or {}
        how = join_input.get("how") or "inner"
        if how in ("semi", "anti"):
            mapping = join_input.get("join_mapping") or []
            left_on = [m.get("left_col") for m in mapping]
            right_on = [m.get("right_col") for m in mapping]
            right_keys = list(dict.fromkeys(right_on))
            self._add_code(f"{var_name} = {left_df}.join(")
            self._add_code(f"        {right_df}.select([{', '.join(_py_str(k) for k in right_keys)}]),")
            self._add_code(f"        left_on={left_on},")
            self._add_code(f"        right_on={right_on},")
            self._add_code(f'        how="{how}"')
            self._add_code("    )")
            return
        self._handle_standard_join(node, join_input, var_name, left_df, right_df)

    def _handle_standard_join(self, node: _Node, join_input: dict, var_name: str, left_df: str, right_df: str) -> None:
        manager = _JoinManager(join_input)
        manager.auto_rename()
        how = manager.how
        renamed = manager.names_for_table_rename()
        left_on = [left for left, _ in renamed]
        right_on = [right for _, right in renamed] if how in ("outer", "right") else [rc for _, rc in manager.mapping]

        suffix = self._join_suffix(manager)
        if suffix is not None:
            kwargs = [f"left_on={left_on}", f"right_on={right_on}", f'how="{how}"']
            if suffix != "_right":
                kwargs.append(f"suffix={_py_str(suffix)}")
            if any(row["keep"] for row in manager.right if row["join_key"]):
                kwargs.append("keep_right_keys=True")
            self._add_code(f"{var_name} = {left_df}.join(")
            self._add_code(f"        {right_df},")
            for index, kwarg in enumerate(kwargs):
                self._add_code(f"        {kwarg}{',' if index < len(kwargs) - 1 else ''}")
            self._add_code("    )")
            return

        left_tmp, right_tmp = f"_join_{node.id}_left", f"_join_{node.id}_right"
        right_renames = {
            row["old_name"]: row["new_name"]
            for row in manager.right
            if row["old_name"] != row["new_name"]
            and (row["keep"] or row["join_key"])
            and (not row["join_key"] or how in ("outer", "right"))
        }
        left_renames = {
            row["old_name"]: row["new_name"]
            for row in manager.left
            if row["old_name"] != row["new_name"] and (row["keep"] or row["join_key"])
        }
        left_drops = [row["old_name"] for row in manager.left if not row["keep"] and not row["join_key"]]
        right_drops = [row["old_name"] for row in manager.right if not row["keep"] and not row["join_key"]]
        if right_renames:
            self._add_code(f"{right_tmp} = {right_df}.rename({right_renames})")
            right_df = right_tmp
        if left_renames:
            self._add_code(f"{left_tmp} = {left_df}.rename({left_renames})")
            left_df = left_tmp
        if left_drops:
            self._add_code(f"{left_tmp} = {left_df}.drop({left_drops})")
            left_df = left_tmp
        if right_drops:
            self._add_code(f"{right_tmp} = {right_df}.drop({right_drops})")
            right_df = right_tmp

        left_keys, right_keys = manager.key_rows(manager.left), manager.key_rows(manager.right)
        reverse_action: dict | None = None
        after_join_drop_cols: list[str] = []
        if how in ("left", "inner"):
            duplicated = [
                f"{FRAMEWORK}.col({_py_str(row['old_name'])}).alias({_py_str('__DROP__' + row['new_name'] + '__DROP__')})"
                for row in right_keys
                if row["keep"]
            ]
            reverse_action = {
                f"__DROP__{row['new_name']}__DROP__": row["new_name"] for row in right_keys if row["keep"]
            }
            if duplicated:
                self._add_code(f"{right_tmp} = {right_df}.with_columns([{', '.join(duplicated)}])")
                right_df = right_tmp
            after_join_drop_cols = [row["new_name"] for row in left_keys if not row["keep"]]
        elif how == "right":
            duplicated = [
                f"{FRAMEWORK}.col({_py_str(row['new_name'])}).alias({_py_str('__jk_' + row['new_name'])})"
                for row in left_keys
                if row["keep"]
            ]
            for position, key in enumerate(left_on):
                row = manager.on_new_name(manager.left, key)
                if row and row["keep"]:
                    left_on[position] = f"__jk_{row['new_name']}"
            if duplicated:
                self._add_code(f"{left_tmp} = {left_df}.with_columns([{', '.join(duplicated)}])")
                left_df = left_tmp
            kept_left = {row["new_name"] for row in left_keys if row["keep"]}
            after_join_drop_cols = list(
                dict.fromkeys(
                    row["new_name"] if row["new_name"] not in kept_left else row["new_name"] + "_right"
                    for row in right_keys
                    if not row["keep"]
                )
            )
        elif how == "outer":
            left_key_names = {row["new_name"] for row in left_keys}
            to_rename = [row for row in right_keys if row["keep"] and row["new_name"] in left_key_names]
            rename_command = {row["new_name"]: f"__jk_{row['new_name']}" for row in to_rename}
            for position, key in enumerate(right_on):
                row = manager.on_new_name(manager.right, key)
                if row and row["keep"] and row["new_name"] in left_key_names:
                    right_on[position] = f"__jk_{row['new_name']}"
            if rename_command:
                self._add_code(f"{right_tmp} = {right_df}.rename({rename_command})")
                right_df = right_tmp
            reverse_action = {f"__jk_{row['new_name']}": row["new_name"] for row in to_rename}
            after_join_drop_cols = [row["new_name"] for row in left_keys if not row["keep"]] + [
                row["new_name"] if row["new_name"] not in left_key_names else row["new_name"] + "_right"
                for row in right_keys
                if not row["keep"]
            ]

        has_post = bool(after_join_drop_cols) or bool(reverse_action)
        self._add_code(f"{var_name} = {'(' if has_post else ''}{left_df}.join(")
        self._add_code(f"        {right_df},")
        self._add_code(f"        left_on={left_on},")
        self._add_code(f"        right_on={right_on},")
        if how in ("right", "outer"):
            self._add_code(f'        how="{how}",')
            self._add_code(f"        coalesce={how == 'right'}")
        else:
            self._add_code(f'        how="{how}"')
        self._add_code("    )")
        if after_join_drop_cols:
            self._add_code(f".drop({after_join_drop_cols})")
        if reverse_action:
            self._add_code(f".rename({reverse_action})")
        if has_post:
            self._add_code(")")

    @staticmethod
    def _join_suffix(manager: _JoinManager) -> str | None:
        """The one suffix a left/inner join's right-side renames follow, when that is all the renaming there is."""
        if manager.how not in ("left", "inner"):
            return None
        if any(not row["keep"] or row["new_name"] != row["old_name"] for row in manager.left):
            return None
        left_names = {row["old_name"] for row in manager.left}
        if len({row["keep"] for row in manager.right if row["join_key"]}) != 1:
            return None
        suffixes = set()
        for row in manager.right:
            if not row["keep"]:
                if not row["join_key"]:
                    return None
                continue
            if row["old_name"] in left_names:
                if row["new_name"] == row["old_name"] or not row["new_name"].startswith(row["old_name"]):
                    return None
                if row["new_name"] in left_names:
                    return None
                suffixes.add(row["new_name"][len(row["old_name"]) :])
            elif row["new_name"] != row["old_name"]:
                return None
        if len(suffixes) > 1:
            return None
        return suffixes.pop() if suffixes else "_right"


_OPERATOR_ALIASES = {
    "=": "equals",
    "==": "equals",
    "!=": "not_equals",
    "<>": "not_equals",
    ">": "greater_than",
    ">=": "greater_than_or_equals",
    "<": "less_than",
    "<=": "less_than_or_equals",
}


def _select_row(row: dict) -> dict:
    """One select row with core's defaults filled in."""
    old = row.get("old_name")
    new = row.get("new_name")
    new = old if new is None else new
    data_type = row.get("data_type")
    # A stored type with no flag beside it is a cast the user chose (older files carry no flag).
    data_type_change = row["data_type_change"] if "data_type_change" in row else data_type is not None
    return {
        "old_name": old,
        "new_name": new,
        "keep": row.get("keep", True),
        "is_available": row.get("is_available", True),
        "is_altered": bool(row.get("is_altered", False) or old != new or data_type_change),
        "data_type_change": bool(data_type_change),
        "data_type": data_type,
        "position": row.get("position"),
    }


def _formula_entries(settings: dict) -> list[dict]:
    functions = settings.get("functions")
    if isinstance(functions, list):
        return [entry for entry in functions if isinstance(entry, dict)]
    function = settings.get("function")
    return [function] if isinstance(function, dict) else []


def _polars_dtype(dtype: str) -> str:
    return f"{FRAMEWORK}.{_DTYPE_MAP.get(dtype, 'Utf8')}"


def _agg_function(agg: str) -> str:
    """The aggregation call for an ``AggColl.agg`` name, e.g. ``"sum"`` -> ``"sum()"``."""
    return {
        "avg": "mean()",
        "average": "mean()",
        "concat": f"str.join({STRING_CONCAT_DELIMITER!r})",
    }.get(agg, f"{agg}()")


def render_notebook(flow: dict, schemas: dict | None = None, locked: dict | None = None) -> dict:
    """Render ``flow`` (flowfile_core's dialect) as notebook cells.

    ``schemas`` maps a node id to its output columns (``[{name, data_type}]``), where known.
    ``locked`` maps a node id to the reason the editor keeps it out of code: it renders as a
    placeholder and its settings are never read.
    """
    return NotebookRenderer(flow, schemas, locked).render()
