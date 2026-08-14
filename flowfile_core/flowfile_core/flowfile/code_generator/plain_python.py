"""Plain-Python (teaching) export.

A fourth code-generation flavour whose output uses no dataframe library at all —
just lists, dicts, loops and the standard library. It exists to answer "how would
I write this myself?", which is a different question from "how do I run this in
production" (that is what the Polars and FlowFrame flavours are for).

Two properties keep it honest:

* **No stealth Polars.** The supported node set is derived from the emitters that
  actually exist on :class:`PlainPythonHandlersMixin`, so a node type nobody wrote
  an emitter for can never fall through to an inherited Polars handler.
* **No stealth wrong answers.** A node this flavour cannot express becomes an
  exercise stub that raises ``NotImplementedError`` — never a silent passthrough
  that would produce different rows than the canvas does.

Unlike the other flavours this one is not part of the codegen parity campaign: it
is a one-way teaching artifact and is not expected to round-trip.
"""

from flowfile_core.flowfile.code_generator.chain_fusion import NodeEmission
from flowfile_core.flowfile.code_generator.code_generator import FlowGraphCodeConverter
from flowfile_core.flowfile.code_generator.plain_python_handlers import (
    _TEMP_NAMES,
    PlainPythonHandlersMixin,
    PlainPythonUnsupported,
)
from flowfile_core.flowfile.flow_graph import FlowGraph
from flowfile_core.flowfile.flow_node.flow_node import FlowNode
from flowfile_core.schemas import input_schema

# Derived, never hand-maintained: an allowlist that drifted ahead of the emitters
# would let an inherited Polars handler emit `pl.` into a dependency-free script.
PLAIN_PYTHON_NODE_TYPES: frozenset[str] = frozenset(
    name[len("_handle_") :] for name in vars(PlainPythonHandlersMixin) if name.startswith("_handle_")
)

NODE_EXPLANATIONS: dict[str, str] = {
    "manual_input": (
        "Holds a small table you typed in by hand. In plain Python that is just a list of "
        "dictionaries — one dictionary per row, with the column names as keys. Every other "
        "node in this flow reads and writes that same shape."
    ),
    "read": (
        "Reads rows from a file. For CSV, Python's built-in csv.DictReader gives you exactly "
        "the same list-of-dicts shape — but every value arrives as text, because a CSV file "
        "has no types. Converting the number columns yourself is the step a dataframe library "
        "silently performs for you when it reads the file."
    ),
    "filter": (
        "Keeps the rows that pass a test and drops the rest. Written by hand it is the most "
        "basic loop there is: walk the rows, check each one, append the keepers to a new list. "
        "The one subtlety is empty values — a comparison against an empty value is neither "
        "true nor false, so those rows drop out."
    ),
    "select": (
        "Chooses which columns survive, what they are called, and what order they come in. "
        "By hand you build a fresh dictionary for each row containing only the keys you want. "
        "Renaming a column is nothing more than using a different key."
    ),
    "sort": (
        "Puts the rows in order. Python's sorted() takes a key function that says what to "
        "compare, and returning a tuple from it sorts by several columns at once. Python's "
        "sort is also stable, which is what lets you mix ascending and descending columns by "
        "sorting more than once."
    ),
    "unique": (
        "Removes duplicate rows. The tool for the job is a set, which remembers what you have "
        "already seen and answers 'have I seen this?' quickly no matter how many rows there "
        "are. The remembered key has to be a tuple, because a set can only hold immutable "
        "values."
    ),
    "group_by": (
        "Collapses many rows into one row per group. By hand this is the accumulator-dict "
        "pattern: walk the rows once, filing each into a list under its key, then walk the "
        "groups and summarise each. It is probably the single most reusable loop in "
        "programming — counting words, bucketing log lines and tallying votes are all this."
    ),
    "join": (
        "Matches rows from two tables on a shared key. The obvious way is a loop inside a "
        "loop, which compares every left row against every right row. The good way is to "
        "index one side into a dictionary first and then look each row up, turning "
        "len(left) x len(right) work into len(left) + len(right). That difference is the "
        "reason databases build indexes."
    ),
    "union": (
        "Stacks tables on top of each other. Adding the lists together is the easy part; "
        "the fiddly part is that the tables may not have the same columns, so missing values "
        "have to be filled in."
    ),
    "record_count": (
        "Counts the rows and returns that single number as a one-row table. In plain Python "
        "it is len() — the simplest node there is."
    ),
    # Unsupported types still get prose — "why this one is different" is itself the lesson.
    "formula": (
        "Computes a new column from an expression you typed. Flowfile hands that expression to "
        "a parser that turns it into dataframe operations — so writing this node by hand in "
        "general would mean writing an expression language, not a for loop. Your particular "
        "formula, though, is usually a one-line calculation you can do inside a loop yourself. "
        "That is what the stub below is for."
    ),
    "polars_code": (
        "Runs Polars code you wrote yourself. There is nothing to translate — the node's whole "
        "content is already dataframe-library code, so a plain-Python version would mean "
        "reimplementing whatever that code does."
    ),
    "sql_query": (
        "Runs a SQL query against the incoming table. Turning SQL into loops means writing a "
        "query engine — parsing, planning and executing — which is a much bigger exercise than "
        "any single node here."
    ),
    "python_script": (
        "Runs your own Python in a sandboxed kernel. It is already plain Python, so there is "
        "nothing for this panel to translate — read the node's own code instead."
    ),
}


class FlowGraphToPlainPythonConverter(PlainPythonHandlersMixin, FlowGraphCodeConverter):
    """Generates dependency-free Python (lists, dicts, loops) from a FlowGraph.

    ``PlainPythonHandlersMixin`` comes first in the MRO so its emitters win over
    the Polars ones for every node type it covers.
    """

    function_name = "run_etl_pipeline"

    def __init__(self, flow_graph: FlowGraph):
        super().__init__(flow_graph)
        # Long explanations are emitted once per script, not once per node.
        self._taught: set[str] = set()
        # node_id -> (start, end) into the rendered body, for the per-node explainer.
        self._rendered_spans: dict[int, tuple[int, int]] = {}
        self._rendered_body: list[str] = []
        self._temp_names = self._plan_temp_names()
        # Aggregated column -> value-list identifier; re-planned per group_by node.
        self._value_names: dict[str, str] = {}

    def _plan_temp_names(self) -> dict[str, str]:
        """Loop-variable names that cannot collide with a user-pinned node reference.

        A node named `row` would otherwise be rebound by `for row in row:` — pinned
        names survive boundary renaming, so the loop temps are what must move.
        """
        pinned = {
            reference
            for node in self.flow_graph.nodes
            if (reference := getattr(node.setting_input, "node_reference", None))
        }
        chosen: dict[str, str] = {}
        used = set(pinned)
        for name in _TEMP_NAMES:
            candidate, bump = name, 2
            while candidate in used:
                candidate, bump = f"{name}_{bump}", bump + 1
            chosen[name] = candidate
            used.add(candidate)
        return chosen

    # ------------------------------------------------------------- dispatching

    def _resolve_handler(self, node: FlowNode):
        """Only emit node types this flavour has a real plain-Python emitter for.

        Everything else — including custom nodes, whose ``process()`` is Polars —
        returns None and lands in ``_handle_unsupported`` as an exercise stub.
        """
        # Custom nodes first: a user node whose slug collides with a built-in
        # would otherwise reach that built-in's emitter and blow up on settings
        # it does not have.
        settings = node.setting_input
        if isinstance(settings, input_schema.UserDefinedNode) or getattr(settings, "is_user_defined", False):
            return None
        if node.node_type not in PLAIN_PYTHON_NODE_TYPES:
            return None
        handler = getattr(self, f"_handle_{node.node_type}", None)
        if handler is None:
            return None

        def guarded(settings, var_name: str, input_vars: dict[str, str]) -> None:
            mark = len(self.code_lines)
            try:
                handler(settings, var_name, input_vars)
            except PlainPythonUnsupported as exc:
                del self.code_lines[mark:]
                self.node_var_mapping[node.node_id] = var_name
                self._emit_exercise_stub(node, var_name, input_vars, exc.reason)

        return guarded

    def _handle_unsupported(self, node: FlowNode, var_name: str, input_vars: dict[str, str]) -> None:
        """Emit an exercise stub rather than failing the whole export.

        Deliberately does not append to ``unsupported_nodes``, so ``_convert_core``
        never raises: one expression-engine node should not cost you the other
        eight nodes' worth of readable code.
        """
        self._emit_exercise_stub(node, var_name, input_vars, self._stub_reason(node))

    @staticmethod
    def _stub_reason(node: FlowNode) -> str:
        node_type = node.node_type
        if node_type == "formula":
            return "Flowfile evaluates this with its expression engine"
        if node_type in ("polars_code", "sql_query", "python_script"):
            return "this node contains code written against a dataframe library"
        return f"'{node_type}' has no plain-Python emitter yet"

    @staticmethod
    def _stub_detail(node: FlowNode) -> str | None:
        """The interesting bit of the node's config, quoted into the stub."""
        settings = node.setting_input
        function = getattr(settings, "function", None)
        if function is not None and getattr(function, "function", None):
            return str(function.function)
        filter_input = getattr(settings, "filter_input", None)
        if filter_input is not None and getattr(filter_input, "advanced_filter", None):
            return str(filter_input.advanced_filter)
        return None

    def _emit_exercise_stub(
        self, node: FlowNode, var_name: str, input_vars: dict[str, str], reason: str
    ) -> None:
        """A named function that raises, framed as something for the reader to write.

        Keeps the script dependency-free and keeps it honest: it stops at the gap
        instead of quietly producing rows that differ from the canvas.
        """
        safe_type = "".join(char if char.isalnum() else "_" for char in node.node_type)
        function_name = f"{safe_type}_{node.node_id}"
        arguments = [value for value in input_vars.values()] or []
        parameters = ", ".join(f"rows_{index}" for index in range(len(arguments))) or ""

        self._section(f"{node.node_type} (node {node.node_id}) — over to you")
        self._add_code(f"# {reason}.")
        detail = self._stub_detail(node)
        if detail:
            self._add_code("# The rule it applies is:")
            for line in str(detail).splitlines():
                self._add_code(f"#     {line}")
        self._add_code("# Writing this one by hand is the exercise. Replace the raise below with")
        self._add_code("# a loop that returns the new list of rows, and the rest of the script runs.")
        self._add_code(f"def {function_name}({parameters}):")
        self._add_code(f"    raise NotImplementedError({self._py_str(reason)})")
        self._add_code("")
        self._add_code(f"{var_name} = {function_name}({', '.join(arguments)})")
        self._add_code("")

    # ---------------------------------------------------------------- assembly

    def _render_body(self) -> list[str]:
        """Lay the node blocks out in order, with no chain fusion.

        Fusion is right for Polars (``df.filter(...).sort(...)`` reads well as one
        pipe) and structurally wrong here — you cannot fuse two for-loops into an
        expression. One labelled block per node is also what keeps the generated
        script legible against the canvas.
        """
        emissions: list[NodeEmission] = []
        node_by_id: dict[int, FlowNode] = {}
        for node, effective_var, start, end in self._node_spans:
            lines = self.code_lines[start:end]
            while lines and lines[-1] == "":
                lines = lines[:-1]
            if not lines:
                continue
            emissions.append(
                NodeEmission(
                    node.node_id,
                    effective_var,
                    lines,
                    None,
                    0,
                    self._is_flow_output(node),
                    bool(getattr(node.setting_input, "node_reference", None)),
                )
            )
            node_by_id[node.node_id] = node

        body: list[str] = []
        spans: dict[int, tuple[int, int]] = {}
        for emission in emissions:
            start = len(body)
            body.extend(emission.lines)
            spans[emission.node_id] = (start, len(body))
            body.append("")

        survivors = {emission.node_id for emission in emissions}
        rename = self._plan_boundary_names(emissions, survivors, node_by_id)
        rendered = self._apply_renames(body, rename)
        self._rendered_body = rendered
        self._rendered_spans = spans
        return rendered

    def _docstring_lines(self) -> list[str]:
        return [
            '    """',
            f"    {self.flow_graph.__name__}",
            "    Generated from Flowfile — plain Python, no dataframe library.",
            "",
            "    Every table here is a list of dicts: one dict per row, keyed by column",
            "    name. Read it top to bottom; each block is one node on the canvas.",
            '    """',
        ]

    def _append_module_epilogue(self, lines: list[str]) -> None:
        lines.append("")
        lines.append("")
        lines.append('if __name__ == "__main__":')
        lines.append(f"    pipeline_output = {self.function_name}()")
        if len(self.output_nodes) > 1:
            lines.append("    print(pipeline_output)")
        else:
            # `or []` covers a flow with nothing to return (writers only, or empty).
            lines.append("    for row in pipeline_output or []:")
            lines.append("        print(row)")

    # --------------------------------------------------------------- explainer

    def node_snippet(self, node_id: int) -> str | None:
        """The rendered block for one node, after boundary renaming."""
        span = self._rendered_spans.get(node_id)
        if span is None:
            return None
        start, end = span
        return "\n".join(self._rendered_body[start:end]).rstrip()


def export_flow_to_plain_python(flow_graph: FlowGraph) -> str:
    """Render a flow as dependency-free Python built from lists, dicts and loops."""
    return FlowGraphToPlainPythonConverter(flow_graph).convert()


def explain_node_in_plain_python(flow_graph: FlowGraph, node_id: int) -> dict:
    """Plain-English description of one node plus its plain-Python equivalent.

    The snippet is generated from the node's *actual* settings, so it reflects the
    columns and operators the user configured rather than a generic example.
    """
    node = flow_graph.get_node(node_id)
    node_type = node.node_type if node is not None else ""
    supported = node_type in PLAIN_PYTHON_NODE_TYPES

    converter = FlowGraphToPlainPythonConverter(flow_graph)
    snippet: str | None = None
    try:
        converter.convert()
        snippet = converter.node_snippet(node_id)
    except Exception:
        snippet = None

    return {
        "node_id": node_id,
        "node_type": node_type,
        "supported": supported,
        "explanation": NODE_EXPLANATIONS.get(node_type),
        "code": snippet,
    }
