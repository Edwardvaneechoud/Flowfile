"""Whether a formula node's entries can be evaluated in one parallel step.

A formula node evaluates its entries sequentially — entry N sees the outputs of entries
1..N-1 — but when no entry reads a column another entry writes, the sequential and the
parallel evaluation produce the same frame. ``flowfile_frame.with_columns`` uses that to
decide whether Polars' parallel semantics can be represented by one Formula node at all.
Code generation deliberately does not use it: an exported node always chains one
``with_columns`` step per entry, so the reader sees the evaluation order.

The predicate fails closed: an expression whose column references cannot be determined is
treated as conflicting with every other entry.
"""

from collections.abc import Sequence

from flowfile_core.flowfile.settings_validation import _expression_column_references


def entries_are_independent(entries: Sequence[tuple[str, str]]) -> bool:
    """True when no entry reads a column another entry writes.

    ``entries`` are ``(output_name, expression)`` pairs in evaluation order. An entry reading
    the column it writes itself is not a conflict — sequential and parallel evaluation both
    read the upstream value for it. Two entries writing the same output name are never
    independent: sequentially the later one wins, in one call the result is undefined. Blank
    expressions contribute neither reads nor writes, and an unparseable expression makes the
    whole set dependent.
    """
    active = [(name, expression) for name, expression in entries if expression and expression.strip()]
    if len(active) < 2:
        return True
    outputs = [name for name, _ in active]
    if len(set(outputs)) != len(outputs):
        return False
    reads: list[set[str]] = []
    for _, expression in active:
        references = _expression_column_references(expression)
        if references is None:
            return False
        reads.append(set(references))
    return all(not own_reads & (set(outputs) - {outputs[index]}) for index, own_reads in enumerate(reads))
