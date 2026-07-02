"""Ratchet guard: inline execution_location branches in flow_graph.py may only shrink.

Nodes migrated to the ExecutionBackend seam (output, database_reader,
random_split) must not regress to inline ``if execution_location == "local"``
branches, and new nodes must not add any. Lower the ceiling as follow-up
migrations land; never raise it.
"""

import re
from pathlib import Path

import flowfile_core.flowfile.flow_graph as flow_graph_module

# 13 sites existed before the backend seam; 3 have been migrated.
MAX_INLINE_LOCATION_COMPARISONS = 10

_MIGRATED_FUNCTIONS = ["def add_output", "def add_database_reader", "def add_random_split"]


def _flow_graph_source() -> str:
    return Path(flow_graph_module.__file__).read_text()


def test_inline_location_branch_count_only_shrinks():
    source = _flow_graph_source()
    comparisons = re.findall(r'execution_location [!=]= "local"', source)
    assert len(comparisons) <= MAX_INLINE_LOCATION_COMPARISONS, (
        f"flow_graph.py has {len(comparisons)} inline execution_location comparisons "
        f"(ceiling {MAX_INLINE_LOCATION_COMPARISONS}). Route new local/remote variance through "
        f"ExecutionBackend (flowfile/execution/backends) instead of branching inline."
    )


def test_migrated_builders_stay_branch_free():
    source = _flow_graph_source()
    for marker in _MIGRATED_FUNCTIONS:
        start = source.index(marker)
        # A method body ends where the next def at the same indentation starts.
        next_def = source.find("\n    def ", start + 1)
        body = source[start : next_def if next_def != -1 else len(source)]
        assert 'execution_location == "local"' not in body, f"{marker} regressed to an inline location branch"
        assert 'execution_location != "local"' not in body, f"{marker} regressed to an inline location branch"
