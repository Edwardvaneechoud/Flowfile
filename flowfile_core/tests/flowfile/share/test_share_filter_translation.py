"""The translated filter has to mean the same thing after the link round trip.

Rewriting an advanced filter into a basic one is only worth doing if the two
select the same rows, so every case here runs the flow twice: once with the
expression the user wrote, once with the basic filter that came back out of an
encoded share link. The browser side of the same contract is pinned by the
``translated advanced filter`` fixtures in ``flowfile_wasm/tests/helpers/parity.ts``,
which run these settings through the engine the recipient actually gets.
"""

import pytest

from flowfile_core.flowfile.share.encoder import decode_share_hash, encode_envelope
from flowfile_core.flowfile.share.transform import build_share_envelope
from flowfile_core.schemas import transform_schema

from tests.flowfile.share.conftest import add_filter, add_manual_input, make_graph

# A null city and a negative balance: `!=` has to drop the null row on both
# paths, and a negative bound has to survive the parser's negation wrapper.
ROWS = [
    {"city": "Amsterdam", "quantity": 3, "price": 9.5, "balance": -10},
    {"city": "Berlin", "quantity": 8, "price": 10.5, "balance": 0},
    {"city": "Amsterdam", "quantity": 12, "price": 20.0, "balance": 7},
    {"city": None, "quantity": 7, "price": 10.0, "balance": -3},
]

EXPRESSIONS = [
    "[quantity] > 7",
    "[quantity] <= 3",
    "[quantity] != 7",
    '[city] = "Amsterdam"',
    '[city] != "Amsterdam"',
    "[price] > 10",
    "[balance] >= -5",
]


def _run(filter_input: transform_schema.FilterInput) -> list[dict]:
    graph = make_graph(name="filter_round_trip")
    add_manual_input(graph, node_id=1, data=ROWS)
    add_filter(graph, 2, 1, filter_input)

    run_info = graph.run_graph()
    assert run_info is not None and run_info.success, "the filter flow did not run"
    return graph.get_node(2).get_resulting_data().collect().to_dicts()


def _shared_filter_settings(expression: str) -> dict:
    """The filter node's settings as they come back out of an encoded link."""
    graph = make_graph(name="filter_share")
    add_manual_input(graph, node_id=1, data=ROWS)
    add_filter(graph, 2, 1, transform_schema.FilterInput(mode="advanced", advanced_filter=expression))

    envelope = decode_share_hash(encode_envelope(build_share_envelope(graph).envelope))
    node = next(node for node in envelope["flow"]["nodes"] if node["id"] == 2)
    return node["setting_input"]["filter_input"]


@pytest.mark.parametrize("expression", EXPRESSIONS)
def test_the_decoded_basic_filter_selects_the_same_rows(expression):
    decoded = _shared_filter_settings(expression)
    assert decoded["mode"] == "basic", f"{expression!r} was not translated"
    assert not decoded.get("advanced_filter"), "the expression body travelled anyway"

    advanced_rows = _run(transform_schema.FilterInput(mode="advanced", advanced_filter=expression))
    basic_rows = _run(transform_schema.FilterInput(**decoded))
    assert basic_rows == advanced_rows
    assert advanced_rows != ROWS, f"{expression!r} filters nothing, so it proves nothing"


def test_an_untranslatable_filter_still_arrives_as_a_placeholder():
    graph = make_graph(name="filter_share_placeholder")
    add_manual_input(graph, node_id=1, data=ROWS)
    add_filter(
        graph,
        2,
        1,
        transform_schema.FilterInput(mode="advanced", advanced_filter="[quantity] > 7 and [price] > 10"),
    )

    envelope = decode_share_hash(encode_envelope(build_share_envelope(graph).envelope))
    node = next(node for node in envelope["flow"]["nodes"] if node["id"] == 2)
    assert node["setting_input"]["is_placeholder"] is True
    assert node["type"].endswith("__unsupported")
