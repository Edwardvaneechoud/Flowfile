"""Pure tokenizing and scoring of the global search."""

from flowfile_core.search.matching import (
    EXACT,
    PREFIX,
    SUBSTRING,
    WORD_START,
    like_pattern,
    match_quality,
    score_fields,
    sql_narrowable,
    tokenize,
)


def test_tokenize_splits_on_whitespace_and_dots_and_dedupes():
    assert tokenize("  Sales.Orders  sales ") == ["sales", "orders"]
    assert tokenize("a/b\\c:d,e;f") == ["a", "b", "c", "d", "e", "f"]
    assert tokenize("one two three four five six seven eight nine") == [
        "one",
        "two",
        "three",
        "four",
        "five",
        "six",
        "seven",
        "eight",
    ]
    assert tokenize(" ... ") == []


def test_tokenize_trims_quotes_and_wildcards_and_drops_bare_separators():
    assert tokenize('"orders" `raw*` ord? - __') == ["orders", "raw", "ord"]


def test_match_quality_ranks_whole_prefix_word_and_substring():
    assert match_quality("orders", "Orders") == EXACT
    assert match_quality("ord", "orders_2024") == PREFIX
    assert match_quality("orders", "raw_orders") == WORD_START
    assert match_quality("orders", "raw orders") == WORD_START
    assert match_quality("der", "orders") == SUBSTRING
    assert match_quality("xyz", "orders") == 0.0


def test_word_start_found_after_an_earlier_mid_word_hit():
    assert match_quality("id", "valid_id") == WORD_START


def test_every_token_must_match_some_field():
    fields = [("name", "orders"), ("path", "finance.sales"), ("description", None)]
    assert score_fields(["orders", "sales"], fields) is not None
    assert score_fields(["orders", "marketing"], fields) is None
    assert score_fields([], fields) is None


def test_name_outranks_path_and_description():
    by_name = score_fields(["orders"], [("name", "orders"), ("description", "x")])
    by_path = score_fields(["orders"], [("name", "x"), ("path", "orders.x")])
    by_description = score_fields(["orders"], [("name", "x"), ("description", "orders of the day")])
    assert by_name.score > by_path.score > by_description.score


def test_whole_query_as_name_gets_a_bonus_over_a_prefix():
    exact = score_fields(["sales", "orders"], [("name", "sales_orders")])
    prefix = score_fields(["sales", "orders"], [("name", "sales_orders_2024")])
    assert exact.score > prefix.score


def test_matches_name_the_fields_that_matched():
    scored = score_fields(
        ["orders", "finance"], [("name", "orders"), ("path", "finance.sales"), ("description", "orders")]
    )
    assert scored.matches == (("name", "orders"), ("path", "finance.sales"))


def test_separators_are_interchangeable():
    assert match_quality("sales-orders", "sales_orders") == EXACT
    assert match_quality("sales_orders", "Sales Orders") == EXACT
    assert match_quality("sales-ord", "raw.sales_orders") == WORD_START
    assert match_quality("sales-orders", "salesorders") == 0.0


def test_like_pattern_escapes_percent_and_backslash_and_widens_separators():
    assert like_pattern("50%") == "%50\\%%"
    assert like_pattern("a\\b") == "%a\\\\b%"
    assert like_pattern("sales-_orders") == "%sales%orders%"


def test_only_ascii_tokens_without_quotes_narrow_in_sql():
    assert sql_narrowable("orders")
    assert not sql_narrowable("caf\u00e9")
    assert not sql_narrowable('a"b')


def test_an_ascii_word_folds_case_like_sql_does():
    """SQL's LIKE folds ASCII letters only, so an ASCII word must not match text only Unicode folding makes match."""
    assert match_quality("ki", "K\u0130RA") == 0.0
    assert match_quality("k", "\u212aelvin") == 0.0
    assert match_quality("caf\u00e9", "CAF\u00c9") == EXACT
    assert match_quality("orders", "ORDERS") == EXACT
