"""Query fuzzing and parser edge case tests.

Each case asserts the parser's actual contract — which inputs are rejected,
with which error code, and what the accepted ones return. Asserting only that
the server stayed alive would pass even if a guard stopped firing entirely,
which is how a nesting or term-count limit silently disappears.
"""

from __future__ import annotations

import pytest

from lib.protocol import (
    INDEX_ERRORS,
    QUERY_ERRORS,
    assert_error,
    parse_count,
    parse_search_results,
)
from lib.raw_socket import raw_tcp_exchange

# Parser guards enforced by the server. Nesting depth is a property of the
# boolean expression parser, so reaching it requires a boolean operator: a
# parenthesised query with no AND/OR/NOT never enters that parser at all.
MAX_NESTING_DEPTH = 32
# The cap counts the terms joined onto the first one, so a query may carry
# MAX_AND_TERMS + 1 terms in total before it trips.
MAX_AND_TERMS = 64
# Measured in bytes over the query expression (search text, AND/NOT terms and
# filter columns and values), not in characters over the raw command line.
MAX_QUERY_LENGTH = 128


@pytest.mark.edge_cases
class TestQueryFuzzing:
    """QueryParser edge case and fuzzing tests."""

    def _exchange(self, mygramdb, command: str, timeout: float = 10.0) -> bytes:
        return raw_tcp_exchange(
            mygramdb.host, mygramdb.tcp_port, (command + "\r\n").encode("utf-8"), timeout=timeout
        )

    def _nested_boolean(self, depth: int) -> str:
        """Build a boolean expression nested to exactly ``depth`` levels."""
        return "(" * depth + "ab AND cd" + ")" * depth

    def test_nesting_just_below_limit_is_accepted(self, mygramdb, seed_data):
        """A boolean expression one level below the limit parses normally."""
        query = self._nested_boolean(MAX_NESTING_DEPTH - 1)
        assert len(query) <= MAX_QUERY_LENGTH, "probe must not trip the length guard first"
        parse_search_results(self._exchange(mygramdb, f"SEARCH testdb.articles {query}"))

    def test_exceeds_nesting_limit(self, mygramdb, seed_data):
        """At the limit the boolean parser refuses, naming nesting depth.

        The expression must contain a boolean operator: without one the query
        never reaches the expression parser, so a parenthesis-only probe would
        pass whether or not the guard exists.
        """
        query = self._nested_boolean(MAX_NESTING_DEPTH)
        assert len(query) <= MAX_QUERY_LENGTH, "probe must not trip the length guard first"
        assert_error(
            self._exchange(mygramdb, f"SEARCH testdb.articles {query}"),
            code_in=QUERY_ERRORS,
            message_contains="nesting depth",
        )

    def test_deep_parentheses_without_operator_are_plain_text(self, mygramdb, seed_data):
        """Parentheses with no operator are query text, not a nested expression.

        This pins the boundary that makes the nesting test above meaningful:
        the same depth that errors with an operator must not error without one.
        """
        query = "(" * MAX_NESTING_DEPTH + "ab" + ")" * MAX_NESTING_DEPTH
        assert len(query) <= MAX_QUERY_LENGTH, "probe must not trip the length guard first"
        parse_search_results(self._exchange(mygramdb, f"SEARCH testdb.articles {query}"))

    def test_unmatched_open_parens(self, mygramdb, seed_data):
        """An unclosed parenthesis is a parse error naming the parenthesis."""
        assert_error(
            self._exchange(mygramdb, "SEARCH testdb.articles (((test"),
            code_in=QUERY_ERRORS,
            message_contains="parenthesis",
        )

    def test_unmatched_close_parens(self, mygramdb, seed_data):
        """An unmatched closing parenthesis is a parse error."""
        assert_error(
            self._exchange(mygramdb, "SEARCH testdb.articles test)))"),
            code_in=QUERY_ERRORS,
            message_contains="parenthesis",
        )

    def test_empty_parentheses(self, mygramdb, seed_data):
        """Empty parentheses carry no term, so they match nothing."""
        raw = self._exchange(mygramdb, "SEARCH testdb.articles ()")
        assert parse_search_results(raw).ids == []

    def test_only_operators_no_terms(self, mygramdb, seed_data):
        """Operators with no operand are a boolean expression error."""
        assert_error(
            self._exchange(mygramdb, "SEARCH testdb.articles AND OR NOT"),
            code_in=QUERY_ERRORS,
            message_contains="boolean",
        )

    def test_repeated_not_operators(self, mygramdb, seed_data):
        """Stacked NOT operators parse and return a well-formed result set."""
        result = parse_search_results(
            self._exchange(mygramdb, "SEARCH testdb.articles NOT NOT NOT test")
        )
        assert result.total > 0, "stacked NOT operators returned no matches at all"

    def _and_chain(self, term_count: int) -> str:
        """Build an AND chain of single-character terms.

        Single characters keep the expression well under the byte limit so that
        the term cap is what the query trips, not the length guard.
        """
        return " AND ".join("abcdefghijklmnopqrstuvwxyz"[i % 26] for i in range(term_count))

    def test_and_chain_at_term_cap_is_accepted(self, mygramdb, seed_data):
        """A chain exactly at the cap parses, pinning the off-by-one."""
        term_count = MAX_AND_TERMS + 1
        terms = self._and_chain(term_count)
        # One byte per term, so the expression length is the term count.
        assert term_count <= MAX_QUERY_LENGTH, "probe must not trip the length guard first"
        parse_search_results(self._exchange(mygramdb, f"SEARCH testdb.articles {terms}"))

    def test_long_and_chain(self, mygramdb, seed_data):
        """An AND chain one term past the cap is refused, naming the cap."""
        terms = self._and_chain(MAX_AND_TERMS + 2)
        assert_error(
            self._exchange(mygramdb, f"SEARCH testdb.articles {terms}"),
            code_in=QUERY_ERRORS,
            message_contains="too many and terms",
        )

    def test_unclosed_double_quotes(self, mygramdb, seed_data):
        """An unclosed double quote is a parse error naming the quote."""
        assert_error(
            self._exchange(mygramdb, 'SEARCH testdb.articles "unclosed'),
            code_in=QUERY_ERRORS,
            message_contains="unclosed quote",
        )

    def test_unclosed_single_quotes(self, mygramdb, seed_data):
        """An unclosed single quote is rejected the same way as a double quote.

        Treating one quote style as a literal and the other as a delimiter
        would make the same text mean different things per quote character.
        """
        assert_error(
            self._exchange(mygramdb, "SEARCH testdb.articles 'unclosed"),
            code_in=QUERY_ERRORS,
            message_contains="unclosed quote",
        )

    def test_filter_injection(self, mygramdb, seed_data):
        """A SQL fragment in a FILTER clause is rejected as a filter operator.

        The fragment must be refused by the filter grammar rather than reaching
        any query-construction path, and the table must survive intact.
        """
        before = parse_count(self._exchange(mygramdb, "COUNT testdb.articles test"))
        assert_error(
            self._exchange(mygramdb, "SEARCH testdb.articles test FILTER status 1; DROP TABLE"),
            code_in=QUERY_ERRORS,
            message_contains="invalid filter operator",
        )
        after = parse_count(self._exchange(mygramdb, "COUNT testdb.articles test"))
        assert after == before, (
            f"document count changed across the injection attempt: {before} -> {after}"
        )

    def test_negative_limit(self, mygramdb, seed_data):
        """A negative LIMIT is refused rather than wrapping to a huge value."""
        assert_error(
            self._exchange(mygramdb, "SEARCH testdb.articles test LIMIT -1"),
            code_in=QUERY_ERRORS,
            message_contains="limit must be positive",
        )

    def test_zero_limit(self, mygramdb, seed_data):
        """LIMIT 0 is refused, not silently treated as unlimited."""
        assert_error(
            self._exchange(mygramdb, "SEARCH testdb.articles test LIMIT 0"),
            code_in=QUERY_ERRORS,
            message_contains="limit must be positive",
        )

    def test_limit_overflow(self, mygramdb, seed_data):
        """A LIMIT past the value range is refused, not truncated."""
        assert_error(
            self._exchange(mygramdb, "SEARCH testdb.articles test LIMIT 999999999999"),
            code_in=QUERY_ERRORS,
            message_contains="invalid limit value",
        )

    def test_limit_with_huge_offset(self, mygramdb, seed_data):
        """An offset past the end returns no ids while still reporting a total."""
        result = mygramdb.search("testdb.articles", "test", offset=999999, limit=10)
        assert result["ids"] == [], f"offset past the end returned ids: {result['ids'][:10]}"

    def test_unknown_command(self, mygramdb, seed_data):
        """An unknown verb is refused and named back to the caller."""
        assert_error(
            self._exchange(mygramdb, "FOOBAR testdb.articles test"),
            code_in=QUERY_ERRORS,
            message_contains="unknown command",
        )

    def test_search_nonexistent_table(self, mygramdb, seed_data):
        """A missing table is an index-layer error, distinct from a parse error.

        Collapsing this into the query-error range would stop callers being
        able to tell a typo in the table name from a malformed query.
        """
        assert_error(
            self._exchange(mygramdb, "SEARCH nonexistent_table_xyz test"),
            code_in=INDEX_ERRORS,
            message_contains="table not found",
        )

    @pytest.mark.parametrize("keyword", ["AND", "FILTER", "LIMIT", "SORT"])
    def test_clause_keyword_alone_is_not_search_text(self, mygramdb, seed_data, keyword):
        """A bare clause keyword leaves no search text, so SEARCH is refused."""
        assert_error(
            self._exchange(mygramdb, f"SEARCH testdb.articles {keyword}"),
            code_in=QUERY_ERRORS,
            message_contains="requires search text",
        )

    @pytest.mark.parametrize("keyword", ["OR", "NOT"])
    def test_operator_keyword_alone_is_query_text(self, mygramdb, seed_data, keyword):
        """A bare OR/NOT is consumed as query text and matches nothing."""
        raw = self._exchange(mygramdb, f"SEARCH testdb.articles {keyword}")
        assert parse_search_results(raw).ids == []

    @pytest.mark.parametrize("length", [MAX_QUERY_LENGTH + 1, 1000, 10000])
    def test_query_over_length_boundary_is_refused(self, mygramdb, seed_data, length):
        """Every over-length query is refused, naming its length and the limit."""
        resp = assert_error(
            self._exchange(mygramdb, "SEARCH testdb.articles " + "a" * length),
            code_in=QUERY_ERRORS,
            message_contains="length",
        )
        assert str(length) in resp.message, (
            f"the rejected length should be reported, got: {resp.message!r}"
        )
        assert str(MAX_QUERY_LENGTH) in resp.message, (
            f"the configured limit should be reported, got: {resp.message!r}"
        )

    def test_query_at_length_boundary_is_accepted(self, mygramdb, seed_data):
        """A query exactly at the limit is accepted, pinning the comparison.

        Without this the limit could drift to an off-by-one and the rejection
        tests above would not notice.
        """
        parse_search_results(
            self._exchange(mygramdb, "SEARCH testdb.articles " + "a" * MAX_QUERY_LENGTH)
        )
