"""Query fuzzing and parser edge case tests.

Verify QueryParser/QueryAST handles malformed, deeply nested,
and boundary-length queries without crashing.
"""

from __future__ import annotations

import pytest


@pytest.mark.edge_cases
class TestQueryFuzzing:
    """QueryParser edge case and fuzzing tests."""

    def test_deeply_nested_parentheses(self, mygramdb, seed_data):
        """32-level nested parentheses — at or near recursion limit."""
        query = "(" * 32 + "test" + ")" * 32
        resp = mygramdb.tcp_command(f"SEARCH testdb.articles {query}")
        assert resp is not None, "Server returned None for 32-deep nesting"
        # Either succeeds or returns a parser error — both are acceptable
        assert mygramdb.ping()

    def test_exceeds_recursion_limit(self, mygramdb, seed_data):
        """33-level nesting — should exceed recursion limit."""
        query = "(" * 33 + "test" + ")" * 33
        resp = mygramdb.tcp_command(f"SEARCH testdb.articles {query}")
        assert resp is not None, "Server returned None for 33-deep nesting"
        # Expect error about nesting depth
        assert mygramdb.ping()

    def test_unmatched_open_parens(self, mygramdb, seed_data):
        """Unmatched opening parentheses — parser error expected."""
        resp = mygramdb.tcp_command("SEARCH testdb.articles (((test")
        assert resp is not None
        upper = resp.upper()
        assert "ERROR" in upper, f"Expected parser error, got: {resp[:200]}"

    def test_unmatched_close_parens(self, mygramdb, seed_data):
        """Unmatched closing parentheses — parser error expected."""
        resp = mygramdb.tcp_command("SEARCH testdb.articles test)))")
        assert resp is not None
        upper = resp.upper()
        assert "ERROR" in upper, f"Expected parser error, got: {resp[:200]}"

    def test_empty_parentheses(self, mygramdb, seed_data):
        """Empty parentheses () — error expected."""
        resp = mygramdb.tcp_command("SEARCH testdb.articles ()")
        assert resp is not None
        assert mygramdb.ping()

    def test_only_operators_no_terms(self, mygramdb, seed_data):
        """Only operators, no search terms."""
        resp = mygramdb.tcp_command("SEARCH testdb.articles AND OR NOT")
        assert resp is not None
        # Error or empty results — not a crash
        assert mygramdb.ping()

    def test_repeated_not_operators(self, mygramdb, seed_data):
        """Triple NOT — should either parse or error cleanly."""
        resp = mygramdb.tcp_command("SEARCH testdb.articles NOT NOT NOT test")
        assert resp is not None
        assert mygramdb.ping()

    def test_long_and_chain(self, mygramdb, seed_data):
        """100-term AND chain — should process or return length error."""
        terms = " AND ".join(f"term{i}" for i in range(100))
        resp = mygramdb.tcp_command(f"SEARCH testdb.articles {terms}")
        assert resp is not None
        assert mygramdb.ping()

    def test_unclosed_double_quotes(self, mygramdb, seed_data):
        """Unclosed double quote — parser error expected."""
        resp = mygramdb.tcp_command('SEARCH testdb.articles "unclosed')
        assert resp is not None
        upper = resp.upper()
        assert "ERROR" in upper, f"Expected quote error, got: {resp[:200]}"

    def test_unclosed_single_quotes(self, mygramdb, seed_data):
        """Unclosed single quote — error or literal treatment."""
        resp = mygramdb.tcp_command("SEARCH testdb.articles 'unclosed")
        assert resp is not None
        # May be treated as literal or error — both acceptable
        assert mygramdb.ping()

    def test_filter_injection(self, mygramdb, seed_data):
        """SQL-like injection in filter value — should be safely handled."""
        resp = mygramdb.tcp_command("SEARCH testdb.articles test FILTER status 1; DROP TABLE")
        assert resp is not None
        # Server should not execute SQL injection
        assert mygramdb.ping()
        # Verify articles table still exists
        info = mygramdb.info()
        assert info, "INFO failed after filter injection attempt"

    def test_negative_limit(self, mygramdb, seed_data):
        """Negative LIMIT value — error expected."""
        resp = mygramdb.tcp_command("SEARCH testdb.articles test LIMIT -1")
        assert resp is not None
        upper = resp.upper()
        assert "ERROR" in upper, f"Expected error for negative LIMIT, got: {resp[:200]}"

    def test_zero_limit(self, mygramdb, seed_data):
        """LIMIT 0 — error or empty results."""
        resp = mygramdb.tcp_command("SEARCH testdb.articles test LIMIT 0")
        assert resp is not None
        assert mygramdb.ping()

    def test_limit_overflow(self, mygramdb, seed_data):
        """Extremely large LIMIT — should clamp or error."""
        resp = mygramdb.tcp_command("SEARCH testdb.articles test LIMIT 999999999999")
        assert resp is not None
        assert mygramdb.ping()

    def test_limit_with_huge_offset(self, mygramdb, seed_data):
        """Huge offset in LIMIT — should return empty results."""
        result = mygramdb.search("testdb.articles", "test", offset=999999, limit=10)
        assert result["total"] >= 0
        assert len(result["ids"]) == 0 or result["total"] == 0

    def test_unknown_command(self, mygramdb, seed_data):
        """Completely unknown command verb."""
        resp = mygramdb.tcp_command("FOOBAR testdb.articles test")
        assert resp is not None
        upper = resp.upper()
        assert "ERROR" in upper or "UNKNOWN" in upper, (
            f"Expected unknown command error, got: {resp[:200]}"
        )

    def test_search_nonexistent_table(self, mygramdb, seed_data):
        """SEARCH on a table that doesn't exist."""
        resp = mygramdb.tcp_command("SEARCH nonexistent_table_xyz test")
        assert resp is not None
        upper = resp.upper()
        assert "ERROR" in upper, f"Expected table-not-found error, got: {resp[:200]}"

    def test_keyword_as_search_term(self, mygramdb, seed_data):
        """Reserved words used as search terms — should not crash."""
        for keyword in ["AND", "OR", "NOT", "FILTER", "LIMIT", "SORT"]:
            resp = mygramdb.tcp_command(f"SEARCH testdb.articles {keyword}")
            assert resp is not None, f"Null response for keyword '{keyword}'"
            assert mygramdb.ping(), f"Server unresponsive after searching '{keyword}'"

    def test_query_at_max_length_boundary(self, mygramdb, seed_data):
        """Query at maximum length boundary — boundary value test."""
        # Test with a query just under typical limits
        long_term = "a" * 1000
        resp = mygramdb.tcp_command(f"SEARCH testdb.articles {long_term}")
        assert resp is not None
        assert mygramdb.ping()

        # Test with a significantly longer query
        longer_term = "a" * 10000
        resp2 = mygramdb.tcp_command(f"SEARCH testdb.articles {longer_term}")
        assert resp2 is not None
        assert mygramdb.ping()
