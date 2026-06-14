"""Cross-check TCP and HTTP search surfaces."""

import uuid

import pytest

from lib.wait import wait_until_gte

pytestmark = pytest.mark.search


class TestHttpParity:
    """Verify HTTP search/count stay aligned with the TCP protocol."""

    def test_search_and_count_match_tcp(self, mysql, mygramdb, seed_data):
        marker = f"http_parity_{uuid.uuid4().hex[:8]}"
        rows = [
            {
                "title": f"HTTP Parity {i}",
                "content": f"Document {i} carries {marker} parity marker",
                "status": 1 if i % 2 == 0 else 2,
                "category": "tech",
                "enabled": 1,
            }
            for i in range(6)
        ]
        mysql.insert_rows("articles", rows)

        wait_until_gte(
            lambda: mygramdb.count("testdb.articles", marker),
            minimum=6,
            timeout=10,
            interval=0.5,
            description="HTTP parity test data",
        )

        tcp = mygramdb.search("testdb.articles", marker, sort="id ASC", limit=20)
        http = mygramdb.http_search("testdb.articles", marker, sort="id ASC", limit=20)
        assert http["status"] == 200
        assert http["total"] == tcp["total"]
        assert http["ids"] == tcp["ids"]

        tcp_filtered = mygramdb.search(
            "testdb.articles", marker, filters={"status": 1}, sort="id ASC", limit=20
        )
        http_filtered = mygramdb.http_search(
            "testdb.articles", marker, filters={"status": 1}, sort="id ASC", limit=20
        )
        assert http_filtered["status"] == 200
        assert http_filtered["total"] == tcp_filtered["total"]
        assert http_filtered["ids"] == tcp_filtered["ids"]

        http_count_status, http_count = mygramdb.http_count("testdb.articles", marker)
        assert http_count_status == 200
        assert http_count == mygramdb.count("testdb.articles", marker)

    def test_score_sort_matches_tcp(self, mysql, mygramdb, seed_data):
        marker = f"http_score_{uuid.uuid4().hex[:8]}"
        mysql.insert_rows(
            "articles",
            [
                {
                    "title": "HTTP Score Dense",
                    "content": f"{marker} {marker} {marker} dense scoring document",
                    "status": 1,
                    "category": "tech",
                    "enabled": 1,
                },
                {
                    "title": "HTTP Score Sparse",
                    "content": f"{marker} sparse scoring document",
                    "status": 1,
                    "category": "tech",
                    "enabled": 1,
                },
            ],
        )

        wait_until_gte(
            lambda: mygramdb.count("testdb.articles", marker),
            minimum=2,
            timeout=10,
            interval=0.5,
            description="HTTP score parity data",
        )

        tcp = mygramdb.search("testdb.articles", marker, sort="_score DESC", limit=10)
        http = mygramdb.http_search("testdb.articles", marker, sort="_score DESC", limit=10)
        assert http["status"] == 200
        assert http["total"] == tcp["total"]
        assert http["ids"] == tcp["ids"]

    def test_http_invalid_boolean_is_client_error(self, mygramdb, seed_data):
        status, body = mygramdb.http_post(
            "/tables/testdb/articles/search", {"q": "(broken OR", "limit": 10}
        )
        assert status == 400
        assert isinstance(body, dict)
        assert "error" in body

    def test_count_rejects_search_only_options(self, mygramdb, seed_data):
        status, body = mygramdb.http_post(
            "/tables/testdb/articles/count", {"q": "test", "sort": {"column": "_score"}}
        )
        assert status == 400
        assert isinstance(body, dict)
        assert "error" in body

    def test_tcp_facet_has_e2e_coverage(self, mysql, mygramdb, seed_data):
        marker = f"facet_e2e_{uuid.uuid4().hex[:8]}"
        mysql.insert_rows(
            "articles",
            [
                {
                    "title": "Facet E2E Tech",
                    "content": f"{marker} facet coverage tech",
                    "status": 1,
                    "category": "tech",
                    "enabled": 1,
                },
                {
                    "title": "Facet E2E Science",
                    "content": f"{marker} facet coverage science",
                    "status": 1,
                    "category": "science",
                    "enabled": 1,
                },
            ],
        )

        wait_until_gte(
            lambda: mygramdb.count("testdb.articles", marker),
            minimum=2,
            timeout=10,
            interval=0.5,
            description="facet e2e data",
        )

        counts = mygramdb.facet("testdb.articles", "category", marker)
        assert counts.get("tech", 0) >= 1
        assert counts.get("science", 0) >= 1
