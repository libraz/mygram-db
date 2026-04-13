"""Test that replication events are reflected in SEARCH results (not just COUNT).

Bug report: "Replication progresses but search results don't update"
Root cause candidates:
  1. Index not updated after replication event (cache-independent)
  2. Cache not invalidated after replication event (cache-dependent)

These tests use SEARCH (not COUNT) to verify actual document retrieval,
and run both with cache disabled and enabled to isolate the root cause.
"""

from __future__ import annotations

import time
import uuid

import pytest

from lib.wait import wait_until, wait_until_gte

pytestmark = pytest.mark.replication


class TestSearchFreshnessNocache:
    """Verify search results reflect replication events with cache DISABLED.

    Failures here indicate an index update bug (not cache).
    """

    @pytest.fixture(autouse=True)
    def _disable_cache(self, mygramdb):
        """Disable cache for all tests in this class."""
        mygramdb.tcp_command("CACHE DISABLE")
        mygramdb.cache_clear()
        yield
        mygramdb.tcp_command("CACHE ENABLE")

    def test_insert_appears_in_search(self, mysql, mygramdb, seed_data):
        """INSERT a row and verify SEARCH returns it by keyword."""
        marker = f"freshins_{uuid.uuid4().hex[:8]}"
        mysql.insert_rows(
            "articles",
            [
                {
                    "title": "Freshness Test",
                    "content": f"This document contains {marker} for freshness verification",
                    "status": 1,
                    "category": "tech",
                    "enabled": 1,
                }
            ],
        )

        def _search_finds_marker():
            result = mygramdb.search("articles", marker, limit=10)
            return result["total"] >= 1 and len(result["ids"]) >= 1

        wait_until(
            _search_finds_marker,
            timeout=20,
            interval=0.5,
            description=f"SEARCH to return {marker} after INSERT",
        )

    def test_update_text_changes_search_results(self, mysql, mygramdb, seed_data):
        """UPDATE text column: old keyword disappears, new keyword appears in SEARCH."""
        old_marker = f"freshold_{uuid.uuid4().hex[:8]}"
        new_marker = f"freshnew_{uuid.uuid4().hex[:8]}"

        mysql.insert_rows(
            "articles",
            [
                {
                    "title": "Freshness Update Test",
                    "content": f"Original content with {old_marker} keyword",
                    "status": 1,
                    "category": "tech",
                    "enabled": 1,
                }
            ],
        )

        # Wait for INSERT to propagate
        wait_until(
            lambda: mygramdb.search("articles", old_marker, limit=10)["total"] >= 1,
            timeout=20,
            interval=0.5,
            description=f"SEARCH to find {old_marker} after INSERT",
        )

        # Capture the document ID
        result_before = mygramdb.search("articles", old_marker, limit=10)
        result_before["ids"][0]

        # UPDATE the text column
        mysql.update(
            "articles",
            f"content = 'Changed content with {new_marker} keyword'",
            f"content LIKE '%{old_marker}%'",
        )

        # New keyword should appear in SEARCH
        wait_until(
            lambda: mygramdb.search("articles", new_marker, limit=10)["total"] >= 1,
            timeout=20,
            interval=0.5,
            description=f"SEARCH to find {new_marker} after UPDATE",
        )

        # Old keyword should disappear from SEARCH
        result_old = mygramdb.search("articles", old_marker, limit=10)
        assert result_old["total"] == 0, (
            f"Old keyword '{old_marker}' should no longer match after UPDATE, "
            f"but SEARCH still returns {result_old['total']} results"
        )

    def test_delete_removes_from_search(self, mysql, mygramdb, seed_data):
        """DELETE a row and verify SEARCH no longer returns it."""
        marker = f"freshdel_{uuid.uuid4().hex[:8]}"
        mysql.insert_rows(
            "articles",
            [
                {
                    "title": "Freshness Delete Test",
                    "content": f"Content with {marker} to be deleted",
                    "status": 1,
                    "category": "tech",
                    "enabled": 1,
                }
            ],
        )

        wait_until(
            lambda: mygramdb.search("articles", marker, limit=10)["total"] >= 1,
            timeout=20,
            interval=0.5,
            description=f"SEARCH to find {marker} after INSERT",
        )

        # DELETE the row
        mysql.delete("articles", f"content LIKE '%{marker}%'")

        # Wait for deletion to propagate
        wait_until(
            lambda: mygramdb.search("articles", marker, limit=10)["total"] == 0,
            timeout=20,
            interval=0.5,
            description=f"SEARCH to return 0 results for {marker} after DELETE",
        )

    def test_bulk_insert_all_searchable(self, mysql, mygramdb, seed_data):
        """Bulk INSERT 20 rows, verify all are individually findable via SEARCH."""
        batch_marker = f"freshbulk_{uuid.uuid4().hex[:8]}"
        row_markers = []
        rows = []
        for i in range(20):
            row_marker = f"{batch_marker}_r{i}"
            row_markers.append(row_marker)
            rows.append(
                {
                    "title": f"Bulk {i}",
                    "content": f"Bulk content with {batch_marker} and unique {row_marker}",
                    "status": 1,
                    "category": "tech",
                    "enabled": 1,
                }
            )
        mysql.insert_rows("articles", rows)

        # Wait for all rows via batch marker
        wait_until_gte(
            lambda: mygramdb.search("articles", batch_marker, limit=100)["total"],
            minimum=20,
            timeout=30,
            interval=1,
            description=f"SEARCH to find all 20 {batch_marker} rows",
        )

        # Spot-check individual rows are findable
        for idx in [0, 9, 19]:
            result = mygramdb.search("articles", row_markers[idx], limit=10)
            assert result["total"] >= 1, (
                f"Row {idx} with marker '{row_markers[idx]}' not found in SEARCH"
            )


class TestSearchFreshnessWithCache:
    """Verify search results reflect replication events with cache ENABLED.

    Failures here (when TestSearchFreshnessNocache passes) indicate
    a cache invalidation bug.
    """

    @pytest.fixture(autouse=True)
    def _enable_cache(self, mygramdb):
        """Ensure cache is enabled and cleared for each test."""
        mygramdb.tcp_command("CACHE ENABLE")
        mygramdb.cache_clear()
        yield

    def test_insert_appears_in_search_with_cache(self, mysql, mygramdb, seed_data):
        """INSERT after a cached SEARCH — new doc must appear."""
        marker = f"cachefresh_{uuid.uuid4().hex[:8]}"

        # Prime the cache with an empty result
        result_before = mygramdb.search("articles", marker, limit=10)
        assert result_before["total"] == 0

        # INSERT a matching row
        mysql.insert_rows(
            "articles",
            [
                {
                    "title": "Cache Freshness",
                    "content": f"Document with {marker} for cache test",
                    "status": 1,
                    "category": "tech",
                    "enabled": 1,
                }
            ],
        )

        # SEARCH should eventually return the new doc (cache must be invalidated)
        wait_until(
            lambda: mygramdb.search("articles", marker, limit=10)["total"] >= 1,
            timeout=20,
            interval=0.5,
            description=f"cached SEARCH to find {marker} after INSERT",
        )

    def test_update_invalidates_cached_search(self, mysql, mygramdb, seed_data):
        """UPDATE text after cached SEARCH — old keyword must vanish, new must appear."""
        old_marker = f"cacheold_{uuid.uuid4().hex[:8]}"
        new_marker = f"cachenew_{uuid.uuid4().hex[:8]}"

        mysql.insert_rows(
            "articles",
            [
                {
                    "title": "Cache Update Test",
                    "content": f"Content with {old_marker} for cache update test",
                    "status": 1,
                    "category": "tech",
                    "enabled": 1,
                }
            ],
        )

        # Wait and cache the old result
        wait_until(
            lambda: mygramdb.search("articles", old_marker, limit=10)["total"] >= 1,
            timeout=20,
            interval=0.5,
            description=f"SEARCH to find {old_marker}",
        )

        # Issue a second SEARCH to ensure it's cached
        cached_result = mygramdb.search("articles", old_marker, limit=10)
        assert cached_result["total"] >= 1

        # UPDATE
        mysql.update(
            "articles",
            f"content = 'Updated with {new_marker} instead'",
            f"content LIKE '%{old_marker}%'",
        )

        # New keyword must appear (cache invalidated)
        wait_until(
            lambda: mygramdb.search("articles", new_marker, limit=10)["total"] >= 1,
            timeout=20,
            interval=0.5,
            description=f"cached SEARCH to find {new_marker} after UPDATE",
        )

        # Old keyword must vanish (cache invalidated)
        wait_until(
            lambda: mygramdb.search("articles", old_marker, limit=10)["total"] == 0,
            timeout=20,
            interval=0.5,
            description=f"cached SEARCH to drop {old_marker} after UPDATE",
        )

    def test_delete_invalidates_cached_search(self, mysql, mygramdb, seed_data):
        """DELETE after cached SEARCH — deleted doc must vanish."""
        marker = f"cachedel_{uuid.uuid4().hex[:8]}"

        mysql.insert_rows(
            "articles",
            [
                {
                    "title": "Cache Delete Test",
                    "content": f"Content with {marker} to be deleted",
                    "status": 1,
                    "category": "tech",
                    "enabled": 1,
                }
            ],
        )

        wait_until(
            lambda: mygramdb.search("articles", marker, limit=10)["total"] >= 1,
            timeout=20,
            interval=0.5,
            description=f"SEARCH to find {marker}",
        )

        # Cache it
        mygramdb.search("articles", marker, limit=10)

        # DELETE
        mysql.delete("articles", f"content LIKE '%{marker}%'")

        # Must vanish from cached SEARCH
        wait_until(
            lambda: mygramdb.search("articles", marker, limit=10)["total"] == 0,
            timeout=20,
            interval=0.5,
            description=f"cached SEARCH to drop {marker} after DELETE",
        )

    def test_filter_update_reflected_in_cached_search(self, mysql, mygramdb, seed_data):
        """UPDATE filter column after cached filtered SEARCH — results must change."""
        marker = f"cachefilt_{uuid.uuid4().hex[:8]}"

        mysql.insert_rows(
            "articles",
            [
                {
                    "title": "Cache Filter Test",
                    "content": f"Content with {marker} for filter cache test",
                    "status": 1,
                    "category": "tech",
                    "enabled": 1,
                }
            ],
        )

        wait_until(
            lambda: mygramdb.search("articles", marker, limit=10)["total"] >= 1,
            timeout=20,
            interval=0.5,
            description=f"SEARCH to find {marker}",
        )

        # Cache a filtered search
        filtered = mygramdb.search("articles", marker, filters={"status": 1}, limit=10)
        assert filtered["total"] >= 1

        # UPDATE the filter column value
        mysql.update("articles", "status = 99", f"content LIKE '%{marker}%'")

        # Filtered search with status=1 should now return 0
        wait_until(
            lambda: (
                mygramdb.search("articles", marker, filters={"status": 1}, limit=10)["total"] == 0
            ),
            timeout=20,
            interval=0.5,
            description=f"cached filtered SEARCH to drop {marker} after status UPDATE",
        )

        # Filtered search with status=99 should return 1
        result_new = mygramdb.search("articles", marker, filters={"status": 99}, limit=10)
        assert result_new["total"] >= 1, (
            f"Expected status=99 filter to match after UPDATE, got {result_new['total']}"
        )


class TestSearchFreshnessCacheToggle:
    """Verify search results are correct when cache is toggled ON/OFF mid-flight.

    Key risk: Disable() does not clear cache entries. If invalidation
    events are missed while cache is disabled, stale entries may be
    served when cache is re-enabled.
    """

    def test_disable_during_replication_then_reenable(self, mysql, mygramdb, seed_data):
        """Cache ON → cache result → DISABLE → replication → ENABLE → must see fresh data.

        Regression: Disable() stops invalidation queue, so replication events
        during the OFF window are never processed. Re-enabling may serve stale entries.
        """
        marker = f"toggle1_{uuid.uuid4().hex[:8]}"
        new_marker = f"toggle1new_{uuid.uuid4().hex[:8]}"

        # 1. Cache ON, insert and cache a result
        mygramdb.tcp_command("CACHE ENABLE")
        mygramdb.cache_clear()

        mysql.insert_rows(
            "articles",
            [
                {
                    "title": "Toggle Test",
                    "content": f"Content with {marker} for toggle test",
                    "status": 1,
                    "category": "tech",
                    "enabled": 1,
                }
            ],
        )

        wait_until(
            lambda: mygramdb.search("articles", marker, limit=10)["total"] >= 1,
            timeout=20,
            interval=0.5,
            description=f"SEARCH to find {marker}",
        )

        # Cache the result
        cached = mygramdb.search("articles", marker, limit=10)
        assert cached["total"] >= 1

        # 2. Disable cache
        mygramdb.tcp_command("CACHE DISABLE")

        # 3. Replication event while cache is off
        mysql.update(
            "articles",
            f"content = 'Updated to {new_marker} while cache off'",
            f"content LIKE '%{marker}%'",
        )

        # Wait for replication (search directly, no cache)
        wait_until(
            lambda: mygramdb.search("articles", new_marker, limit=10)["total"] >= 1,
            timeout=20,
            interval=0.5,
            description=f"SEARCH to find {new_marker} with cache off",
        )

        # 4. Re-enable cache
        mygramdb.tcp_command("CACHE ENABLE")

        # 5. Search must return fresh data, not stale cached entry
        result_new = mygramdb.search("articles", new_marker, limit=10)
        assert result_new["total"] >= 1, (
            f"After cache re-enable, SEARCH should find '{new_marker}' "
            f"but got total={result_new['total']}"
        )

        result_old = mygramdb.search("articles", marker, limit=10)
        assert result_old["total"] == 0, (
            f"After cache re-enable, old keyword '{marker}' should not match "
            f"but SEARCH returned total={result_old['total']} (stale cache?)"
        )

    def test_insert_while_cache_off_then_reenable(self, mysql, mygramdb, seed_data):
        """Cache ON → DISABLE → INSERT → ENABLE → new doc must appear in SEARCH.

        If the cache still has an empty-result entry from before the INSERT,
        re-enabling might serve the stale empty result.
        """
        marker = f"toggle2_{uuid.uuid4().hex[:8]}"

        # 1. Cache ON, prime with empty result
        mygramdb.tcp_command("CACHE ENABLE")
        mygramdb.cache_clear()

        empty_result = mygramdb.search("articles", marker, limit=10)
        assert empty_result["total"] == 0

        # 2. Disable cache
        mygramdb.tcp_command("CACHE DISABLE")

        # 3. INSERT while cache off
        mysql.insert_rows(
            "articles",
            [
                {
                    "title": "Toggle Insert Test",
                    "content": f"New document with {marker} inserted while cache off",
                    "status": 1,
                    "category": "tech",
                    "enabled": 1,
                }
            ],
        )

        wait_until(
            lambda: mygramdb.search("articles", marker, limit=10)["total"] >= 1,
            timeout=20,
            interval=0.5,
            description=f"SEARCH to find {marker} with cache off",
        )

        # 4. Re-enable cache
        mygramdb.tcp_command("CACHE ENABLE")

        # 5. Must find the new document, not return stale empty result
        result = mygramdb.search("articles", marker, limit=10)
        assert result["total"] >= 1, (
            f"After cache re-enable, SEARCH should find '{marker}' "
            f"but got total={result['total']} (stale empty cache entry?)"
        )

    def test_delete_while_cache_off_then_reenable(self, mysql, mygramdb, seed_data):
        """Cache ON → cache result → DISABLE → DELETE → ENABLE → deleted doc must vanish."""
        marker = f"toggle3_{uuid.uuid4().hex[:8]}"

        # 1. Cache ON, insert and cache
        mygramdb.tcp_command("CACHE ENABLE")
        mygramdb.cache_clear()

        mysql.insert_rows(
            "articles",
            [
                {
                    "title": "Toggle Delete Test",
                    "content": f"Content with {marker} to be deleted during cache off",
                    "status": 1,
                    "category": "tech",
                    "enabled": 1,
                }
            ],
        )

        wait_until(
            lambda: mygramdb.search("articles", marker, limit=10)["total"] >= 1,
            timeout=20,
            interval=0.5,
            description=f"SEARCH to find {marker}",
        )

        # Cache the result (total >= 1)
        cached = mygramdb.search("articles", marker, limit=10)
        assert cached["total"] >= 1

        # 2. Disable cache
        mygramdb.tcp_command("CACHE DISABLE")

        # 3. DELETE while cache off
        mysql.delete("articles", f"content LIKE '%{marker}%'")

        wait_until(
            lambda: mygramdb.search("articles", marker, limit=10)["total"] == 0,
            timeout=20,
            interval=0.5,
            description=f"SEARCH to return 0 for {marker} with cache off",
        )

        # 4. Re-enable cache
        mygramdb.tcp_command("CACHE ENABLE")

        # 5. Must return 0, not stale cached result
        result = mygramdb.search("articles", marker, limit=10)
        assert result["total"] == 0, (
            f"After cache re-enable, SEARCH for deleted '{marker}' "
            f"should return 0 but got total={result['total']} (stale cache?)"
        )

    def test_rapid_toggle_during_writes(self, mysql, mygramdb, seed_data):
        """Toggle cache ON/OFF rapidly while data changes. Final state must be correct."""
        marker = f"toggle4_{uuid.uuid4().hex[:8]}"

        mygramdb.tcp_command("CACHE ENABLE")
        mygramdb.cache_clear()

        # Insert 5 documents
        rows = [
            {
                "title": f"Rapid Toggle {i}",
                "content": f"Content with {marker} for rapid toggle test row {i}",
                "status": 1,
                "category": "tech",
                "enabled": 1,
            }
            for i in range(5)
        ]
        mysql.insert_rows("articles", rows)

        wait_until_gte(
            lambda: mygramdb.search("articles", marker, limit=100)["total"],
            minimum=5,
            timeout=20,
            interval=0.5,
            description=f"SEARCH to find all 5 {marker} rows",
        )

        # Rapidly toggle cache while deleting
        for i in range(3):
            mygramdb.tcp_command("CACHE DISABLE")
            mysql.delete("articles", f"content LIKE '%{marker}%row {i}%'")
            time.sleep(0.5)
            mygramdb.tcp_command("CACHE ENABLE")
            time.sleep(0.5)

        # Wait for replication to settle
        time.sleep(3)

        # Final state: 2 rows remaining (deleted 0, 1, 2)
        result = mygramdb.search("articles", marker, limit=100)
        assert result["total"] == 2, (
            f"After rapid toggle + deletes, expected 2 remaining rows "
            f"but SEARCH returned total={result['total']}"
        )
