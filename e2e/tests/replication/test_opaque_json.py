"""Regression coverage for opaque values in MySQL binary JSON row events."""

from __future__ import annotations

import contextlib
import uuid

import pytest

from lib.wait import wait_until_gte

pytestmark = [pytest.mark.replication, pytest.mark.mysql_only]


class TestOpaqueJsonReplication:
    def test_opaque_json_values_do_not_stall_replication(self, mysql, mygramdb, seed_data):
        """Temporal and DECIMAL JSON members must not replay the same GTID forever."""
        marker = f"opaque_json_{uuid.uuid4().hex[:8]}"

        with contextlib.suppress(Exception):
            mysql.execute("ALTER TABLE articles DROP COLUMN opaque_payload")

        try:
            mysql.execute("ALTER TABLE articles ADD COLUMN opaque_payload JSON NULL")
            mysql.execute(
                "INSERT INTO articles "
                "(title, content, status, category, enabled, opaque_payload) VALUES ("
                f"'Opaque JSON', 'opaque JSON replication {marker}', 1, 'replication', 1, "
                "JSON_OBJECT('observed_at', NOW(6), "
                "'amount', CAST('123.45' AS DECIMAL(10, 2))))"
            )

            wait_until_gte(
                lambda: mygramdb.count("testdb.articles", marker),
                minimum=1,
                timeout=20,
                interval=0.25,
                description="opaque JSON row event to be applied",
            )
            assert "status: running" in mygramdb.replication_status().lower()
        finally:
            with contextlib.suppress(Exception):
                mysql.execute("ALTER TABLE articles DROP COLUMN opaque_payload")
            mygramdb.sync("testdb.articles", timeout=60)
