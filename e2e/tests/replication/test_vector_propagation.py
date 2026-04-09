"""Test VECTOR column replication from MySQL 9.x to MygramDB.

These tests verify that tables containing VECTOR columns can be replicated
without errors. VECTOR data itself is not indexed by MygramDB (it's a
full-text search engine), but the parser must handle VECTOR columns
gracefully when they appear in binlog events.

Requires MySQL >= 9.0 (skipped on 8.x).
"""

from __future__ import annotations

import struct
import uuid

import pytest

from lib.wait import wait_until, wait_until_gte

pytestmark = [
    pytest.mark.replication,
    pytest.mark.mysql9,
]


def _vector_to_hex(floats: list[float]) -> str:
    """Encode float list to MySQL VECTOR hex literal: 0x...."""
    binary = struct.pack(f"<{len(floats)}f", *floats)
    return "0x" + binary.hex()


def _skip_if_no_vector(mysql_version: tuple[int, int]) -> None:
    if mysql_version[0] < 9:
        pytest.skip("VECTOR type requires MySQL >= 9.0")


class TestVectorPropagation:
    """Test that tables with VECTOR columns replicate correctly."""

    def test_insert_with_vector_column(
        self, mysql, mygramdb, mysql_version, setup_vector_table, seed_data
    ):
        """INSERT into table with VECTOR column should not break replication."""
        _skip_if_no_vector(mysql_version)

        marker = f"vec_ins_{uuid.uuid4().hex[:8]}"
        vec_hex = _vector_to_hex([1.0, 2.0, 3.0])

        mysql.execute(
            f"INSERT INTO vec_articles (title, content, embedding, status, enabled) "
            f"VALUES ('Vector Test', '{marker}', {vec_hex}, 1, 1)"
        )

        # Verify replication still works by checking a normal table
        check_marker = f"veccheck_{uuid.uuid4().hex[:8]}"
        mysql.insert_rows("articles", [{
            "title": "After Vector Insert",
            "content": f"Verifying replication after vector insert {check_marker}",
            "status": 1,
            "category": "tech",
            "enabled": 1,
        }])

        wait_until_gte(
            lambda: mygramdb.count("articles", check_marker),
            minimum=1,
            timeout=20,
            interval=0.5,
            description="replication works after VECTOR INSERT",
        )

    def test_update_with_vector_column(
        self, mysql, mygramdb, mysql_version, setup_vector_table, seed_data
    ):
        """UPDATE on table with VECTOR column should not break replication."""
        _skip_if_no_vector(mysql_version)

        marker = f"vec_upd_{uuid.uuid4().hex[:8]}"
        vec_hex = _vector_to_hex([0.1, 0.2, 0.3])

        mysql.execute(
            f"INSERT INTO vec_articles (title, content, embedding, status, enabled) "
            f"VALUES ('Update Target', '{marker}', {vec_hex}, 1, 1)"
        )

        # Update the vector column
        new_vec_hex = _vector_to_hex([0.4, 0.5, 0.6])
        mysql.execute(
            f"UPDATE vec_articles SET embedding = {new_vec_hex} "
            f"WHERE content = '{marker}'"
        )

        # Verify replication continues
        check_marker = f"vecupd_check_{uuid.uuid4().hex[:8]}"
        mysql.insert_rows("articles", [{
            "title": "After Vector Update",
            "content": f"Verify after vector update {check_marker}",
            "status": 1,
            "category": "tech",
            "enabled": 1,
        }])

        wait_until_gte(
            lambda: mygramdb.count("articles", check_marker),
            minimum=1,
            timeout=20,
            interval=0.5,
            description="replication works after VECTOR UPDATE",
        )

    def test_delete_with_vector_column(
        self, mysql, mygramdb, mysql_version, setup_vector_table, seed_data
    ):
        """DELETE on table with VECTOR column should not break replication."""
        _skip_if_no_vector(mysql_version)

        marker = f"vec_del_{uuid.uuid4().hex[:8]}"
        vec_hex = _vector_to_hex([9.0, 8.0, 7.0])

        mysql.execute(
            f"INSERT INTO vec_articles (title, content, embedding, status, enabled) "
            f"VALUES ('Delete Target', '{marker}', {vec_hex}, 1, 1)"
        )
        mysql.execute(f"DELETE FROM vec_articles WHERE content = '{marker}'")

        # Verify replication continues
        check_marker = f"vecdel_check_{uuid.uuid4().hex[:8]}"
        mysql.insert_rows("articles", [{
            "title": "After Vector Delete",
            "content": f"Verify after vector delete {check_marker}",
            "status": 1,
            "category": "tech",
            "enabled": 1,
        }])

        wait_until_gte(
            lambda: mygramdb.count("articles", check_marker),
            minimum=1,
            timeout=20,
            interval=0.5,
            description="replication works after VECTOR DELETE",
        )

    def test_batch_inserts_with_vector(
        self, mysql, mygramdb, mysql_version, setup_vector_table, seed_data
    ):
        """Batch INSERTs with VECTOR columns should not break replication."""
        _skip_if_no_vector(mysql_version)

        marker = f"vec_batch_{uuid.uuid4().hex[:8]}"

        for i in range(10):
            vec_hex = _vector_to_hex([float(i), float(i + 1), float(i + 2)])
            mysql.execute(
                f"INSERT INTO vec_articles (title, content, embedding, status, enabled) "
                f"VALUES ('Batch {i}', '{marker}_{i}', {vec_hex}, 1, 1)"
            )

        # Verify replication is healthy after batch
        check_marker = f"vecbatch_check_{uuid.uuid4().hex[:8]}"
        mysql.insert_rows("articles", [{
            "title": "After Vector Batch",
            "content": f"Verify after vector batch {check_marker}",
            "status": 1,
            "category": "tech",
            "enabled": 1,
        }])

        wait_until_gte(
            lambda: mygramdb.count("articles", check_marker),
            minimum=1,
            timeout=20,
            interval=0.5,
            description="replication works after VECTOR batch INSERT",
        )
