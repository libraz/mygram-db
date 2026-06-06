"""MySQL failover E2E coverage for GTID validation and resume."""

from __future__ import annotations

import os
import socket
import uuid
from typing import Any

import pytest

from lib.mysql_client import MysqlClient
from lib.wait import wait_until, wait_until_gte

pytestmark = pytest.mark.resilience

SECONDARY_PORT = 23307


def _secondary_mysql() -> MysqlClient:
    return MysqlClient(
        host="127.0.0.1",
        port=int(os.environ.get("MYSQL_SECONDARY_PORT", str(SECONDARY_PORT))),
        user="root",
        password=os.environ.get("MYSQL_TEST_PASSWORD", "test_root_password"),
        database="testdb",
    )


def _single_value(mysql: MysqlClient, sql: str, key: str) -> str:
    rows = mysql.execute(sql)
    assert rows, f"query returned no rows: {sql}"
    return str(rows[0][key])


def _replication_status_value(mygramdb: Any, key: str) -> str:
    response = _tcp_command_until(mygramdb, "REPLICATION STATUS", b"END")
    assert response, "REPLICATION STATUS returned no response"
    for line in response.splitlines():
        if line.startswith(f"{key}:"):
            return line.split(":", 1)[1].strip()
    raise AssertionError(f"{key} not found in REPLICATION STATUS:\n{response}")


def _tcp_command_until(mygramdb: Any, command: str, marker: bytes, timeout: float = 10.0) -> str:
    with socket.create_connection((mygramdb.host, mygramdb.tcp_port), timeout=timeout) as sock:
        sock.settimeout(timeout)
        sock.sendall((command + "\r\n").encode("utf-8"))
        data = b""
        while marker not in data:
            chunk = sock.recv(65536)
            if not chunk:
                break
            data += chunk
        return data.decode("utf-8", errors="replace").strip()


def _quote_sql(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "''")


class TestMySQLFailover:
    def test_reconnect_to_secondary_preserves_gtid_and_replicates_new_events(
        self, mysql: MysqlClient, mygramdb: Any
    ) -> None:
        secondary = _secondary_mysql()
        wait_until(
            lambda: secondary.ping(),
            timeout=60,
            interval=2,
            description="secondary MySQL to be ready",
        )

        primary_uuid = _single_value(mysql, "SELECT @@GLOBAL.server_uuid AS uuid", "uuid")
        secondary_uuid = _single_value(secondary, "SELECT @@GLOBAL.server_uuid AS uuid", "uuid")
        assert primary_uuid != secondary_uuid

        primary_marker = f"failover_primary_{uuid.uuid4().hex[:8]}"
        mysql.insert_rows(
            "articles",
            [
                {
                    "title": "Failover Primary",
                    "content": f"{primary_marker} replicated before failover",
                    "status": 1,
                    "category": "failover",
                    "enabled": 1,
                }
            ],
        )

        wait_until_gte(
            lambda: mygramdb.count("articles", primary_marker),
            minimum=1,
            timeout=15,
            interval=0.5,
            description="primary event to replicate before failover",
        )

        current_gtid = _replication_status_value(mygramdb, "current_gtid")
        assert primary_uuid in current_gtid

        # The secondary is an independent MySQL instance. Seed equivalent data
        # and add the primary GTID set to gtid_purged so MygramDB can validate
        # that its processed position is safe on the failover target.
        secondary.insert_rows(
            "articles",
            [
                {
                    "id": 1,
                    "title": "Failover Primary",
                    "content": f"{primary_marker} replicated before failover",
                    "status": 1,
                    "category": "failover",
                    "enabled": 1,
                }
            ],
        )
        secondary.execute(f"SET @@GLOBAL.gtid_purged = '+{_quote_sql(current_gtid)}'")

        set_result = mygramdb.tcp_command(f"SET mysql.port = {SECONDARY_PORT}", timeout=30)
        assert set_result is not None and (
            set_result.startswith("OK") or set_result.startswith("+OK")
        ), set_result

        wait_until(
            lambda: _replication_status_value(mygramdb, "status") == "running",
            timeout=30,
            interval=1,
            description="replication to restart after failover",
        )

        secondary_marker = f"failover_secondary_{uuid.uuid4().hex[:8]}"
        secondary.insert_rows(
            "articles",
            [
                {
                    "title": "Failover Secondary",
                    "content": f"{secondary_marker} replicated after failover",
                    "status": 1,
                    "category": "failover",
                    "enabled": 1,
                }
            ],
        )

        wait_until_gte(
            lambda: mygramdb.count("articles", secondary_marker),
            minimum=1,
            timeout=20,
            interval=0.5,
            description="secondary event to replicate after failover",
        )

        final_gtid = _replication_status_value(mygramdb, "current_gtid")
        assert primary_uuid in final_gtid
        assert secondary_uuid in final_gtid
        assert mygramdb.count("articles", primary_marker) >= 1
