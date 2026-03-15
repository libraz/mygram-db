"""MySQL client for e2e tests using mysql-connector-python."""

from __future__ import annotations

from typing import Any

import mysql.connector


class MysqlClient:
    """MySQL client that connects directly to the test database."""

    def __init__(
        self,
        host: str,
        port: int = 3306,
        user: str = "root",
        password: str = "test_root_password",
        database: str = "testdb",
    ) -> None:
        self.config = {
            "host": host,
            "port": port,
            "user": user,
            "password": password,
            "database": database,
            "charset": "utf8mb4",
            "use_unicode": True,
            "autocommit": True,
        }

    def _connect(self) -> mysql.connector.MySQLConnection:
        return mysql.connector.connect(**self.config)

    def ping(self) -> bool:
        """Check if MySQL is reachable."""
        try:
            conn = self._connect()
            conn.ping(reconnect=False)
            conn.close()
            return True
        except Exception:
            return False

    def execute(self, sql: str, params: tuple | None = None) -> list[dict[str, Any]]:
        """Execute SQL and return results as list of dicts."""
        conn = self._connect()
        try:
            cursor = conn.cursor(dictionary=True)
            cursor.execute(sql, params)
            if cursor.description:
                results = cursor.fetchall()
            else:
                results = []
            cursor.close()
            return results
        finally:
            conn.close()

    def execute_many(self, sql: str, data: list[tuple]) -> int:
        """Execute SQL with multiple parameter sets. Returns affected row count."""
        conn = self._connect()
        try:
            cursor = conn.cursor()
            cursor.executemany(sql, data)
            affected = cursor.rowcount
            cursor.close()
            return affected
        finally:
            conn.close()

    def insert_rows(self, table: str, rows: list[dict[str, Any]]) -> int:
        """Insert multiple rows into a table. Returns number of rows inserted."""
        if not rows:
            return 0
        columns = list(rows[0].keys())
        placeholders = ", ".join(["%s"] * len(columns))
        col_names = ", ".join(columns)
        sql = f"INSERT INTO {table} ({col_names}) VALUES ({placeholders})"
        data = [tuple(row[col] for col in columns) for row in rows]
        return self.execute_many(sql, data)

    def count(self, table: str, where: str | None = None) -> int:
        """Count rows in a table."""
        sql = f"SELECT COUNT(*) as cnt FROM {table}"
        if where:
            sql += f" WHERE {where}"
        result = self.execute(sql)
        return int(result[0]["cnt"])

    def truncate(self, table: str) -> None:
        """Truncate a table."""
        self.execute(f"TRUNCATE TABLE {table}")

    def delete(self, table: str, where: str) -> int:
        """Delete rows from a table."""
        conn = self._connect()
        try:
            cursor = conn.cursor()
            cursor.execute(f"DELETE FROM {table} WHERE {where}")
            affected = cursor.rowcount
            cursor.close()
            return affected
        finally:
            conn.close()

    def update(self, table: str, set_clause: str, where: str) -> int:
        """Update rows in a table."""
        conn = self._connect()
        try:
            cursor = conn.cursor()
            cursor.execute(f"UPDATE {table} SET {set_clause} WHERE {where}")
            affected = cursor.rowcount
            cursor.close()
            return affected
        finally:
            conn.close()

    def fulltext_search(
        self, table: str, column: str, query: str, where: str | None = None
    ) -> list[dict[str, Any]]:
        """Search using MySQL FULLTEXT index."""
        match = f"MATCH({column}) AGAINST(%s IN BOOLEAN MODE)"
        sql = f"SELECT * FROM {table} WHERE {match}"
        if where:
            sql += f" AND {where}"
        return self.execute(sql, (query,))
