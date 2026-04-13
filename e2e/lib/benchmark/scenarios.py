"""Benchmark scenario definitions and command builders.

Loads scenario configurations from JSON and builds MygramDB TCP commands
or MySQL queries for benchmark execution.
"""

from __future__ import annotations

import json
from dataclasses import dataclass


@dataclass
class Scenario:
    """A single benchmark scenario definition."""

    name: str
    data_count: int
    queries: list[str]
    query_type: str
    filters: dict[str, str] | None = None
    limit: int = 100
    offset: int = 0
    sort: str | None = None
    mixed_content: bool = False


def build_mygramdb_command(scenario: Scenario, query: str) -> str:
    """Build a MygramDB TCP command string for the given scenario and query.

    Args:
        scenario: The benchmark scenario definition.
        query: The search term to use.

    Returns:
        A formatted TCP command string.
    """
    if scenario.query_type == "connection_cost":
        return "INFO"

    if scenario.query_type == "count":
        parts = ["COUNT", "articles", query]
        if scenario.filters:
            for key, val in scenario.filters.items():
                parts.extend(["FILTER", key, "=", val])
        return " ".join(parts)

    # search
    parts = ["SEARCH", "articles", query]
    if scenario.filters:
        for key, val in scenario.filters.items():
            parts.extend(["FILTER", key, "=", val])
    if scenario.sort:
        parts.extend(["SORT", scenario.sort])
    if scenario.offset > 0:
        parts.extend(["LIMIT", f"{scenario.offset},{scenario.limit}"])
    else:
        parts.extend(["LIMIT", str(scenario.limit)])
    return " ".join(parts)


def build_mysql_query(scenario: Scenario, query: str) -> tuple[str, tuple]:
    """Build a MySQL query and parameters for the given scenario and query.

    Args:
        scenario: The benchmark scenario definition.
        query: The search term to use.

    Returns:
        A tuple of (sql_string, params_tuple).
    """
    if scenario.query_type == "connection_cost":
        return ("SELECT 1", ())

    params: list[str] = [query]

    if scenario.query_type == "count":
        sql = (
            "SELECT COUNT(*) as cnt FROM articles"
            " WHERE MATCH(content) AGAINST(%s IN BOOLEAN MODE)"
            " AND enabled = 1 AND deleted_at IS NULL"
        )
        if scenario.filters:
            for key, val in scenario.filters.items():
                sql += f" AND {key} = %s"
                params.append(val)
        return (sql, tuple(params))

    # search
    sql = (
        "SELECT id FROM articles"
        " WHERE MATCH(content) AGAINST(%s IN BOOLEAN MODE)"
        " AND enabled = 1 AND deleted_at IS NULL"
    )
    if scenario.filters:
        for key, val in scenario.filters.items():
            sql += f" AND {key} = %s"
            params.append(val)
    if scenario.sort:
        sql += f" ORDER BY {scenario.sort}"
    sql += f" LIMIT {scenario.limit}"
    return (sql, tuple(params))


def build_mygramdb_commands(scenario: Scenario) -> list[str]:
    """Build MygramDB commands for all queries in a scenario."""
    return [build_mygramdb_command(scenario, q) for q in scenario.queries]


def build_mysql_commands(scenario: Scenario) -> list[str]:
    """Build MySQL commands (pipe-encoded) for all queries in a scenario.

    The MysqlConnection.command_timed() expects pipe-separated format:
    sql|param1|param2|...
    """
    commands: list[str] = []
    for q in scenario.queries:
        sql, params = build_mysql_query(scenario, q)
        parts = [sql, *(str(p) for p in params)]
        commands.append("|".join(parts))
    return commands


def load_scenarios(path: str) -> list[Scenario]:
    """Load benchmark scenarios from a JSON file.

    Args:
        path: Path to the JSON file containing scenario definitions.

    Returns:
        A list of Scenario objects.
    """
    with open(path, encoding="utf-8") as f:
        data = json.load(f)

    scenarios: list[Scenario] = []
    for entry in data["scenarios"]:
        scenarios.append(
            Scenario(
                name=entry["name"],
                data_count=entry["data_count"],
                queries=entry["queries"],
                query_type=entry["query_type"],
                filters=entry.get("filters"),
                limit=entry.get("limit", 100),
                offset=entry.get("offset", 0),
                sort=entry.get("sort"),
                mixed_content=entry.get("mixed_content", False),
            )
        )
    return scenarios


def get_concurrency_levels(mode: str) -> list[int]:
    """Return concurrency levels for the given benchmark mode.

    Args:
        mode: One of "quick", "standard", or "saturation".

    Returns:
        A list of concurrency level integers.

    Raises:
        ValueError: If mode is not recognized.
    """
    levels = {
        "quick": [1, 4, 16],
        "standard": [1, 2, 4, 8, 16, 32, 64],
        "saturation": [1, 2, 4, 8, 16, 32, 64, 128, 256, 512],
    }
    if mode not in levels:
        raise ValueError(f"Unknown concurrency mode: {mode!r} (expected: {', '.join(levels)})")
    return levels[mode]
