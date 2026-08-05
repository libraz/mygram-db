"""Verify startup dump restoration and replication resume from the dump GTID."""

from __future__ import annotations

import os
import signal
import socket
import subprocess
import uuid
from pathlib import Path

import pytest

from lib.mygramdb_client import MygramdbClient
from lib.wait import wait_until

pytestmark = pytest.mark.persistence

E2E_ROOT = Path(__file__).resolve().parents[2]
PROJECT_ROOT = E2E_ROOT.parent
MYGRAMDB_BINARY = Path(os.environ.get("MYGRAMDB_BINARY", PROJECT_ROOT / "build/bin/mygramdb"))
SOURCE_CONFIG = Path(os.environ.get("MYGRAMDB_CONFIG", E2E_ROOT / "docker/mygramdb-test.yaml"))
MYSQL_PORT = int(os.environ.get("MYSQL_PORT", "23306"))
DUMP_DIR = Path(os.environ.get("MYGRAMDB_DUMP_DIR", E2E_ROOT / "results/dumps"))


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _dedicated_config(
    tcp_port: int,
    http_port: int,
    *,
    load_on_startup: bool,
    server_id: int,
    dump_dir: Path = DUMP_DIR,
) -> str:
    """Rewrite only endpoint/persistence values while preserving the table schema."""
    lines = SOURCE_CONFIG.read_text(encoding="utf-8").splitlines()
    output: list[str] = []
    section = ""
    api_transport = ""
    inserted_load_keys = False

    for line in lines:
        if line and not line.startswith(" ") and line.endswith(":"):
            section = line[:-1]
            api_transport = ""
        elif (
            section == "api"
            and line.startswith("  ")
            and not line.startswith("    ")
            and line.endswith(":")
        ):
            api_transport = line.strip()[:-1]

        stripped = line.strip()
        if section == "mysql" and stripped.startswith("port:"):
            line = f"  port: {MYSQL_PORT}"
        elif section == "replication" and stripped.startswith("server_id:"):
            line = f"  server_id: {server_id}"
        elif section == "dump" and stripped.startswith("dir:"):
            line = f'  dir: "{dump_dir.resolve()}"'
        elif section == "api" and api_transport == "tcp" and stripped.startswith("port:"):
            line = f"    port: {tcp_port}"
        elif section == "api" and api_transport == "http" and stripped.startswith("port:"):
            line = f"    port: {http_port}"

        output.append(line)
        if line == "dump:" and not inserted_load_keys:
            output.extend(
                [
                    '  default_filename: "mygramdb.dmp"',
                    f"  load_on_startup: {str(load_on_startup).lower()}",
                ]
            )
            inserted_load_keys = True

    assert inserted_load_keys
    return "\n".join(output) + "\n"


def _wait_for_dump_save(client: MygramdbClient) -> None:
    def completed() -> bool:
        status = client.tcp_command_multiline("DUMP STATUS", timeout=5.0, terminator=b"END\r\n")
        if status is not None and "status: FAILED" in status:
            raise AssertionError(f"DUMP SAVE failed:\n{status}")
        return bool(
            status and "status: COMPLETED" in status and "save_in_progress: false" in status
        )

    wait_until(completed, timeout=60, interval=0.25, description="startup-restore dump save")


@pytest.mark.timeout(180)
def test_manual_dump_load_marks_http_and_tcp_ready(tmp_path: Path, mygramdb, seed_data) -> None:
    """A complete manual restore changes data initialization from false to true."""
    assert mygramdb.dump_save(), "DUMP SAVE should start"
    _wait_for_dump_save(mygramdb)

    tcp_port = _free_port()
    http_port = _free_port()
    config_path = tmp_path / "manual-restore.yaml"
    config_path.write_text(
        _dedicated_config(tcp_port, http_port, load_on_startup=False, server_id=199997),
        encoding="utf-8",
    )
    log_path = tmp_path / "manual-restore.log"
    process_env = os.environ.copy()
    for name in (
        "MYGRAM_MYSQL_HOST",
        "MYGRAM_MYSQL_PORT",
        "MYGRAM_MYSQL_USER",
        "MYGRAM_MYSQL_PASSWORD",
        "MYGRAM_MYSQL_DATABASE",
    ):
        process_env.pop(name, None)

    with log_path.open("w", encoding="utf-8") as log_file:
        process = subprocess.Popen(
            [str(MYGRAMDB_BINARY), "-c", str(config_path)],
            stdout=log_file,
            stderr=subprocess.STDOUT,
            cwd=PROJECT_ROOT,
            env=process_env,
        )
        client = MygramdbClient("127.0.0.1", tcp_port=tcp_port, http_port=http_port)
        try:
            wait_until(client.ping, timeout=90, interval=0.5, description="manual-restore server")
            assert not client.health_ready()
            before = client.tcp_command_multiline("INFO", timeout=5.0, terminator=b"END\r\n")
            assert before is not None and "data_initialized: false" in before
            assert "readiness: not_ready" in before

            assert client.dump_load("mygramdb.dmp"), "manual DUMP LOAD should succeed"
            wait_until(
                client.health_ready, timeout=30, interval=0.25, description="manual dump readiness"
            )
            after = client.tcp_command_multiline("INFO", timeout=5.0, terminator=b"END\r\n")
            assert after is not None and "data_initialized: true" in after
            assert "readiness: ready" in after
        finally:
            if process.poll() is None:
                process.send_signal(signal.SIGTERM)
                try:
                    process.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)


@pytest.mark.timeout(180)
def test_missing_startup_dump_falls_back_to_mysql_snapshot(tmp_path: Path, seed_data) -> None:
    """A missing configured startup dump performs one normal initial MySQL load."""
    tcp_port = _free_port()
    http_port = _free_port()
    empty_dump_dir = tmp_path / "empty-dumps"
    empty_dump_dir.mkdir()
    config_path = tmp_path / "startup-fallback.yaml"
    config_path.write_text(
        _dedicated_config(
            tcp_port,
            http_port,
            load_on_startup=True,
            server_id=199996,
            dump_dir=empty_dump_dir,
        ),
        encoding="utf-8",
    )
    log_path = tmp_path / "startup-fallback.log"
    process_env = os.environ.copy()
    for name in (
        "MYGRAM_MYSQL_HOST",
        "MYGRAM_MYSQL_PORT",
        "MYGRAM_MYSQL_USER",
        "MYGRAM_MYSQL_PASSWORD",
        "MYGRAM_MYSQL_DATABASE",
    ):
        process_env.pop(name, None)

    with log_path.open("w", encoding="utf-8") as log_file:
        process = subprocess.Popen(
            [str(MYGRAMDB_BINARY), "-c", str(config_path)],
            stdout=log_file,
            stderr=subprocess.STDOUT,
            cwd=PROJECT_ROOT,
            env=process_env,
        )
        client = MygramdbClient("127.0.0.1", tcp_port=tcp_port, http_port=http_port)
        try:
            wait_until(
                client.ping, timeout=90, interval=0.5, description="fallback snapshot server"
            )
            wait_until(
                client.health_ready,
                timeout=90,
                interval=0.5,
                description="fallback snapshot readiness",
            )
            info = client.tcp_command_multiline("INFO", timeout=5.0, terminator=b"END\r\n")
            assert info is not None and "data_initialized: true" in info
        finally:
            if process.poll() is None:
                process.send_signal(signal.SIGTERM)
                try:
                    process.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)

    log_text = log_path.read_text(encoding="utf-8")
    assert '"event":"startup_dump_load_failed"' in log_text
    assert '"action":"fallback_to_mysql_snapshot"' in log_text
    assert '"event":"initial_load_completed"' in log_text


@pytest.mark.timeout(180)
def test_startup_dump_restore_resumes_replication_from_dump_gtid(
    tmp_path: Path, mysql, mygramdb, seed_data
) -> None:
    """A restored process skips MySQL snapshot loading and catches up after its dump GTID."""
    assert mygramdb.dump_save(), "DUMP SAVE should start"
    _wait_for_dump_save(mygramdb)
    dump_path = DUMP_DIR / "mygramdb.dmp"
    assert dump_path.is_file()

    marker = f"startup_restore_{uuid.uuid4().hex}"
    mysql.insert_rows(
        "articles",
        [
            {
                "title": "Startup Restore Catch-up",
                "content": marker,
                "status": 1,
                "category": "persistence",
                "enabled": 1,
            }
        ],
    )

    tcp_port = _free_port()
    http_port = _free_port()
    config_path = tmp_path / "startup-restore.yaml"
    log_path = tmp_path / "startup-restore.log"
    config_path.write_text(
        _dedicated_config(tcp_port, http_port, load_on_startup=True, server_id=199998),
        encoding="utf-8",
    )

    process_env = os.environ.copy()
    for name in (
        "MYGRAM_MYSQL_HOST",
        "MYGRAM_MYSQL_PORT",
        "MYGRAM_MYSQL_USER",
        "MYGRAM_MYSQL_PASSWORD",
        "MYGRAM_MYSQL_DATABASE",
    ):
        process_env.pop(name, None)

    with log_path.open("w", encoding="utf-8") as log_file:
        process = subprocess.Popen(
            [str(MYGRAMDB_BINARY), "-c", str(config_path)],
            stdout=log_file,
            stderr=subprocess.STDOUT,
            cwd=PROJECT_ROOT,
            env=process_env,
        )
        client = MygramdbClient("127.0.0.1", tcp_port=tcp_port, http_port=http_port)
        try:
            wait_until(client.ping, timeout=90, interval=0.5, description="startup-restored server")
            wait_until(
                client.health_ready,
                timeout=90,
                interval=0.5,
                description="startup restore readiness",
            )
            wait_until(
                lambda: client.count("testdb.articles", marker) == 1,
                timeout=30,
                interval=0.25,
                description="replication catch-up from startup dump GTID",
            )

            info = client.tcp_command_multiline("INFO", timeout=5.0, terminator=b"END\r\n")
            assert info is not None and "data_initialized: true" in info
            assert "readiness: ready" in info
        finally:
            if process.poll() is None:
                process.send_signal(signal.SIGTERM)
                try:
                    process.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)

    log_text = log_path.read_text(encoding="utf-8")
    assert '"event":"startup_dump_load_completed"' in log_text
    assert '"event":"initial_load_completed"' not in log_text

    mysql.execute("DELETE FROM articles WHERE content = %s", (marker,))
    wait_until(
        lambda: mygramdb.count("testdb.articles", marker) == 0,
        timeout=30,
        interval=0.25,
        description="startup restore marker cleanup",
    )
