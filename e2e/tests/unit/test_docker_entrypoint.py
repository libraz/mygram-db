"""Unit tests for the container entrypoint's configuration generation.

The script is executed directly with /bin/sh, so these need no image and no
running service. Every run is pointed at a temporary directory and ends in
/usr/bin/true instead of the server binary.
"""

from __future__ import annotations

import os
import stat
import subprocess
from pathlib import Path

import pytest

ENTRYPOINT = Path(__file__).resolve().parents[3] / "support" / "docker" / "entrypoint.sh"


def run_entrypoint(tmp_path: Path, **overrides: str) -> subprocess.CompletedProcess[str]:
    """Run the entrypoint with everything writable pointed inside tmp_path."""
    env = dict(os.environ)
    env.pop("MYSQL_PASSWORD", None)
    env.update(
        {
            "CONFIG_FILE": str(tmp_path / "config.yaml"),
            "DUMP_DIR": str(tmp_path / "dumps"),
            "REPLICATION_STATE_FILE": str(tmp_path / "replication.state"),
            "MYGRAMDB_BINARY": "/usr/bin/true",
        }
    )
    env.update(overrides)
    return subprocess.run(
        ["/bin/sh", str(ENTRYPOINT), "true"],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )


def test_missing_mysql_password_refuses_to_generate_a_configuration(tmp_path: Path) -> None:
    result = run_entrypoint(tmp_path)

    assert result.returncode != 0
    assert "MYSQL_PASSWORD must be set" in result.stderr
    assert not (tmp_path / "config.yaml").exists(), (
        "a configuration was written despite the missing password"
    )


def test_generated_configuration_is_not_readable_by_other_accounts(tmp_path: Path) -> None:
    result = run_entrypoint(tmp_path, MYSQL_PASSWORD="s3cret-from-the-environment")

    assert result.returncode == 0, result.stderr
    config = tmp_path / "config.yaml"
    mode = stat.S_IMODE(config.stat().st_mode)
    # The password is in the file from its first write, so the mode has to be
    # right at creation rather than tightened afterwards.
    assert mode & (stat.S_IRWXG | stat.S_IRWXO) == 0, f"config.yaml was created with mode {mode:o}"
    assert 'password: "s3cret-from-the-environment"' in config.read_text()


@pytest.mark.parametrize(
    "hostile_value",
    [
        '"\napi:\n  admin_token: ""',  # close the scalar and add a key
        "line-one\nline-two",  # a bare newline
        'quote"inside',
        "back\\slash",
    ],
)
def test_environment_values_cannot_introduce_configuration_keys(
    tmp_path: Path, hostile_value: str
) -> None:
    result = run_entrypoint(tmp_path, MYSQL_PASSWORD="placeholder", MYSQL_USER=hostile_value)

    assert result.returncode == 0, result.stderr
    text = (tmp_path / "config.yaml").read_text()
    user_lines = [line for line in text.splitlines() if line.startswith("  user:")]
    assert len(user_lines) == 1
    # Whatever the value contained, it stays inside one double-quoted scalar on
    # one line, so it cannot become a sibling key.
    assert user_lines[0].startswith('  user: "')
    assert user_lines[0].endswith('"')
    assert 'admin_token: ""' not in text.replace('  admin_token: ""', "")


def test_an_existing_configuration_is_left_alone(tmp_path: Path) -> None:
    config = tmp_path / "config.yaml"
    config.write_text("# operator supplied\n")

    result = run_entrypoint(tmp_path)

    # No password is required on this path: nothing is generated.
    assert result.returncode == 0, result.stderr
    assert config.read_text() == "# operator supplied\n"


def test_rejected_numeric_and_boolean_values_stop_before_writing(tmp_path: Path) -> None:
    port = run_entrypoint(tmp_path, MYSQL_PASSWORD="placeholder", MYSQL_PORT="3306; rm -rf /")
    assert port.returncode != 0
    assert "MYSQL_PORT must be an unsigned integer" in port.stderr

    flag = run_entrypoint(tmp_path, MYSQL_PASSWORD="placeholder", MYSQL_SSL_ENABLE="yes")
    assert flag.returncode != 0
    assert "MYSQL_SSL_ENABLE must be true or false" in flag.stderr

    assert not (tmp_path / "config.yaml").exists()
