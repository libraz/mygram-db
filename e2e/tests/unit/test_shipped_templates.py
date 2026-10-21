"""Every shipped configuration template must load under `mygramdb -t`.

Covers the example configurations, the package-verification fixture and the
file the container entrypoint generates, so a template cannot drift from the
schema or from the startup rules the server enforces.
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]
BINARY = Path(os.environ.get("MYGRAMDB_BINARY", ROOT / "build" / "bin" / "mygramdb"))
ENTRYPOINT = ROOT / "support" / "docker" / "entrypoint.sh"

TEMPLATES = sorted(
    [
        *(ROOT / "examples").glob("config*.yaml"),
        *(ROOT / "examples").glob("config*.json"),
        *(ROOT / "support" / "testing").glob("config-*.yaml"),
    ]
)

pytestmark = pytest.mark.skipif(
    not BINARY.exists(), reason=f"mygramdb binary not found at {BINARY}"
)


def validate(config: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [str(BINARY), "-t", "-c", str(config)],
        capture_output=True,
        text=True,
        check=False,
    )


def test_template_set_is_not_empty() -> None:
    assert TEMPLATES, "no shipped configuration templates were found"


@pytest.mark.parametrize("template", TEMPLATES, ids=lambda p: str(p.relative_to(ROOT)))
def test_shipped_template_passes_config_test(template: Path) -> None:
    result = validate(template)

    assert result.returncode == 0, result.stdout + result.stderr


def test_entrypoint_generated_configuration_passes_config_test(tmp_path: Path) -> None:
    config = tmp_path / "config.yaml"
    env = dict(os.environ)
    env.update(
        {
            "CONFIG_FILE": str(config),
            "DUMP_DIR": str(tmp_path / "dumps"),
            "REPLICATION_STATE_FILE": str(tmp_path / "replication.state"),
            "MYGRAMDB_BINARY": "/usr/bin/true",
            "MYSQL_PASSWORD": "placeholder",
            "API_ADMIN_TOKEN": "placeholder-admin-token",
            "NETWORK_ALLOW_CIDRS": "127.0.0.1/32,172.16.0.0/12",
        }
    )
    generated = subprocess.run(
        ["/bin/sh", str(ENTRYPOINT), "true"], env=env, capture_output=True, text=True, check=False
    )
    assert generated.returncode == 0, generated.stderr

    result = validate(config)

    assert result.returncode == 0, result.stdout + result.stderr
