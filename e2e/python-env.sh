#!/bin/bash
# Shared Python environment provisioning for the e2e entry-point scripts.
# Source this file, then call e2e_python_setup <e2e-dir>; it exports
# E2E_PYTHON_BIN pointing at the interpreter the suite should run under.
#
# Resolution order:
#   1. $E2E_PYTHON - explicit interpreter, for CI images that provision their own
#   2. rye         - project standard; installs exactly what requirements-dev.lock pins
#   3. venv + pip  - fallback that still installs from the lockfile, so a
#                    PEP 668 system Python (Homebrew) is never written to

e2e_python_setup() {
    local e2e_dir="$1"

    if [[ -n "${E2E_PYTHON:-}" ]]; then
        E2E_PYTHON_BIN="$E2E_PYTHON"
        if [[ ! -x "$E2E_PYTHON_BIN" ]]; then
            echo "ERROR: E2E_PYTHON is set to '$E2E_PYTHON' but it is not executable" >&2
            return 1
        fi
        export E2E_PYTHON_BIN
        return 0
    fi

    E2E_PYTHON_BIN="$e2e_dir/.venv/bin/python"

    if command -v rye >/dev/null 2>&1; then
        echo "Syncing Python dependencies with rye..."
        # --no-lock installs the pinned lockfile without re-resolving, so a
        # test run can never silently change dependency versions.
        (cd "$e2e_dir" && rye sync --no-lock)
        export E2E_PYTHON_BIN
        return 0
    fi

    echo "rye not found; falling back to a local virtualenv." >&2
    if [[ ! -x "$E2E_PYTHON_BIN" ]]; then
        python3 -m venv "$e2e_dir/.venv"
    fi
    if ! "$E2E_PYTHON_BIN" -c "import pytest, mysql.connector" 2>/dev/null; then
        echo "Installing Python dependencies..."
        # Run from the e2e directory: the lockfile's '-e file:.' entry is
        # resolved against the working directory, not the requirements file.
        (cd "$e2e_dir" && "$E2E_PYTHON_BIN" -m pip install -q -r requirements-dev.lock)
    fi
    export E2E_PYTHON_BIN
}
