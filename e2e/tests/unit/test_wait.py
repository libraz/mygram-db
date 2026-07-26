"""Unit tests for polling wait diagnostics."""

import pytest

from lib.wait import WaitTimeoutError, wait_until_gte, wait_until_value


def test_wait_until_value_reports_last_exception() -> None:
    def failing_value() -> int:
        raise RuntimeError("protocol response changed")

    with pytest.raises(WaitTimeoutError, match="last error: protocol response changed"):
        wait_until_value(failing_value, 7, timeout=0.01, interval=0)


def test_wait_until_gte_reports_last_exception_and_value() -> None:
    calls = 0

    def intermittent_value() -> int:
        nonlocal calls
        calls += 1
        if calls == 1:
            return 2
        raise ConnectionError("connection refused")

    with pytest.raises(
        WaitTimeoutError,
        match=r"expected >= 3, last=2, last error: connection refused",
    ):
        wait_until_gte(intermittent_value, 3, timeout=0.01, interval=0)
