"""Polling wait utilities for async operations."""

from __future__ import annotations

import time
from typing import Callable, TypeVar

T = TypeVar("T")


class WaitTimeout(Exception):
    """Raised when a wait operation times out."""

    def __init__(self, description: str, timeout: float) -> None:
        super().__init__(f"Timed out waiting for {description} after {timeout}s")
        self.description = description
        self.timeout = timeout


def wait_until(
    predicate: Callable[[], bool],
    *,
    timeout: float = 30.0,
    interval: float = 1.0,
    description: str = "condition",
) -> None:
    """Poll until predicate returns True or timeout is reached."""
    deadline = time.monotonic() + timeout
    last_error: Exception | None = None

    while time.monotonic() < deadline:
        try:
            if predicate():
                return
        except Exception as e:
            last_error = e
        time.sleep(interval)

    msg = f"Timed out waiting for {description} after {timeout}s"
    if last_error:
        msg += f" (last error: {last_error})"
    raise WaitTimeout(description, timeout)


def wait_until_value(
    func: Callable[[], T],
    expected: T,
    *,
    timeout: float = 30.0,
    interval: float = 1.0,
    description: str = "value",
) -> T:
    """Poll until func() returns the expected value."""
    deadline = time.monotonic() + timeout
    last_value: T | None = None

    while time.monotonic() < deadline:
        try:
            last_value = func()
            if last_value == expected:
                return last_value
        except Exception:
            pass
        time.sleep(interval)

    raise WaitTimeout(
        f"{description} (expected={expected}, last={last_value})",
        timeout,
    )


def wait_until_gte(
    func: Callable[[], int | float],
    minimum: int | float,
    *,
    timeout: float = 30.0,
    interval: float = 1.0,
    description: str = "value",
) -> int | float:
    """Poll until func() returns a value >= minimum."""
    deadline = time.monotonic() + timeout
    last_value: int | float = 0

    while time.monotonic() < deadline:
        try:
            last_value = func()
            if last_value >= minimum:
                return last_value
        except Exception:
            pass
        time.sleep(interval)

    raise WaitTimeout(
        f"{description} (expected >= {minimum}, last={last_value})",
        timeout,
    )
