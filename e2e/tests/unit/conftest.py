"""Fixtures for service-independent e2e helper unit tests."""

import pytest


@pytest.fixture(autouse=True)
def ensure_replication() -> None:
    """Override the service-backed root fixture for pure helper tests."""
