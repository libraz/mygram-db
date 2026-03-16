"""Smoke tests for health endpoints."""

import pytest

pytestmark = pytest.mark.smoke


class TestHealthEndpoints:
    """Test health check endpoints."""

    def test_health_live(self, mygramdb):
        """Health live endpoint should return OK."""
        assert mygramdb.health_live()

    def test_health_ready(self, mygramdb, seed_data):
        """Health ready endpoint should return OK after sync."""
        assert mygramdb.health_ready()

    def test_health_detail(self, mygramdb, seed_data):
        """Health detail should return structured data."""
        detail = mygramdb.health_detail()
        assert isinstance(detail, dict)
        assert len(detail) > 0

    def test_metrics_endpoint(self, mygramdb):
        """Metrics endpoint should return Prometheus format."""
        raw = mygramdb.metrics()
        assert isinstance(raw, str)
        assert len(raw) > 0
