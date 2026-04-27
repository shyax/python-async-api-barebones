"""FastAPI-based failure simulation server for integration and load tests."""

from mock_server.app import FailureProfile, build_app

__all__ = ["FailureProfile", "build_app"]
