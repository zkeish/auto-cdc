"""Pytest configuration for auto-cdc tests."""

import pytest


@pytest.fixture
def sample_data():
    """Provide sample data for testing."""
    return {
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "value": [100, 200, 300],
    }


@pytest.fixture
def sample_keys():
    """Provide sample keys for testing."""
    return ["id"]
