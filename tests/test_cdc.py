"""Unit tests for CDC module."""

import pytest
from datetime import datetime
from auto_cdc import CDC, CDCException


class TestCDC:
    """Test cases for CDC class."""

    def test_cdc_tracker_table_name(self):
        """Test CDC tracker table name generation."""
        folder_path = "/Volumes/catalog/schema/table"
        expected = "catalog.schema_table"

        result = CDC.cdc_tracker_table_name(folder_path)
        assert result == expected

    def test_cdc_tracker_table_name_nested(self):
        """Test CDC tracker table name with nested path."""
        folder_path = "/Volumes/catalog/schema/my/nested/path"
        expected = "catalog.schema_my_nested_path"

        result = CDC.cdc_tracker_table_name(folder_path)
        assert result == expected

    def test_cdc_tracker_table_name_strips_slashes(self):
        """Test that leading/trailing slashes are handled correctly."""
        folder_path = "/Volumes/catalog/schema/table/"
        expected = "catalog.schema_table"

        result = CDC.cdc_tracker_table_name(folder_path)
        assert result == expected

    @pytest.mark.skip(reason="Requires Databricks environment")
    def test_write_to_cdc_feed(self, sample_data, sample_keys):
        """Test writing to CDC feed (requires Databricks)."""
        # This test requires a live Databricks environment
        # and would be run in an integration test suite
        pass
