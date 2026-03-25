"""
Utility functions for auto-cdc package.
"""

import os
from datetime import datetime
from typing import Any


class Utils:
    """Utility class for CDC operations."""

    @staticmethod
    def format_file_datetime(prefix: str, timestamp: datetime, extension: str) -> str:
        """
        Format a datetime into a file name.

        Args:
            prefix: File prefix (e.g., 'cdc')
            timestamp: Datetime to format
            extension: File extension (e.g., 'parquet')

        Returns:
            Formatted filename string
        """
        if isinstance(timestamp, datetime):
            time_str = timestamp.strftime("%Y%m%d_%H%M%S")
        else:
            time_str = timestamp
        return f"{prefix}_{time_str}.{extension}"

    @staticmethod
    def push_to_volume(dataframe: Any, output_path: str, batch: bool = True) -> None:
        """
        Push dataframe to a volume path.

        Args:
            dataframe: Spark dataframe to write
            output_path: Target path for output
            batch: Whether to use batch mode

        Raises:
            ImportError: If dbutils is not available
        """
        try:
            from databricks.sdk.runtime import dbutils
        except ImportError:
            raise ImportError(
                "dbutils not available. This function requires a Databricks environment."
            )

        write_mode = "append" if batch else "overwrite"
        dataframe.write.format("parquet").mode(write_mode).save(output_path)
