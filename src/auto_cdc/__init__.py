"""
auto-cdc: Automated Change Data Capture for Databricks Delta Lake
"""

__version__ = "0.1.0"
__author__ = "Your Name"
__description__ = "Automated Change Data Capture framework for Databricks Delta Lake"

from .cdc import CDC
from .exceptions import CDCException, CDCVersionError, CDCSchemaError

__all__ = [
    "CDC",
    "CDCException",
    "CDCVersionError",
    "CDCSchemaError",
]
