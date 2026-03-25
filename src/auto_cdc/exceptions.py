"""
Custom exceptions for auto-cdc package.
"""


class CDCException(Exception):
    """Base exception for CDC operations."""
    pass


class CDCVersionError(CDCException):
    """Raised when there's an issue with CDC versions."""
    pass


class CDCSchemaError(CDCException):
    """Raised when there's a schema mismatch or evolution issue."""
    pass


class CDCFileError(CDCException):
    """Raised when there's an issue with file operations."""
    pass
