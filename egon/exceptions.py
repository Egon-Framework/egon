"""Custom exception classes raised by the parent package."""


class MissingConnectionError(Exception):
    """Raised when data cannot be properly communicated due to a broken connection between Egon objects"""


class NodeValidationError(Exception):
    """Raised when an individual node fails to validate properly"""
