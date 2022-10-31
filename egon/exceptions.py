"""Custom exception classes raised by the porent package."""


class MissingConnectionError(Exception):
    """Raised when data cannot be properly communicated due to a broken connection between Egon objects"""
