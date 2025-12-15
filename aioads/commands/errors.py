"""
This module provides errors emitted by commands
"""

from aioads.ads_error_codes import AdsErrorCode


class AdsCommandError(Exception):
    """Base class for all Ads command errors."""

    def __init__(self, error_code: AdsErrorCode, *args):
        self.error_code = error_code
        super().__init__(*args)

    def description(self) -> str:
        """Returns a human-readable description of the error."""
        return (
            f"ADS Command Error\n"
            f"  Code: {self.error_code}\n"
            f"  Description: {self.error_code.description}"
            f"  Args: {self.args}"
        )

    def __str__(self) -> str:
        """Returns a formatted string representation of the error."""
        return self.description()

    def __repr__(self) -> str:
        """Returns a detailed representation of the error for debugging."""
        return f"AdsCommandError(error_code={self.error_code!r}, error_message:{self.error_code.description} args={self.args!r})"
