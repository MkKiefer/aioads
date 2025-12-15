"""
ADS Error Definitions for ADS protocol communication.
"""

from aioads.ads_error_codes import AdsErrorCode


class AdsAmsHeaderError(Exception):
    """Raised when there is an issue with the Ads AMS headers."""

    def __init__(self, error_code: AdsErrorCode, *args):
        self.error_code = error_code
        super().__init__(*args)

    def description(self) -> str:
        """Returns a human-readable description of the AMS header error."""
        return (
            f"ADS AMS Header Error\n"
            f"  Code: {self.error_code}\n"
            f"  Description: {self.error_code.description}"
        )

    def __str__(self) -> str:
        """Returns a formatted string representation of the AMS header error."""
        return self.description()

    def __repr__(self) -> str:
        """Returns a detailed representation of the AMS header error for debugging."""
        return (
            f"AdsAmsHeaderError(error_code={self.error_code!r}, "
            f"error_message:{self.error_code.description} args={self.args!r})"
        )
