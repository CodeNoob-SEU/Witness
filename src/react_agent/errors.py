"""Public exception types."""


class ReActAgentError(Exception):
    """Base class for framework errors."""


class ConfigurationError(ReActAgentError, ValueError):
    """Raised for invalid framework or tool configuration."""


class ModelInvocationError(ReActAgentError):
    """A provider request failed after the provider client's retry policy."""

    def __init__(self, message: str, *, request_id: str | None = None) -> None:
        super().__init__(message)
        self.request_id = request_id
