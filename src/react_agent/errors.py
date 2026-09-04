"""Public exception types."""


class ReActAgentError(Exception):
    """Base class for framework errors."""


class ConfigurationError(ReActAgentError, ValueError):
    """Raised for invalid framework or tool configuration."""


class ModelInvocationError(ReActAgentError):
    """A provider request failed after the provider client's retry policy.

    ``retryable`` is the provider adapter's classification: connection
    failures, timeouts, rate limits and server errors are transient and may be
    retried with a fresh attempt; semantic 4xx rejections and unparseable
    responses are not. ``status_code`` / ``error_code`` / ``error_param`` are
    structural provider metadata (never the provider's free-text message) so
    they are safe to persist as evidence.
    """

    def __init__(
        self,
        message: str,
        *,
        request_id: str | None = None,
        status_code: int | None = None,
        error_code: str | None = None,
        error_param: str | None = None,
        retryable: bool = False,
    ) -> None:
        super().__init__(message)
        self.request_id = request_id
        self.status_code = status_code
        self.error_code = error_code
        self.error_param = error_param
        self.retryable = retryable
