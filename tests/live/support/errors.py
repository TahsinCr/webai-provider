class SafetyError(RuntimeError):
    """A live test stopped before it could risk unrelated account data."""


class CleanupError(SafetyError):
    """A test-created remote object could not be verified as deleted."""


class LiveApiError(RuntimeError):
    """A provider error with sensitive request and response details suppressed."""
