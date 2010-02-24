"""

Common Exceptions

"""

UNREGISTERED_FMT = """
Task of kind %s is not registered, please make sure it's imported.
""".strip()


class NotRegistered(KeyError):
    """The task is not registered."""

    def __init__(self, message, *args, **kwargs):
        message = UNREGISTERED_FMT % str(message)
        KeyError.__init__(self, message, *args, **kwargs)


class AlreadyRegistered(Exception):
    """The task is already registered."""


class TimeoutError(Exception):
    """The operation timed out."""


class MaxRetriesExceededError(Exception):
    """The tasks max restart limit has been exceeded."""


class RetryTaskError(Exception):
    """The task is to be retried later."""

    def __init__(self, message, exc, *args, **kwargs):
        self.exc = exc
        Exception.__init__(self, message, exc, *args, **kwargs)
