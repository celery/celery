"""

Common Exceptions

"""
from billiard.pool import SoftTimeLimitExceeded as _SoftTimeLimitExceeded

UNREGISTERED_FMT = """
Task of kind %s is not registered, please make sure it's imported.
""".strip()


class RouteNotFound(KeyError):
    """Task routed to a queue not in the routing table (CELERY_QUEUES)."""


class SoftTimeLimitExceeded(_SoftTimeLimitExceeded):
    """The soft time limit has been exceeded. This exception is raised
    to give the task a chance to clean up."""
    pass


class ImproperlyConfigured(Exception):
    """Celery is somehow improperly configured."""
    pass


class NotRegistered(KeyError):
    """The task is not registered."""

    def __init__(self, message, *args, **kwargs):
        message = UNREGISTERED_FMT % str(message)
        KeyError.__init__(self, message, *args, **kwargs)


class AlreadyRegistered(Exception):
    """The task is already registered."""
    pass


class TimeoutError(Exception):
    """The operation timed out."""
    pass


class MaxRetriesExceededError(Exception):
    """The tasks max restart limit has been exceeded."""
    pass


class RetryTaskError(Exception):
    """The task is to be retried later."""

    def __init__(self, message, exc, *args, **kwargs):
        self.exc = exc
        Exception.__init__(self, message, exc, *args, **kwargs)


class TaskRevokedError(Exception):
    """The task has been revoked, so no result available."""
    pass
