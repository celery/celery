# -*- coding: utf-8 -*-
"""
    celery.exceptions
    ~~~~~~~~~~~~~~~~~

    This module contains all exceptions used by the Celery API.

"""
from __future__ import absolute_import

from billiard.exceptions import (  # noqa
    SoftTimeLimitExceeded, TimeLimitExceeded, WorkerLostError, Terminated,
)

UNREGISTERED_FMT = """\
Task of kind %s is not registered, please make sure it's imported.\
"""


class SecurityError(Exception):
    """Security related exceptions.

    Handle with care.

    """


class Ignore(Exception):
    """A task can raise this to ignore doing state updates."""


class SystemTerminate(SystemExit):
    """Signals that the worker should terminate."""


class QueueNotFound(KeyError):
    """Task routed to a queue not in CELERY_QUEUES."""


class ImproperlyConfigured(ImportError):
    """Celery is somehow improperly configured."""


class NotRegistered(KeyError):
    """The task is not registered."""

    def __repr__(self):
        return UNREGISTERED_FMT % str(self)


class AlreadyRegistered(Exception):
    """The task is already registered."""


class TimeoutError(Exception):
    """The operation timed out."""


class MaxRetriesExceededError(Exception):
    """The tasks max restart limit has been exceeded."""


class RetryTaskError(Exception):
    """The task is to be retried later."""

    #: Optional message describing context of retry.
    message = None

    #: Exception (if any) that caused the retry to happen.
    exc = None

    #: Time of retry (ETA), either int or :class:`~datetime.datetime`.
    when = None

    def __init__(self, message=None, exc=None, when=None, **kwargs):
        from kombu.utils.encoding import safe_repr
        self.message = message
        if isinstance(exc, basestring):
            self.exc, self.excs = None, exc
        else:
            self.exc, self.excs = exc, safe_repr(exc) if exc else None
        self.when = when
        Exception.__init__(self, exc, when, **kwargs)

    def humanize(self):
        if isinstance(self.when, int):
            return 'in %ss' % self.when
        return 'at %s' % (self.when, )

    def __str__(self):
        if self.message:
            return self.message
        if self.excs:
            return 'Retry %s: %r' % (self.humanize(), self.excs)
        return 'Retry %s' % self.humanize()

    def __reduce__(self):
        return self.__class__, (self.message, self.excs, self.when)


class TaskRevokedError(Exception):
    """The task has been revoked, so no result available."""


class NotConfigured(UserWarning):
    """Celery has not been configured, as no config module has been found."""


class AlwaysEagerIgnored(UserWarning):
    """send_task ignores CELERY_ALWAYS_EAGER option"""


class InvalidTaskError(Exception):
    """The task has invalid data or is not properly constructed."""


class CPendingDeprecationWarning(PendingDeprecationWarning):
    pass


class CDeprecationWarning(DeprecationWarning):
    pass


class IncompleteStream(Exception):
    """Found the end of a stream of data, but the data is not yet complete."""


class ChordError(Exception):
    """A task part of the chord raised an exception."""
