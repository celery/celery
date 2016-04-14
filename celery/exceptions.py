# -*- coding: utf-8 -*-
"""
    celery.exceptions
    ~~~~~~~~~~~~~~~~~

    This module contains all exceptions used by the Celery API.

"""
from __future__ import absolute_import, unicode_literals

import numbers

from .five import python_2_unicode_compatible, string_t

from billiard.exceptions import (  # noqa
    SoftTimeLimitExceeded, TimeLimitExceeded, WorkerLostError, Terminated,
)

__all__ = [
    'CeleryError', 'CeleryWarning', 'TaskPredicate',
    'SecurityError', 'Ignore', 'QueueNotFound',
    'WorkerShutdown', 'WorkerTerminate',
    'ImproperlyConfigured', 'NotRegistered', 'AlreadyRegistered',
    'TimeoutError', 'MaxRetriesExceededError', 'Retry', 'Reject',
    'TaskRevokedError', 'NotConfigured', 'AlwaysEagerIgnored',
    'InvalidTaskError', 'ChordError', 'CPendingDeprecationWarning',
    'CDeprecationWarning', 'FixupWarning', 'DuplicateNodenameWarning',
    'SoftTimeLimitExceeded', 'TimeLimitExceeded', 'WorkerLostError',
    'Terminated', 'IncompleteStream'
]

UNREGISTERED_FMT = """\
Task of kind {0} is not registered, please make sure it's imported.\
"""


class CeleryError(Exception):
    pass


class CeleryWarning(UserWarning):
    pass


class SecurityError(CeleryError):
    """Security related exceptions.

    Handle with care.

    """


class TaskPredicate(CeleryError):
    pass


@python_2_unicode_compatible
class Retry(TaskPredicate):
    """The task is to be retried later."""

    #: Optional message describing context of retry.
    message = None

    #: Exception (if any) that caused the retry to happen.
    exc = None

    #: Time of retry (ETA), either :class:`numbers.Real` or
    #: :class:`~datetime.datetime`.
    when = None

    def __init__(self, message=None, exc=None, when=None, **kwargs):
        from kombu.utils.encoding import safe_repr
        self.message = message
        if isinstance(exc, string_t):
            self.exc, self.excs = None, exc
        else:
            self.exc, self.excs = exc, safe_repr(exc) if exc else None
        self.when = when
        Exception.__init__(self, exc, when, **kwargs)

    def humanize(self):
        if isinstance(self.when, numbers.Real):
            return 'in {0.when}s'.format(self)
        return 'at {0.when}'.format(self)

    def __str__(self):
        if self.message:
            return self.message
        if self.excs:
            return 'Retry {0}: {1}'.format(self.humanize(), self.excs)
        return 'Retry {0}'.format(self.humanize())

    def __reduce__(self):
        return self.__class__, (self.message, self.excs, self.when)
RetryTaskError = Retry   # XXX compat


class Ignore(TaskPredicate):
    """A task can raise this to ignore doing state updates."""


@python_2_unicode_compatible
class Reject(TaskPredicate):
    """A task can raise this if it wants to reject/re-queue the message."""

    def __init__(self, reason=None, requeue=False):
        self.reason = reason
        self.requeue = requeue
        super(Reject, self).__init__(reason, requeue)

    def __repr__(self):
        return 'reject requeue=%s: %s' % (self.requeue, self.reason)


class WorkerTerminate(SystemExit):
    """Signals that the worker should terminate immediately."""
SystemTerminate = WorkerTerminate  # XXX compat


class WorkerShutdown(SystemExit):
    """Signals that the worker should perform a warm shutdown."""


class QueueNotFound(KeyError):
    """Task routed to a queue not in ``conf.queues``."""


class ImproperlyConfigured(ImportError):
    """Celery is somehow improperly configured."""


@python_2_unicode_compatible
class NotRegistered(KeyError, CeleryError):
    """The task is not registered."""

    def __repr__(self):
        return UNREGISTERED_FMT.format(self)


class AlreadyRegistered(CeleryError):
    """The task is already registered."""


class TimeoutError(CeleryError):
    """The operation timed out."""


class MaxRetriesExceededError(CeleryError):
    """The tasks max restart limit has been exceeded."""


class TaskRevokedError(CeleryError):
    """The task has been revoked, so no result available."""


class NotConfigured(CeleryWarning):
    """Celery has not been configured, as no config module has been found."""


class AlwaysEagerIgnored(CeleryWarning):
    """send_task ignores :setting:`task_always_eager` option"""


class InvalidTaskError(CeleryError):
    """The task has invalid data or is not properly constructed."""


class IncompleteStream(CeleryError):
    """Found the end of a stream of data, but the data is not yet complete."""


class ChordError(CeleryError):
    """A task part of the chord raised an exception."""


class CPendingDeprecationWarning(PendingDeprecationWarning):
    pass


class CDeprecationWarning(DeprecationWarning):
    pass


class FixupWarning(CeleryWarning):
    pass


class DuplicateNodenameWarning(CeleryWarning):
    """Multiple workers are using the same nodename."""
