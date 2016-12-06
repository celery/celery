# -*- coding: utf-8 -*-
"""Celery error types.

Error Hierarchy
===============

- :exc:`Exception`
    - :exc:`celery.exceptions.CeleryError`
        - :exc:`~celery.exceptions.ImproperlyConfigured`
        - :exc:`~celery.exceptions.SecurityError`
        - :exc:`~celery.exceptions.TaskPredicate`
            - :exc:`~celery.exceptions.Ignore`
            - :exc:`~celery.exceptions.Reject`
            - :exc:`~celery.exceptions.Retry`
        - :exc:`~celery.exceptions.TaskError`
            - :exc:`~celery.exceptions.QueueNotFound`
            - :exc:`~celery.exceptions.IncompleteStream`
            - :exc:`~celery.exceptions.NotRegistered`
            - :exc:`~celery.exceptions.AlreadyRegistered`
            - :exc:`~celery.exceptions.TimeoutError`
            - :exc:`~celery.exceptions.MaxRetriesExceededError`
            - :exc:`~celery.exceptions.TaskRevokedError`
            - :exc:`~celery.exceptions.InvalidTaskError`
            - :exc:`~celery.exceptions.ChordError`
    - :class:`kombu.exceptions.KombuError`
        - :exc:`~celery.exceptions.OperationalError`

            Raised when a transport connection error occurs while
            sending a message (be it a task, remote control command error).

            .. note::
                This exception does not inherit from
                :exc:`~celery.exceptions.CeleryError`.
    - **billiard errors** (prefork pool)
        - :exc:`~celery.exceptions.SoftTimeLimitExceeded`
        - :exc:`~celery.exceptions.TimeLimitExceeded`
        - :exc:`~celery.exceptions.WorkerLostError`
        - :exc:`~celery.exceptions.Terminated`
- :class:`UserWarning`
    - :class:`~celery.exceptions.CeleryWarning`
        - :class:`~celery.exceptions.AlwaysEagerIgnored`
        - :class:`~celery.exceptions.DuplicateNodenameWarning`
        - :class:`~celery.exceptions.FixupWarning`
        - :class:`~celery.exceptions.NotConfigured`
- :exc:`BaseException`
    - :exc:`SystemExit`
        - :exc:`~celery.exceptions.WorkerTerminate`
        - :exc:`~celery.exceptions.WorkerShutdown`
"""
from __future__ import absolute_import, unicode_literals
import numbers
from .five import python_2_unicode_compatible, string_t
from billiard.exceptions import (
    SoftTimeLimitExceeded, TimeLimitExceeded, WorkerLostError, Terminated,
)
from kombu.exceptions import OperationalError

__all__ = [
    # Warnings
    'CeleryWarning',
    'AlwaysEagerIgnored', 'DuplicateNodenameWarning',
    'FixupWarning', 'NotConfigured',

    # Core errors
    'CeleryError',
    'ImproperlyConfigured', 'SecurityError',

    # Kombu (messaging) errors.
    'OperationalError',

    # Task semi-predicates
    'TaskPredicate', 'Ignore', 'Reject', 'Retry',

    # Task related errors.
    'TaskError', 'QueueNotFound', 'IncompleteStream',
    'NotRegistered', 'AlreadyRegistered', 'TimeoutError',
    'MaxRetriesExceededError', 'TaskRevokedError',
    'InvalidTaskError', 'ChordError',

    # Billiard task errors.
    'SoftTimeLimitExceeded', 'TimeLimitExceeded',
    'WorkerLostError', 'Terminated',

    # Deprecation warnings (forcing Python to emit them).
    'CPendingDeprecationWarning', 'CDeprecationWarning',

    # Worker shutdown semi-predicates (inherits from SystemExit).
    'WorkerShutdown', 'WorkerTerminate',
]

UNREGISTERED_FMT = """\
Task of kind {0} never registered, please make sure it's imported.\
"""


class CeleryWarning(UserWarning):
    """Base class for all Celery warnings."""


class AlwaysEagerIgnored(CeleryWarning):
    """send_task ignores :setting:`task_always_eager` option."""


class DuplicateNodenameWarning(CeleryWarning):
    """Multiple workers are using the same nodename."""


class FixupWarning(CeleryWarning):
    """Fixup related warning."""


class NotConfigured(CeleryWarning):
    """Celery hasn't been configured, as no config module has been found."""


class CeleryError(Exception):
    """Base class for all Celery errors."""


class TaskPredicate(CeleryError):
    """Base class for task-related semi-predicates."""


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
        super(Retry, self).__init__(self, exc, when, **kwargs)

    def humanize(self):
        if isinstance(self.when, numbers.Number):
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
RetryTaskError = Retry  # noqa: E305 XXX compat


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


class ImproperlyConfigured(CeleryError):
    """Celery is somehow improperly configured."""


class SecurityError(CeleryError):
    """Security related exception."""


class TaskError(CeleryError):
    """Task related errors."""


class QueueNotFound(KeyError, TaskError):
    """Task routed to a queue not in ``conf.queues``."""


class IncompleteStream(TaskError):
    """Found the end of a stream of data, but the data isn't complete."""


@python_2_unicode_compatible
class NotRegistered(KeyError, TaskError):
    """The task ain't registered."""

    def __repr__(self):
        return UNREGISTERED_FMT.format(self)


class AlreadyRegistered(TaskError):
    """The task is already registered."""
    # XXX Unused


class TimeoutError(TaskError):
    """The operation timed out."""


class MaxRetriesExceededError(TaskError):
    """The tasks max restart limit has been exceeded."""


class TaskRevokedError(TaskError):
    """The task has been revoked, so no result available."""


class InvalidTaskError(TaskError):
    """The task has invalid data or ain't properly constructed."""


class ChordError(TaskError):
    """A task part of the chord raised an exception."""


class CPendingDeprecationWarning(PendingDeprecationWarning):
    """Warning of pending deprecation."""


class CDeprecationWarning(DeprecationWarning):
    """Warning of deprecation."""


class WorkerTerminate(SystemExit):
    """Signals that the worker should terminate immediately."""
SystemTerminate = WorkerTerminate  # noqa: E305 XXX compat


class WorkerShutdown(SystemExit):
    """Signals that the worker should perform a warm shutdown."""
