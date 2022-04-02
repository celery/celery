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
        - :exc:`~celery.exceptions.BackendError`
            - :exc:`~celery.exceptions.BackendGetMetaError`
            - :exc:`~celery.exceptions.BackendStoreError`
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
        - :class:`~celery.exceptions.SecurityWarning`
- :exc:`BaseException`
    - :exc:`SystemExit`
        - :exc:`~celery.exceptions.WorkerTerminate`
        - :exc:`~celery.exceptions.WorkerShutdown`
"""

from datetime import datetime
import numbers
from types import TracebackType
from typing import Any, Optional, Tuple, Type, TypeVar, TYPE_CHECKING, Union

from billiard.exceptions import SoftTimeLimitExceeded, Terminated, TimeLimitExceeded, WorkerLostError
from click import ClickException
from kombu.exceptions import OperationalError

if TYPE_CHECKING:
    from celery.canvas import Signature

__all__ = (
    'reraise',
    # Warnings
    'CeleryWarning',
    'AlwaysEagerIgnored', 'DuplicateNodenameWarning',
    'FixupWarning', 'NotConfigured', 'SecurityWarning',

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

    # Backend related errors.
    'BackendError', 'BackendGetMetaError', 'BackendStoreError',

    # Billiard task errors.
    'SoftTimeLimitExceeded', 'TimeLimitExceeded',
    'WorkerLostError', 'Terminated',

    # Deprecation warnings (forcing Python to emit them).
    'CPendingDeprecationWarning', 'CDeprecationWarning',

    # Worker shutdown semi-predicates (inherits from SystemExit).
    'WorkerShutdown', 'WorkerTerminate',

    'CeleryCommandException',
)

UNREGISTERED_FMT = """\
Task of kind {0} never registered, please make sure it's imported.\
"""


_T = TypeVar("_T", bound="Exception")


def reraise(tp: Type[_T], value: _T, tb: Optional[TracebackType] = None) -> _T:
    """Reraise exception."""
    if value.__traceback__ is not tb:
        raise value.with_traceback(tb)
    raise value


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


class SecurityWarning(CeleryWarning):
    """Potential security issue found."""


class CeleryError(Exception):
    """Base class for all Celery errors."""


class TaskPredicate(CeleryError):
    """Base class for task-related semi-predicates."""


class Retry(TaskPredicate):
    """The task is to be retried later."""

    #: Optional message describing context of retry.
    message: Optional[str] = None

    #: Exception (if any) that caused the retry to happen.
    exc: Optional[Exception] = None
    excs: Optional[str] = None

    #: Time of retry (ETA)
    when: Optional[Union[datetime, numbers.Number]] = None

    def __init__(self, message: Optional[str] = None, exc: Optional[Exception] = None,
                 when: Optional[Union[datetime, numbers.Number]] = None, is_eager: bool = False,
                 sig: Optional["Signature"] = None, **kwargs: Any) -> None:
        from kombu.utils.encoding import safe_repr
        self.message = message
        if isinstance(exc, str):
            self.exc, self.excs = None, exc
        else:
            self.exc, self.excs = exc, safe_repr(exc) if exc else None
        self.when = when
        self.is_eager = is_eager
        self.sig = sig
        super().__init__(self, exc, when, **kwargs)

    def humanize(self) -> str:
        if isinstance(self.when, numbers.Number):
            return f'in {self.when}s'
        return f'at {self.when}'

    def __str__(self) -> str:
        if self.message:
            return self.message
        if self.excs:
            return f'Retry {self.humanize()}: {self.excs}'
        return f'Retry {self.humanize()}'

    def __reduce__(self) -> Tuple[
        Type["Retry"],
        Tuple[Optional[str], Optional[Exception], Optional[Union[datetime, numbers.Number]]
    ]]:
        return self.__class__, (self.message, self.exc, self.when)


RetryTaskError = Retry  # XXX compat


class Ignore(TaskPredicate):
    """A task can raise this to ignore doing state updates."""


class Reject(TaskPredicate):
    """A task can raise this if it wants to reject/re-queue the message."""

    def __init__(self, reason: Optional[str] = None, requeue: bool = False) -> None:
        self.reason = reason
        self.requeue = requeue
        super().__init__(reason, requeue)

    def __repr__(self) -> str:
        return f'reject requeue={self.requeue}: {self.reason}'


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


class NotRegistered(KeyError, TaskError):
    """The task is not registered."""

    def __repr__(self) -> str:
        return UNREGISTERED_FMT.format(self)


class AlreadyRegistered(TaskError):
    """The task is already registered."""
    # XXX Unused


class TimeoutError(TaskError):
    """The operation timed out."""


class MaxRetriesExceededError(TaskError):
    """The tasks max restart limit has been exceeded."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.task_args = kwargs.pop("task_args", [])
        self.task_kwargs = kwargs.pop("task_kwargs", dict())
        super().__init__(*args, **kwargs)


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


SystemTerminate = WorkerTerminate  # XXX compat


class WorkerShutdown(SystemExit):
    """Signals that the worker should perform a warm shutdown."""


class BackendError(Exception):
    """An issue writing or reading to/from the backend."""


class BackendGetMetaError(BackendError):
    """An issue reading from the backend."""

    def __init__(self, *args: str, **kwargs: str) -> None:
        self.task_id = kwargs.get('task_id', "")

    def __repr__(self) -> str:
        return super().__repr__() + " task_id:" + self.task_id


class BackendStoreError(BackendError):
    """An issue writing to the backend."""

    def __init__(self, *args: str, **kwargs: str) -> None:
        self.state = kwargs.get('state', "")
        self.task_id = kwargs.get('task_id', "")

    def __repr__(self) -> str:
        return super().__repr__() + " state:" + self.state + " task_id:" + self.task_id


class CeleryCommandException(ClickException):
    """A general command exception which stores an exit code."""

    def __init__(self, message: str, exit_code: int) -> None:
        super().__init__(message=message)
        self.exit_code = exit_code
