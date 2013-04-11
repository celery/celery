# -*- coding: utf-8 -*-
"""
    celery.worker.job
    ~~~~~~~~~~~~~~~~~

    This module defines the :class:`Request` class,
    which specifies how tasks are executed.

"""
from __future__ import absolute_import

import logging
import time
import socket
import sys

from datetime import datetime

from kombu.utils import kwdict, reprcall
from kombu.utils.encoding import safe_repr, safe_str

from celery import signals
from celery.app import app_or_default
from celery.datastructures import ExceptionInfo
from celery.exceptions import (
    Ignore, TaskRevokedError, InvalidTaskError,
    SoftTimeLimitExceeded, TimeLimitExceeded,
    WorkerLostError, Terminated, RetryTaskError,
)
from celery.platforms import signals as _signals
from celery.task.trace import (
    trace_task,
    trace_task_ret,
)
from celery.utils import fun_takes_kwargs
from celery.utils.functional import noop
from celery.utils.log import get_logger
from celery.utils.serialization import get_pickled_exception
from celery.utils.text import truncate
from celery.utils.timeutils import maybe_iso8601, timezone, maybe_make_aware

from . import state

IS_PYPY = hasattr(sys, 'pypy_version_info')

logger = get_logger(__name__)
debug, info, warn, error = (logger.debug, logger.info,
                            logger.warning, logger.error)
_does_info = False
_does_debug = False


def __optimize__():
    global _does_debug
    global _does_info
    _does_debug = logger.isEnabledFor(logging.DEBUG)
    _does_info = logger.isEnabledFor(logging.INFO)
__optimize__()

# Localize
tz_utc = timezone.utc
tz_or_local = timezone.tz_or_local
send_revoked = signals.task_revoked.send

task_accepted = state.task_accepted
task_ready = state.task_ready
revoked_tasks = state.revoked

NEEDS_KWDICT = sys.version_info <= (2, 6)


class Request(object):
    """A request for task execution."""
    if not IS_PYPY:
        __slots__ = (
            'app', 'name', 'id', 'args', 'kwargs', 'on_ack', 'delivery_info',
            'hostname', 'eventer', 'connection_errors', 'task', 'eta',
            'expires', 'request_dict', 'acknowledged',
            'utc', 'time_start', 'worker_pid', '_already_revoked',
            '_terminate_on_ack',
            '_tzlocal', '__weakref__',
        )

    #: Format string used to log task success.
    success_msg = """\
        Task %(name)s[%(id)s] succeeded in %(runtime)ss: %(return_value)s
    """

    #: Format string used to log task failure.
    error_msg = """\
        Task %(name)s[%(id)s] raised exception: %(exc)s
    """

    #: Format string used to log internal error.
    internal_error_msg = """\
        Task %(name)s[%(id)s] INTERNAL ERROR: %(exc)s
    """

    ignored_msg = """\
        Task %(name)s[%(id)s] ignored
    """

    #: Format string used to log task retry.
    retry_msg = """Task %(name)s[%(id)s] retry: %(exc)s"""

    def __init__(self, body, on_ack=noop,
                 hostname=None, eventer=None, app=None,
                 connection_errors=None, request_dict=None,
                 delivery_info=None, task=None, **opts):
        self.app = app or app_or_default(app)
        name = self.name = body['task']
        self.id = body['id']
        self.args = body.get('args', [])
        self.kwargs = body.get('kwargs', {})
        try:
            self.kwargs.items
        except AttributeError:
            raise InvalidTaskError(
                'Task keyword arguments is not a mapping')
        if NEEDS_KWDICT:
            self.kwargs = kwdict(self.kwargs)
        eta = body.get('eta')
        expires = body.get('expires')
        utc = self.utc = body.get('utc', False)
        self.on_ack = on_ack
        self.hostname = hostname or socket.gethostname()
        self.eventer = eventer
        self.connection_errors = connection_errors or ()
        self.task = task or self.app.tasks[name]
        self.acknowledged = self._already_revoked = False
        self.time_start = self.worker_pid = self._terminate_on_ack = None
        self._tzlocal = None

        # timezone means the message is timezone-aware, and the only timezone
        # supported at this point is UTC.
        if eta is not None:
            try:
                self.eta = maybe_iso8601(eta)
            except (AttributeError, ValueError), exc:
                raise InvalidTaskError(
                    'invalid eta value %r: %s' % (eta, exc, ))
            if utc:
                self.eta = maybe_make_aware(self.eta, self.tzlocal)
        else:
            self.eta = None
        if expires is not None:
            try:
                self.expires = maybe_iso8601(expires)
            except (AttributeError, ValueError), exc:
                raise InvalidTaskError(
                    'invalid expires value %r: %s' % (expires, exc, ))
            if utc:
                self.expires = maybe_make_aware(self.expires, self.tzlocal)
        else:
            self.expires = None

        delivery_info = {} if delivery_info is None else delivery_info
        self.delivery_info = {
            'exchange': delivery_info.get('exchange'),
            'routing_key': delivery_info.get('routing_key'),
            'priority': delivery_info.get('priority'),
        }

        # amqplib transport adds the channel here for some reason, so need
        # to remove it.
        self.delivery_info.pop('channel', None)
        self.request_dict = body

    @classmethod
    def from_message(cls, message, body, **kwargs):
        # should be deprecated
        return Request(
            body,
            delivery_info=getattr(message, 'delivery_info', None), **kwargs
        )

    def extend_with_default_kwargs(self):
        """Extend the tasks keyword arguments with standard task arguments.

        Currently these are `logfile`, `loglevel`, `task_id`,
        `task_name`, `task_retries`, and `delivery_info`.

        See :meth:`celery.task.base.Task.run` for more information.

        Magic keyword arguments are deprecated and will be removed
        in version 4.0.

        """
        kwargs = dict(self.kwargs)
        default_kwargs = {'logfile': None,   # deprecated
                          'loglevel': None,  # deprecated
                          'task_id': self.id,
                          'task_name': self.name,
                          'task_retries': self.request_dict.get('retries', 0),
                          'task_is_eager': False,
                          'delivery_info': self.delivery_info}
        fun = self.task.run
        supported_keys = fun_takes_kwargs(fun, default_kwargs)
        extend_with = dict((key, val) for key, val in default_kwargs.items()
                           if key in supported_keys)
        kwargs.update(extend_with)
        return kwargs

    def execute_using_pool(self, pool, **kwargs):
        """Like :meth:`execute`, but using a worker pool.

        :param pool: A :class:`celery.concurrency.base.TaskPool` instance.

        :raises celery.exceptions.TaskRevokedError: if the task was revoked
            and ignored.

        """
        task = self.task
        if self.revoked():
            raise TaskRevokedError(self.id)

        hostname = self.hostname
        kwargs = self.kwargs
        if task.accept_magic_kwargs:
            kwargs = self.extend_with_default_kwargs()
        request = self.request_dict
        request.update({'hostname': hostname, 'is_eager': False,
                        'delivery_info': self.delivery_info,
                        'group': self.request_dict.get('taskset')})
        result = pool.apply_async(trace_task_ret,
                                  args=(self.name, self.id,
                                        self.args, kwargs, request),
                                  accept_callback=self.on_accepted,
                                  timeout_callback=self.on_timeout,
                                  callback=self.on_success,
                                  error_callback=self.on_failure,
                                  soft_timeout=task.soft_time_limit,
                                  timeout=task.time_limit)
        return result

    def execute(self, loglevel=None, logfile=None):
        """Execute the task in a :func:`~celery.task.trace.trace_task`.

        :keyword loglevel: The loglevel used by the task.
        :keyword logfile: The logfile used by the task.

        """
        if self.revoked():
            return

        # acknowledge task as being processed.
        if not self.task.acks_late:
            self.acknowledge()

        kwargs = self.kwargs
        if self.task.accept_magic_kwargs:
            kwargs = self.extend_with_default_kwargs()
        request = self.request_dict
        request.update({'loglevel': loglevel, 'logfile': logfile,
                        'hostname': self.hostname, 'is_eager': False,
                        'delivery_info': self.delivery_info})
        retval = trace_task(self.task, self.id, self.args, kwargs, request,
                            **{'hostname': self.hostname,
                               'loader': self.app.loader})
        self.acknowledge()
        return retval

    def maybe_expire(self):
        """If expired, mark the task as revoked."""
        if self.expires:
            now = datetime.now(tz_or_local(self.tzlocal) if self.utc else None)
            if now > self.expires:
                revoked_tasks.add(self.id)
                return True

    def terminate(self, pool, signal=None):
        if self.time_start:
            signal = _signals.signum(signal or 'TERM')
            pool.terminate_job(self.worker_pid, signal)
            self._announce_revoked('terminated', True, signal, False)
        else:
            self._terminate_on_ack = pool, signal

    def _announce_revoked(self, reason, terminated, signum, expired):
        task_ready(self)
        self.send_event('task-revoked',
                        terminated=terminated, signum=signum, expired=expired)
        if self.store_errors:
            self.task.backend.mark_as_revoked(self.id, reason)
        self.acknowledge()
        self._already_revoked = True
        send_revoked(self.task, terminated=terminated,
                     signum=signum, expired=expired)

    def revoked(self):
        """If revoked, skip task and mark state."""
        expired = False
        if self._already_revoked:
            return True
        if self.expires:
            expired = self.maybe_expire()
        if self.id in revoked_tasks:
            warn('Skipping revoked task: %s[%s]', self.name, self.id)
            self._announce_revoked(
                'expired' if expired else 'revoked', False, None, expired,
            )
            return True
        return False

    def send_event(self, type, **fields):
        if self.eventer and self.eventer.enabled:
            self.eventer.send(type, uuid=self.id, **fields)

    def on_accepted(self, pid, time_accepted):
        """Handler called when task is accepted by worker pool."""
        self.worker_pid = pid
        self.time_start = time_accepted
        task_accepted(self)
        if not self.task.acks_late:
            self.acknowledge()
        self.send_event('task-started', pid=pid)
        if _does_debug:
            debug('Task accepted: %s[%s] pid:%r', self.name, self.id, pid)
        if self._terminate_on_ack is not None:
            self.terminate(*self._terminate_on_ack)

    def on_timeout(self, soft, timeout):
        """Handler called if the task times out."""
        task_ready(self)
        if soft:
            warn('Soft time limit (%ss) exceeded for %s[%s]',
                 timeout, self.name, self.id)
            exc = SoftTimeLimitExceeded(timeout)
        else:
            error('Hard time limit (%ss) exceeded for %s[%s]',
                  timeout, self.name, self.id)
            exc = TimeLimitExceeded(timeout)

        if self.store_errors:
            self.task.backend.mark_as_failure(self.id, exc)

    def on_success(self, ret_value, now=None):
        """Handler called if the task was successfully processed."""
        if isinstance(ret_value, ExceptionInfo):
            if isinstance(ret_value.exception, (
                    SystemExit, KeyboardInterrupt)):
                raise ret_value.exception
            return self.on_failure(ret_value)
        task_ready(self)

        if self.task.acks_late:
            self.acknowledge()

        if self.eventer and self.eventer.enabled:
            now = time.time()
            runtime = self.time_start and (time.time() - self.time_start) or 0
            self.send_event('task-succeeded',
                            result=safe_repr(ret_value), runtime=runtime)

        if _does_info:
            now = now or time.time()
            runtime = self.time_start and (time.time() - self.time_start) or 0
            info(self.success_msg.strip(), {
                'id': self.id, 'name': self.name,
                'return_value': self.repr_result(ret_value),
                'runtime': runtime})

    def on_retry(self, exc_info):
        """Handler called if the task should be retried."""
        if self.task.acks_late:
            self.acknowledge()

        self.send_event('task-retried',
                        exception=safe_repr(exc_info.exception.exc),
                        traceback=safe_str(exc_info.traceback))

        if _does_info:
            info(self.retry_msg.strip(), {
                'id': self.id, 'name': self.name,
                'exc': exc_info.exception})

    def on_failure(self, exc_info):
        """Handler called if the task raised an exception."""
        task_ready(self)

        if not exc_info.internal:
            exc = exc_info.exception

            if isinstance(exc, RetryTaskError):
                return self.on_retry(exc_info)

            # These are special cases where the process would not have had
            # time to write the result.
            if self.store_errors:
                if isinstance(exc, WorkerLostError):
                    self.task.backend.mark_as_failure(self.id, exc)
                elif isinstance(exc, Terminated):
                    self._announce_revoked('terminated', True, str(exc), False)
            # (acks_late) acknowledge after result stored.
            if self.task.acks_late:
                self.acknowledge()
        self._log_error(exc_info)

    def _log_error(self, einfo):
        einfo.exception = get_pickled_exception(einfo.exception)
        exception, traceback, exc_info, internal, sargs, skwargs = (
            safe_repr(einfo.exception),
            safe_str(einfo.traceback),
            einfo.exc_info,
            einfo.internal,
            safe_repr(self.args),
            safe_repr(self.kwargs),
        )
        format = self.error_msg
        description = 'raised exception'
        severity = logging.ERROR
        self.send_event(
            'task-failed', exception=exception, traceback=traceback,
        )

        if internal:
            if isinstance(einfo.exception, Ignore):
                format = self.ignored_msg
                description = 'ignored'
                severity = logging.INFO
                exc_info = None
                self.acknowledge()
            else:
                format = self.internal_error_msg
                description = 'INTERNAL ERROR'
                severity = logging.CRITICAL

        context = {
            'hostname': self.hostname,
            'id': self.id,
            'name': self.name,
            'exc': exception,
            'traceback': traceback,
            'args': sargs,
            'kwargs': skwargs,
            'description': description,
        }

        logger.log(severity, format.strip(), context,
                   exc_info=exc_info,
                   extra={'data': {'id': self.id,
                                   'name': self.name,
                                   'args': sargs,
                                   'kwargs': skwargs,
                                   'hostname': self.hostname,
                                   'internal': internal}})

        self.task.send_error_email(context, einfo.exception)

    def acknowledge(self):
        """Acknowledge task."""
        if not self.acknowledged:
            self.on_ack(logger, self.connection_errors)
            self.acknowledged = True

    def repr_result(self, result, maxlen=46):
        # 46 is the length needed to fit
        #     'the quick brown fox jumps over the lazy dog' :)
        return truncate(safe_repr(result), maxlen)

    def info(self, safe=False):
        return {'id': self.id,
                'name': self.name,
                'args': self.args if safe else safe_repr(self.args),
                'kwargs': self.kwargs if safe else safe_repr(self.kwargs),
                'hostname': self.hostname,
                'time_start': self.time_start,
                'acknowledged': self.acknowledged,
                'delivery_info': self.delivery_info,
                'worker_pid': self.worker_pid}

    def __str__(self):
        return '%s[%s]%s%s' % (
            self.name, self.id,
            ' eta:[%s]' % (self.eta, ) if self.eta else '',
            ' expires:[%s]' % (self.expires, ) if self.expires else '')
    shortinfo = __str__

    def __repr__(self):
        return '<%s %s: %s>' % (
            type(self).__name__, self.id,
            reprcall(self.name, self.args, self.kwargs),
        )

    @property
    def tzlocal(self):
        if self._tzlocal is None:
            self._tzlocal = self.app.conf.CELERY_TIMEZONE
        return self._tzlocal

    @property
    def store_errors(self):
        return (not self.task.ignore_result
                or self.task.store_errors_even_if_ignored)

    def _compat_get_task_id(self):
        return self.id

    def _compat_set_task_id(self, value):
        self.id = value
    task_id = property(_compat_get_task_id, _compat_set_task_id)

    def _compat_get_task_name(self):
        return self.name

    def _compat_set_task_name(self, value):
        self.name = value
    task_name = property(_compat_get_task_name, _compat_set_task_name)


class TaskRequest(Request):

    def __init__(self, name, id, args=(), kwargs={},
                 eta=None, expires=None, **options):
        """Compatibility class."""

        super(TaskRequest, self).__init__({
            'task': name, 'id': id, 'args': args,
            'kwargs': kwargs, 'eta': eta,
            'expires': expires}, **options)
