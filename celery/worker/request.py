# -*- coding: utf-8 -*-
"""
    celery.worker.request
    ~~~~~~~~~~~~~~~~~~~~~

    This module defines the :class:`Request` class,
    which specifies how tasks are executed.

"""
from __future__ import absolute_import, unicode_literals

import logging
import socket
import sys

from datetime import datetime
from weakref import ref

from kombu.utils.encoding import safe_repr, safe_str

from celery import signals
from celery.app.trace import trace_task, trace_task_ret
from celery.exceptions import (
    Ignore, TaskRevokedError, InvalidTaskError,
    SoftTimeLimitExceeded, TimeLimitExceeded,
    WorkerLostError, Terminated, Retry, Reject,
)
from celery.five import string
from celery.platforms import signals as _signals
from celery.utils.functional import noop
from celery.utils.log import get_logger
from celery.utils.timeutils import maybe_iso8601, timezone, maybe_make_aware
from celery.utils.serialization import get_pickled_exception

from . import state

__all__ = ['Request']

IS_PYPY = hasattr(sys, 'pypy_version_info')

logger = get_logger(__name__)
debug, info, warn, error = (logger.debug, logger.info,
                            logger.warning, logger.error)
_does_info = False
_does_debug = False

#: Max length of result representation
RESULT_MAXLEN = 128


def __optimize__():
    # this is also called by celery.app.trace.setup_worker_optimizations
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

#: Use when no message object passed to :class:`Request`.
DEFAULT_FIELDS = {
    'headers': None,
    'reply_to': None,
    'correlation_id': None,
    'delivery_info': {
        'exchange': None,
        'routing_key': None,
        'priority': 0,
        'redelivered': False,
    },
}


class RequestV1(object):
    if not IS_PYPY:
        __slots__ = (
            'app', 'message', 'name', 'id', 'root_id', 'parent_id',
            'on_ack', 'hostname', 'eventer', 'connection_errors', 'task',
            'eta', 'expires', 'request_dict', 'acknowledged', 'on_reject',
            'utc', 'time_start', 'worker_pid', '_already_revoked',
            '_terminate_on_ack', '_apply_result',
            '_tzlocal', '__weakref__', '__dict__',
        )


class Request(object):
    """A request for task execution."""
    utc = True
    if not IS_PYPY:  # pragma: no cover
        __slots__ = (
            'app', 'name', 'id', 'on_ack', 'body',
            'hostname', 'eventer', 'connection_errors', 'task', 'eta',
            'expires', 'request_dict', 'acknowledged', 'on_reject',
            'utc', 'time_start', 'worker_pid', 'timeouts',
            'content_type', 'content_encoding',
            '_already_revoked', '_terminate_on_ack', '_apply_result',
            '_tzlocal', '__weakref__', '__dict__',
        )

    def __init__(self, message, on_ack=noop,
                 hostname=None, eventer=None, app=None,
                 connection_errors=None, request_dict=None,
                 task=None, on_reject=noop, body=None, **opts):
        headers = message.headers
        self.app = app
        self.message = message
        name = self.name = headers['c_type']
        self.id = headers['id']
        self.body = message.body if body is None else body
        self.content_type = message.content_type
        self.content_encoding = message.content_encoding
        eta = headers.get('eta')
        expires = headers.get('expires')
        self.timeouts = (headers['timeouts'] if 'timeouts' in headers
                         else (None, None))
        self.on_ack = on_ack
        self.on_reject = on_reject
        self.hostname = hostname or socket.gethostname()
        self.eventer = eventer
        self.connection_errors = connection_errors or ()
        self.task = task or self.app.tasks[name]
        self.acknowledged = self._already_revoked = False
        self.time_start = self.worker_pid = self._terminate_on_ack = None
        self._apply_result = None
        self._tzlocal = None

        # timezone means the message is timezone-aware, and the only timezone
        # supported at this point is UTC.
        if eta is not None:
            try:
                eta = maybe_iso8601(eta)
            except (AttributeError, ValueError, TypeError) as exc:
                raise InvalidTaskError(
                    'invalid eta value {0!r}: {1}'.format(eta, exc))
            self.eta = maybe_make_aware(eta, self.tzlocal)
        else:
            self.eta = None
        if expires is not None:
            try:
                expires = maybe_iso8601(expires)
            except (AttributeError, ValueError, TypeError) as exc:
                raise InvalidTaskError(
                    'invalid expires value {0!r}: {1}'.format(expires, exc))
            self.expires = maybe_make_aware(expires, self.tzlocal)
        else:
            self.expires = None

        delivery_info = message.delivery_info or {}
        properties = message.properties or {}
        headers.update({
            'reply_to': properties.get('reply_to'),
            'correlation_id': properties.get('correlation_id'),
            'delivery_info': {
                'exchange': delivery_info.get('exchange'),
                'routing_key': delivery_info.get('routing_key'),
                'priority': delivery_info.get('priority'),
                'redelivered': delivery_info.get('redelivered'),
            }

        })
        self.request_dict = headers

    @property
    def delivery_info(self):
        return self.request_dict['delivery_info']

    def execute_using_pool(self, pool, **kwargs):
        """Used by the worker to send this task to the pool.

        :param pool: A :class:`celery.concurrency.base.TaskPool` instance.

        :raises celery.exceptions.TaskRevokedError: if the task was revoked
            and ignored.

        """
        task_id = self.id
        task = self.task
        if self.revoked():
            raise TaskRevokedError(task_id)

        body = self.body
        timeout, soft_timeout = self.timeouts
        timeout = timeout or task.time_limit
        soft_timeout = soft_timeout or task.soft_time_limit
        result = pool.apply_async(
            trace_task_ret,
            args=(self.name, task_id, self.request_dict, self.body,
                  self.content_type, self.content_encoding),
            kwargs={'hostname': self.hostname, 'is_eager': False},
            accept_callback=self.on_accepted,
            timeout_callback=self.on_timeout,
            callback=self.on_success,
            error_callback=self.on_failure,
            soft_timeout=soft_timeout or task.soft_time_limit,
            timeout=timeout or task.time_limit,
            correlation_id=task_id,
        )
        # cannot create weakref to None
        self._apply_result = ref(result) if result is not None else result
        return result

    def execute(self, loglevel=None, logfile=None):
        """Execute the task in a :func:`~celery.app.trace.trace_task`.

        :keyword loglevel: The loglevel used by the task.
        :keyword logfile: The logfile used by the task.

        """
        if self.revoked():
            return

        # acknowledge task as being processed.
        if not self.task.acks_late:
            self.acknowledge()

        request = self.request_dict
        args, kwargs = self.message.payload
        request.update({'loglevel': loglevel, 'logfile': logfile,
                        'hostname': self.hostname, 'is_eager': False,
                        'args': args, 'kwargs': kwargs})
        retval = trace_task(self.task, self.id, args, kwargs, request,
                            hostname=self.hostname, loader=self.app.loader,
                            app=self.app)[0]
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
        signal = _signals.signum(signal or 'TERM')
        if self.time_start:
            pool.terminate_job(self.worker_pid, signal)
            self._announce_revoked('terminated', True, signal, False)
        else:
            self._terminate_on_ack = pool, signal
        if self._apply_result is not None:
            obj = self._apply_result()  # is a weakref
            if obj is not None:
                obj.terminate(signal)

    def _announce_revoked(self, reason, terminated, signum, expired):
        task_ready(self)
        self.send_event('task-revoked',
                        terminated=terminated, signum=signum, expired=expired)
        if self.store_errors:
            self.task.backend.mark_as_revoked(self.id, reason, request=self)
        self.acknowledge()
        self._already_revoked = True
        send_revoked(self.task, request=self,
                     terminated=terminated, signum=signum, expired=expired)

    def revoked(self):
        """If revoked, skip task and mark state."""
        expired = False
        if self._already_revoked:
            return True
        if self.expires:
            expired = self.maybe_expire()
        if self.id in revoked_tasks:
            info('Discarding revoked task: %s[%s]', self.name, self.id)
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
        self.send_event('task-started')
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
            self.task.backend.mark_as_failure(self.id, exc, request=self)

        if self.task.acks_late:
            self.acknowledge()

    def on_success(self, failed__retval__runtime, **kwargs):
        """Handler called if the task was successfully processed."""
        failed, retval, runtime = failed__retval__runtime
        if failed:
            if isinstance(retval.exception, (SystemExit, KeyboardInterrupt)):
                raise retval.exception
            return self.on_failure(retval)
        task_ready(self)

        if self.task.acks_late:
            self.acknowledge()

        if self.eventer and self.eventer.enabled:
            self.send_event(
                'task-succeeded', result=retval, runtime=runtime,
            )

    def on_retry(self, exc_info):
        """Handler called if the task should be retried."""
        if self.task.acks_late:
            self.acknowledge()

        self.send_event('task-retried',
                        exception=safe_repr(exc_info.exception.exc),
                        traceback=safe_str(exc_info.traceback))

    def on_failure(self, exc_info, send_failed_event=True):
        """Handler called if the task raised an exception."""
        task_ready(self)

        if isinstance(exc_info.exception, MemoryError):
            raise MemoryError('Process got: %s' % (exc_info.exception, ))
        elif isinstance(exc_info.exception, Reject):
            return self.reject(requeue=exc_info.exception.requeue)
        elif isinstance(exc_info.exception, Ignore):
            return self.acknowledge()

        exc = exc_info.exception

        if isinstance(exc, Retry):
            return self.on_retry(exc_info)

        # These are special cases where the process would not have had
        # time to write the result.
        if self.store_errors:
            if isinstance(exc, WorkerLostError):
                self.task.backend.mark_as_failure(
                    self.id, exc, request=self,
                )
            elif isinstance(exc, Terminated):
                self._announce_revoked(
                    'terminated', True, string(exc), False)
                send_failed_event = False  # already sent revoked event
        # (acks_late) acknowledge after result stored.
        if self.task.acks_late:
            self.acknowledge()

        if send_failed_event:
            self.send_event(
                'task-failed',
                exception=safe_repr(get_pickled_exception(exc_info.exception)),
                traceback=exc_info.traceback,
            )

    def acknowledge(self):
        """Acknowledge task."""
        if not self.acknowledged:
            self.on_ack(logger, self.connection_errors)
            self.acknowledged = True

    def reject(self, requeue=False):
        if not self.acknowledged:
            self.on_reject(logger, self.connection_errors, requeue)
            self.acknowledged = True

    def info(self, safe=False):
        return {'id': self.id,
                'name': self.name,
                'body': self.body,
                'hostname': self.hostname,
                'time_start': self.time_start,
                'acknowledged': self.acknowledged,
                'delivery_info': self.delivery_info,
                'worker_pid': self.worker_pid}

    def __str__(self):
        return '{0.name}[{0.id}]{1}{2}'.format(self,
               ' eta:[{0}]'.format(self.eta) if self.eta else '',
               ' expires:[{0}]'.format(self.expires) if self.expires else '')
    shortinfo = __str__

    def __repr__(self):
        return '<{0} {1}: {2}>'.format(type(self).__name__, self.id, self.name)

    @property
    def tzlocal(self):
        if self._tzlocal is None:
            self._tzlocal = self.app.conf.CELERY_TIMEZONE
        return self._tzlocal

    @property
    def store_errors(self):
        return (not self.task.ignore_result
                or self.task.store_errors_even_if_ignored)

    @property
    def task_id(self):
        # XXX compat
        return self.id

    @task_id.setter  # noqa
    def task_id(self, value):
        self.id = value

    @property
    def task_name(self):
        # XXX compat
        return self.name

    @task_name.setter  # noqa
    def task_name(self, value):
        self.name = value

    @property
    def reply_to(self):
        # used by rpc backend when failures reported by parent process
        return self.request_dict['reply_to']

    @property
    def correlation_id(self):
        # used similarly to reply_to
        return self.request_dict['correlation_id']
