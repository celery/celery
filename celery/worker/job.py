# -*- coding: utf-8 -*-
"""
    celery.worker.job
    ~~~~~~~~~~~~~~~~~

    This module defines the :class:`Request` class,
    which specifies how tasks are executed.

    :copyright: (c) 2009 - 2012 by Ask Solem.
    :license: BSD, see LICENSE for more details.

"""
from __future__ import absolute_import

import logging
import time
import socket
import sys

from datetime import datetime

from .. import exceptions
from ..datastructures import ExceptionInfo
from ..registry import tasks
from ..app import app_or_default
from ..execute.trace import build_tracer, trace_task, report_internal_error
from ..platforms import set_mp_process_title as setps
from ..utils import noop, kwdict, fun_takes_kwargs, truncate_text
from ..utils.encoding import safe_repr, safe_str
from ..utils.timeutils import maybe_iso8601, timezone

from . import state

# Localize
tz_to_local = timezone.to_local
tz_or_local = timezone.tz_or_local
tz_utc = timezone.utc

NEEDS_KWDICT = sys.version_info <= (2, 6)


def execute_and_trace(name, uuid, args, kwargs, request=None, **opts):
    """This is a pickleable method used as a target when applying to pools.

    It's the same as::

        >>> trace_task(name, *args, **kwargs)[0]

    """
    task = tasks[name]
    try:
        hostname = opts.get("hostname")
        setps("celeryd", name, hostname, rate_limit=True)
        try:
            if task.__tracer__ is None:
                task.__tracer__ = build_tracer(name, task, **opts)
            return task.__tracer__(uuid, args, kwargs, request)[0]
        finally:
            setps("celeryd", "-idle-", hostname, rate_limit=True)
    except Exception, exc:
        return report_internal_error(task, exc)


class Request(object):
    """A request for task execution."""
    __slots__ = ("app", "name", "id", "args", "kwargs",
                 "on_ack", "delivery_info", "hostname",
                 "logger", "eventer", "connection_errors",
                 "task", "eta", "expires",
                 "_does_debug", "_does_info", "request_dict",
                 "acknowledged", "success_msg", "error_msg",
                 "retry_msg", "time_start", "worker_pid",
                 "_already_revoked", "_terminate_on_ack", "_tzlocal")

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

    #: Format string used to log task retry.
    retry_msg = """Task %(name)s[%(id)s] retry: %(exc)s"""

    def __init__(self, body, on_ack=noop,
            hostname=None, logger=None, eventer=None, app=None,
            connection_errors=None, request_dict=None,
            delivery_info=None, task=None, **opts):
        self.app = app or app_or_default(app)
        name = self.name = body["task"]
        self.id = body["id"]
        self.args = body.get("args", [])
        self.kwargs = body.get("kwargs", {})
        try:
            self.kwargs.items
        except AttributeError:
            raise exceptions.InvalidTaskError(
                    "Task keyword arguments is not a mapping")
        if NEEDS_KWDICT:
            self.kwargs = kwdict(self.kwargs)
        eta = body.get("eta")
        expires = body.get("expires")
        utc = body.get("utc", False)
        self.on_ack = on_ack
        self.hostname = hostname or socket.gethostname()
        self.logger = logger or self.app.log.get_default_logger()
        self.eventer = eventer
        self.connection_errors = connection_errors or ()
        self.task = task or tasks[name]
        self.acknowledged = self._already_revoked = False
        self.time_start = self.worker_pid = self._terminate_on_ack = None
        self._tzlocal = None

        # timezone means the message is timezone-aware, and the only timezone
        # supported at this point is UTC.
        if eta is not None:
            tz = tz_utc if utc else self.tzlocal
            self.eta = tz_to_local(maybe_iso8601(eta), self.tzlocal, tz)
        else:
            self.eta = None
        if expires is not None:
            tz = tz_utc if utc else self.tzlocal
            self.expires = tz_to_local(maybe_iso8601(expires),
                                       self.tzlocal, tz)
        else:
            self.expires = None

        delivery_info = {} if delivery_info is None else delivery_info
        self.delivery_info = {
            "exchange": delivery_info.get("exchange"),
            "routing_key": delivery_info.get("routing_key"),
        }

        ## shortcuts
        self._does_debug = self.logger.isEnabledFor(logging.DEBUG)
        self._does_info = self.logger.isEnabledFor(logging.INFO)

        self.request_dict = body

    @classmethod
    def from_message(cls, message, body, **kwargs):
        # should be deprecated
        return Request(body,
                   delivery_info=getattr(message, "delivery_info", None),
                   **kwargs)

    def extend_with_default_kwargs(self, loglevel, logfile):
        """Extend the tasks keyword arguments with standard task arguments.

        Currently these are `logfile`, `loglevel`, `task_id`,
        `task_name`, `task_retries`, and `delivery_info`.

        See :meth:`celery.task.base.Task.run` for more information.

        Magic keyword arguments are deprecated and will be removed
        in version 3.0.

        """
        kwargs = dict(self.kwargs)
        default_kwargs = {"logfile": logfile,
                          "loglevel": loglevel,
                          "task_id": self.id,
                          "task_name": self.name,
                          "task_retries": self.request_dict.get("retries", 0),
                          "task_is_eager": False,
                          "delivery_info": self.delivery_info}
        fun = self.task.run
        supported_keys = fun_takes_kwargs(fun, default_kwargs)
        extend_with = dict((key, val) for key, val in default_kwargs.items()
                                if key in supported_keys)
        kwargs.update(extend_with)
        return kwargs

    def execute_using_pool(self, pool, loglevel=None, logfile=None):
        """Like :meth:`execute`, but using a worker pool.

        :param pool: A :class:`multiprocessing.Pool` instance.

        :keyword loglevel: The loglevel used by the task.

        :keyword logfile: The logfile used by the task.

        """
        if self.revoked():
            return

        task = self.task
        hostname = self.hostname
        kwargs = self.kwargs
        if self.task.accept_magic_kwargs:
            kwargs = self.extend_with_default_kwargs(loglevel, logfile)
        request = self.request_dict
        request.update({"loglevel": loglevel, "logfile": logfile,
                        "hostname": hostname, "is_eager": False,
                        "delivery_info": self.delivery_info})
        result = pool.apply_async(execute_and_trace,
                                  args=(self.name, self.id, self.args, kwargs),
                                  kwargs={"hostname": hostname,
                                          "request": request},
                                  accept_callback=self.on_accepted,
                                  timeout_callback=self.on_timeout,
                                  callback=self.on_success,
                                  error_callback=self.on_failure,
                                  soft_timeout=task.soft_time_limit,
                                  timeout=task.time_limit)
        return result

    def execute(self, loglevel=None, logfile=None):
        """Execute the task in a :func:`~celery.execute.trace.trace_task`.

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
            kwargs = self.extend_with_default_kwargs(loglevel, logfile)
        request = self.request_dict
        request.update({"loglevel": loglevel, "logfile": logfile,
                        "hostname": self.hostname, "is_eager": False,
                        "delivery_info": self.delivery_info})
        retval, _ = trace_task(self.task, self.id, self.args, kwargs,
                               **{"hostname": self.hostname,
                                  "loader": self.app.loader,
                                  "request": request})
        self.acknowledge()
        return retval

    def maybe_expire(self):
        """If expired, mark the task as revoked."""
        if self.expires and datetime.now(self.tzlocal) > self.expires:
            state.revoked.add(self.id)
            if self.store_errors:
                self.task.backend.mark_as_revoked(self.id)

    def terminate(self, pool, signal=None):
        if self.time_start:
            return pool.terminate_job(self.worker_pid, signal)
        else:
            self._terminate_on_ack = (True, pool, signal)

    def revoked(self):
        """If revoked, skip task and mark state."""
        if self._already_revoked:
            return True
        if self.expires:
            self.maybe_expire()
        if self.id in state.revoked:
            self.logger.warn("Skipping revoked task: %s[%s]",
                             self.name, self.id)
            self.send_event("task-revoked", uuid=self.id)
            self.acknowledge()
            self._already_revoked = True
            return True
        return False

    def send_event(self, type, **fields):
        if self.eventer and self.eventer.enabled:
            self.eventer.send(type, **fields)

    def on_accepted(self, pid, time_accepted):
        """Handler called when task is accepted by worker pool."""
        self.worker_pid = pid
        self.time_start = time_accepted
        state.task_accepted(self)
        if not self.task.acks_late:
            self.acknowledge()
        self.send_event("task-started", uuid=self.id, pid=pid)
        if self._does_debug:
            self.logger.debug("Task accepted: %s[%s] pid:%r",
                              self.name, self.id, pid)
        if self._terminate_on_ack is not None:
            _, pool, signal = self._terminate_on_ack
            self.terminate(pool, signal)

    def on_timeout(self, soft, timeout):
        """Handler called if the task times out."""
        state.task_ready(self)
        if soft:
            self.logger.warning("Soft time limit (%ss) exceeded for %s[%s]",
                                timeout, self.name, self.id)
            exc = exceptions.SoftTimeLimitExceeded(timeout)
        else:
            self.logger.error("Hard time limit (%ss) exceeded for %s[%s]",
                              timeout, self.name, self.id)
            exc = exceptions.TimeLimitExceeded(timeout)

        if self.store_errors:
            self.task.backend.mark_as_failure(self.id, exc)

    def on_success(self, ret_value, now=None):
        """Handler called if the task was successfully processed."""
        if isinstance(ret_value, ExceptionInfo):
            if isinstance(ret_value.exception, (
                    SystemExit, KeyboardInterrupt)):
                raise ret_value.exception
            return self.on_failure(ret_value)
        state.task_ready(self)

        if self.task.acks_late:
            self.acknowledge()

        if self.eventer and self.eventer.enabled:
            now = time.time()
            runtime = self.time_start and (time.time() - self.time_start) or 0
            self.send_event("task-succeeded", uuid=self.id,
                            result=safe_repr(ret_value), runtime=runtime)

        if self._does_info:
            now = now or time.time()
            runtime = self.time_start and (time.time() - self.time_start) or 0
            self.logger.info(self.success_msg.strip(),
                        {"id": self.id,
                        "name": self.name,
                        "return_value": self.repr_result(ret_value),
                        "runtime": runtime})

    def on_retry(self, exc_info):
        """Handler called if the task should be retried."""
        self.send_event("task-retried", uuid=self.id,
                         exception=safe_repr(exc_info.exception.exc),
                         traceback=safe_str(exc_info.traceback))

        if self._does_info:
            self.logger.info(self.retry_msg.strip(),
                            {"id": self.id,
                             "name": self.name,
                             "exc": safe_repr(exc_info.exception.exc)},
                            exc_info=exc_info)

    def on_failure(self, exc_info):
        """Handler called if the task raised an exception."""
        state.task_ready(self)

        if not exc_info.internal:

            if isinstance(exc_info.exception, exceptions.RetryTaskError):
                return self.on_retry(exc_info)

            # This is a special case as the process would not have had
            # time to write the result.
            if isinstance(exc_info.exception, exceptions.WorkerLostError) and \
                    self.store_errors:
                self.task.backend.mark_as_failure(self.id, exc_info.exception)
            # (acks_late) acknowledge after result stored.
            if self.task.acks_late:
                self.acknowledge()

        self._log_error(exc_info)

    def _log_error(self, exc_info):
        format = self.error_msg
        description = "raised exception"
        severity = logging.ERROR
        self.send_event("task-failed", uuid=self.id,
                         exception=safe_repr(exc_info.exception),
                         traceback=safe_str(exc_info.traceback))

        if exc_info.internal:
            format = self.internal_error_msg
            description = "INTERNAL ERROR"
            severity = logging.CRITICAL

        context = {"hostname": self.hostname,
                   "id": self.id,
                   "name": self.name,
                   "exc": safe_repr(exc_info.exception),
                   "traceback": safe_str(exc_info.traceback),
                   "args": safe_repr(self.args),
                   "kwargs": safe_repr(self.kwargs),
                   "description": description}

        self.logger.log(severity, format.strip(), context,
                        exc_info=exc_info.exc_info,
                        extra={"data": {"id": self.id,
                                        "name": self.name,
                                        "hostname": self.hostname}})

        task_obj = tasks.get(self.name, object)
        task_obj.send_error_email(context, exc_info.exception)

    def acknowledge(self):
        """Acknowledge task."""
        if not self.acknowledged:
            self.on_ack(self.logger, self.connection_errors)
            self.acknowledged = True

    def repr_result(self, result, maxlen=46):
        # 46 is the length needed to fit
        #     "the quick brown fox jumps over the lazy dog" :)
        return truncate_text(safe_repr(result), maxlen)

    def info(self, safe=False):
        return {"id": self.id,
                "name": self.name,
                "args": self.args if safe else safe_repr(self.args),
                "kwargs": self.kwargs if safe else safe_repr(self.kwargs),
                "hostname": self.hostname,
                "time_start": self.time_start,
                "acknowledged": self.acknowledged,
                "delivery_info": self.delivery_info,
                "worker_pid": self.worker_pid}

    def shortinfo(self):
        return "%s[%s]%s%s" % (
                    self.name, self.id,
                    " eta:[%s]" % (self.eta, ) if self.eta else "",
                    " expires:[%s]" % (self.expires, ) if self.expires else "")
    __str__ = shortinfo

    def __repr__(self):
        return '<%s: {name:"%s", id:"%s", args:"%s", kwargs:"%s"}>' % (
                self.__class__.__name__,
                self.name, self.id, self.args, self.kwargs)

    @property
    def tzlocal(self):
        if self._tzlocal is None:
            self._tzlocal = tz_or_local(self.app.conf.CELERY_TIMEZONE)
        return self._tzlocal

    @property
    def store_errors(self):
        return (not self.task.ignore_result
                or self.task.store_errors_even_if_ignored)

    def _compat_get_task_id(self):
        return self.id

    def _compat_set_task_id(self, value):
        self.id = value

    def _compat_get_task_name(self):
        return self.name

    def _compat_set_task_name(self, value):
        self.name = value

    task_id = property(_compat_get_task_id, _compat_set_task_id)
    task_name = property(_compat_get_task_name, _compat_set_task_name)


class TaskRequest(Request):

    def __init__(self, name, id, args=(), kwargs={},
            eta=None, expires=None, **options):
        """Compatibility class."""

        super(TaskRequest, self).__init__({
            "task": name, "id": id, "args": args,
            "kwargs": kwargs, "eta": eta,
            "expires": expires}, **options)
