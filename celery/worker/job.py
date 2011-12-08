# -*- coding: utf-8 -*-
"""
    celery.worker.job
    ~~~~~~~~~~~~~~~~~

    This module defines the :class:`TaskRequest` class,
    which specifies how tasks are executed.

    :copyright: (c) 2009 - 2011 by Ask Solem.
    :license: BSD, see LICENSE for more details.

"""
from __future__ import absolute_import

import logging
import time
import socket

from datetime import datetime

from .. import exceptions
from ..registry import tasks
from ..app import app_or_default
from ..execute.trace import build_tracer, trace_task, report_internal_error
from ..platforms import set_mp_process_title as setps
from ..utils import (noop, kwdict, fun_takes_kwargs,
                     cached_property, truncate_text)
from ..utils.encoding import safe_repr, safe_str
from ..utils.timeutils import maybe_iso8601, timezone

from . import state

# Localize
tz_to_local = timezone.to_local
tz_or_local = timezone.tz_or_local
tz_utc = timezone.utc


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


class TaskRequest(object):
    """A request for task execution."""

    #: Kind of task.  Must be a name registered in the task registry.
    name = None

    #: The task class (set by constructor using :attr:`name`).
    task = None

    #: UUID of the task.
    id = None

    #: UUID of the taskset that this task belongs to.
    taskset = None

    #: List of positional arguments to apply to the task.
    args = None

    #: Mapping of keyword arguments to apply to the task.
    kwargs = None

    #: Number of times the task has been retried.
    retries = 0

    #: The tasks eta (for information only).
    eta = None

    #: When the task expires.
    expires = None

    #: Body of a chord depending on this task.
    chord = None

    #: Callback called when the task should be acknowledged.
    on_ack = None

    #: The message object.  Used to acknowledge the message.
    message = None

    #: Additional delivery info, e.g. contains the path from
    #: Producer to consumer.
    delivery_info = None

    #: Flag set when the task has been acknowledged.
    acknowledged = False

    #: Format string used to log task success.
    success_msg = """\
        Task %(name)s[%(id)s] succeeded in %(runtime)ss: %(return_value)s
    """

    #: Format string used to log task failure.
    error_msg = """\
        Task %(name)s[%(id)s] raised exception: %(exc)s\n%(traceback)s
    """

    #: Format string used to log task retry.
    retry_msg = """Task %(name)s[%(id)s] retry: %(exc)s"""

    #: Timestamp set when the task is started.
    time_start = None

    #: Process id of the worker processing this task (if any).
    worker_pid = None

    _already_revoked = False
    _terminate_on_ack = None

    def __init__(self, task, id, args=[], kwargs={},
            on_ack=noop, retries=0, delivery_info={}, hostname=None,
            logger=None, eventer=None, eta=None, expires=None, app=None,
            taskset=None, chord=None, utc=False, connection_errors=None,
            **opts):
        try:
            kwargs.items
        except AttributeError:
            raise exceptions.InvalidTaskError(
                    "Task keyword arguments is not a mapping")
        self.app = app or app_or_default(app)
        self.name = task
        self.id = id
        self.args = args
        self.kwargs = kwdict(kwargs)
        self.taskset = taskset
        self.retries = retries
        self.chord = chord
        self.on_ack = on_ack
        self.delivery_info = {} if delivery_info is None else delivery_info
        self.hostname = hostname or socket.gethostname()
        self.logger = logger or self.app.log.get_default_logger()
        self.eventer = eventer
        self.connection_errors = connection_errors or ()

        task = self.task = tasks[task]

        # timezone means the message is timezone-aware, and the only timezone
        # supported at this point is UTC.
        if eta is not None:
            tz = tz_utc if utc else self.tzlocal
            self.eta = tz_to_local(maybe_iso8601(eta), self.tzlocal, tz)
        if expires is not None:
            tz = tz_utc if utc else self.tzlocal
            self.expires = tz_to_local(maybe_iso8601(expires),
                                       self.tzlocal, tz)

        # shortcuts
        self._does_debug = self.logger.isEnabledFor(logging.DEBUG)
        self._does_info = self.logger.isEnabledFor(logging.INFO)

        self.request_dict = {"hostname": self.hostname,
                             "id": id, "taskset": taskset,
                             "retries": retries, "is_eager": False,
                             "delivery_info": delivery_info, "chord": chord}

    @classmethod
    def from_message(cls, message, body, on_ack=noop, delivery_info={},
            logger=None, hostname=None, eventer=None, app=None,
            connection_errors=None):
        """Create request from a task message.

        :raises UnknownTaskError: if the message does not describe a task,
            the message is also rejected.

        """
        try:
            D = message.delivery_info
            delivery_info = {"exchange": D.get("exchange"),
                             "routing_key": D.get("routing_key")}
        except (AttributeError, KeyError):
            pass

        try:
            return cls(on_ack=on_ack, logger=logger, eventer=eventer, app=app,
                       delivery_info=delivery_info, hostname=hostname,
                       connection_errors=connection_errors, **body)
        except TypeError:
            for f in ("task", "id"):
                if f not in body:
                    raise exceptions.InvalidTaskError(
                        "Task message is missing required field %r" % (f, ))

    def get_instance_attrs(self, loglevel, logfile):
        return {"logfile": logfile, "loglevel": loglevel,
                "hostname": self.hostname,
                "id": self.id, "taskset": self.taskset,
                "retries": self.retries, "is_eager": False,
                "delivery_info": self.delivery_info, "chord": self.chord}

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
                          "task_retries": self.retries,
                          "task_is_eager": False,
                          "delivery_info": self.delivery_info}
        fun = self.task.run
        supported_keys = fun_takes_kwargs(fun, default_kwargs)
        extend_with = dict((key, val) for key, val in default_kwargs.items()
                                if key in supported_keys)
        kwargs.update(extend_with)
        return kwargs

    def execute_using_pool(self, pool, loglevel=None, logfile=None):
        """Like :meth:`execute`, but using the :mod:`multiprocessing` pool.

        :param pool: A :class:`multiprocessing.Pool` instance.

        :keyword loglevel: The loglevel used by the task.

        :keyword logfile: The logfile used by the task.

        """
        if self.revoked():
            return
        request = self.request_dict

        kwargs = self.kwargs
        if self.task.accept_magic_kwargs:
            kwargs = self.extend_with_default_kwargs(loglevel, logfile)
        request.update({"loglevel": loglevel, "logfile": logfile})
        result = pool.apply_async(execute_and_trace,
                                  args=(self.name, self.id, self.args, kwargs),
                                  kwargs={"hostname": self.hostname,
                                          "request": request},
                                  accept_callback=self.on_accepted,
                                  timeout_callback=self.on_timeout,
                                  callback=self.on_success,
                                  errback=self.on_failure,
                                  soft_timeout=self.task.soft_time_limit,
                                  timeout=self.task.time_limit)
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

        instance_attrs = self.get_instance_attrs(loglevel, logfile)
        retval, _ = trace_task(*self._get_tracer_args(loglevel, logfile, True),
                               **{"hostname": self.hostname,
                                  "loader": self.app.loader,
                                  "request": instance_attrs})
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
        if self.eventer:
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

    def on_success(self, ret_value):
        """Handler called if the task was successfully processed."""
        state.task_ready(self)

        if self.task.acks_late:
            self.acknowledge()

        runtime = self.time_start and (time.time() - self.time_start) or 0
        self.send_event("task-succeeded", uuid=self.id,
                        result=safe_repr(ret_value), runtime=runtime)

        if self._does_info:
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

        if self.task.acks_late:
            self.acknowledge()

        if isinstance(exc_info.exception, exceptions.RetryTaskError):
            return self.on_retry(exc_info)

        # This is a special case as the process would not have had
        # time to write the result.
        if isinstance(exc_info.exception, exceptions.WorkerLostError) and \
                self.store_errors:
            self.task.backend.mark_as_failure(self.id, exc_info.exception)

        self.send_event("task-failed", uuid=self.id,
                         exception=safe_repr(exc_info.exception),
                         traceback=safe_str(exc_info.traceback))

        context = {"hostname": self.hostname,
                   "id": self.id,
                   "name": self.name,
                   "exc": safe_repr(exc_info.exception),
                   "traceback": safe_str(exc_info.traceback),
                   "args": safe_repr(self.args),
                   "kwargs": safe_repr(self.kwargs)}

        self.logger.error(self.error_msg.strip(), context,
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

    def _get_tracer_args(self, loglevel=None, logfile=None, use_real=False):
        """Get the task trace args for this task."""
        kwargs = self.kwargs
        if self.task.accept_magic_kwargs:
            kwargs = self.extend_with_default_kwargs(loglevel, logfile)
        first = self.task if use_real else self.name
        return first, self.id, self.args, kwargs

    @cached_property
    def tzlocal(self):
        return tz_or_local(self.app.conf.CELERY_TIMEZONE)

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

    def _compat_get_taskset_id(self):
        return self.taskset

    def _compat_set_taskset_id(self, value):
        self.taskset = value

    task_id = property(_compat_get_task_id, _compat_set_task_id)
    task_name = property(_compat_get_task_name, _compat_set_task_name)
    taskset_id = property(_compat_get_taskset_id, _compat_set_taskset_id)
