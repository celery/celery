# -*- coding: utf-8 -*-
"""
    celery.execute.trace
    ~~~~~~~~~~~~~~~~~~~~

    This module defines how the task execution is traced:
    errors are recorded, handlers are applied and so on.

    :copyright: (c) 2009 - 2011 by Ask Solem.
    :license: BSD, see LICENSE for more details.

"""
from __future__ import absolute_import

import os
import socket
import sys
import traceback
import warnings

from .. import current_app
from .. import states, signals
from ..datastructures import ExceptionInfo
from ..exceptions import RetryTaskError
from ..registry import tasks
from ..utils.serialization import get_pickleable_exception

send_prerun = signals.task_prerun.send
send_postrun = signals.task_postrun.send
SUCCESS = states.SUCCESS
RETRY = states.RETRY
FAILURE = states.FAILURE
EXCEPTION_STATES = states.EXCEPTION_STATES


class TraceInfo(object):
    __slots__ = ("state", "retval", "exc_info",
                 "exc_type", "exc_value", "tb", "strtb")

    def __init__(self, state, retval=None, exc_info=None):
        self.state = state
        self.retval = retval
        self.exc_info = exc_info
        if exc_info:
            self.exc_type, self.exc_value, self.tb = exc_info
        else:
            self.exc_type = self.exc_value = self.tb = None

    @property
    def strtb(self):
        if self.exc_info:
            return "\n".join(traceback.format_exception(*self.exc_info))


def trace(fun, args, kwargs, propagate=False, Info=TraceInfo):
    """Trace the execution of a function, calling the appropiate callback
    if the function raises retry, an failure or returned successfully.

    :keyword propagate: If true, errors will propagate to the caller.

    """
    try:
        return Info(states.SUCCESS, retval=fun(*args, **kwargs))
    except RetryTaskError, exc:
        return Info(states.RETRY, retval=exc, exc_info=sys.exc_info())
    except Exception, exc:
        if propagate:
            raise
        return Info(states.FAILURE, retval=exc, exc_info=sys.exc_info())
    except BaseException, exc:
        raise
    except:  # pragma: no cover
        # For Python2.5 where raising strings are still allowed
        # (but deprecated)
        if propagate:
            raise
        return Info(states.FAILURE, retval=None, exc_info=sys.exc_info())


class TaskTrace(object):
    """Wraps the task in a jail, catches all exceptions, and
    saves the status and result of the task execution to the task
    meta backend.

    If the call was successful, it saves the result to the task result
    backend, and sets the task status to `"SUCCESS"`.

    If the call raises :exc:`~celery.exceptions.RetryTaskError`, it extracts
    the original exception, uses that as the result and sets the task status
    to `"RETRY"`.

    If the call results in an exception, it saves the exception as the task
    result, and sets the task status to `"FAILURE"`.

    :param task_name: The name of the task to execute.
    :param task_id: The unique id of the task.
    :param args: List of positional args to pass on to the function.
    :param kwargs: Keyword arguments mapping to pass on to the function.

    :keyword loader: Custom loader to use, if not specified the current app
      loader will be used.
    :keyword hostname: Custom hostname to use, if not specified the system
      hostname will be used.

    :returns: the evaluated functions return value on success, or
        the exception instance on failure.

    """

    def __init__(self, task_name, task_id, args, kwargs, task=None,
            request=None, propagate=None, propagate_internal=False,
            eager=False, **_):
        self.task_id = task_id
        self.task_name = task_name
        self.args = args
        self.kwargs = kwargs
        self.task = task or tasks[self.task_name]
        self.request = request or {}
        self.propagate = propagate
        self.propagate_internal = propagate_internal
        self.eager = eager
        self._trace_handlers = {FAILURE: self.handle_failure,
                                RETRY: self.handle_retry}
        self.loader = kwargs.get("loader") or current_app.loader
        self.hostname = kwargs.get("hostname") or socket.gethostname()
        self._store_errors = True
        if eager:
            self._store_errors = False
        elif self.task.ignore_result:
            self._store_errors = self.task.store_errors_even_if_ignored

        # Used by always_eager
        self.state = None
        self.strtb = None

    def __call__(self):
        try:
            task, uuid, args, kwargs, eager = (self.task, self.task_id,
                                               self.args, self.kwargs,
                                               self.eager)
            backend = task.backend
            ignore_result = task.ignore_result
            loader = self.loader

            task.request.update(self.request, args=args,
                                called_directly=False, kwargs=kwargs)
            try:
                # -*- PRE -*-
                send_prerun(sender=task, task_id=uuid, task=task,
                            args=args, kwargs=kwargs)
                loader.on_task_init(uuid, task)
                if not eager and (task.track_started and not ignore_result):
                    backend.mark_as_started(uuid, pid=os.getpid(),
                                            hostname=self.hostname)

                # -*- TRACE -*-
                # self.info is used by always_eager
                info = self.info = trace(task, args, kwargs, self.propagate)
                state, retval, einfo = info.state, info.retval, info.exc_info
                if eager:
                    self.state, self.strtb = info.state, info.strtb

                if state == SUCCESS:
                    task.on_success(retval, uuid, args, kwargs)
                    if not eager and not ignore_result:
                        backend.mark_as_done(uuid, retval)
                    R = retval
                else:
                    R = self.handle_trace(info)

                # -* POST *-
                if state in EXCEPTION_STATES:
                    einfo = ExceptionInfo(einfo)
                task.after_return(state, retval, self.task_id,
                                  self.args, self.kwargs, einfo)
                send_postrun(sender=task, task_id=uuid, task=task,
                             args=args, kwargs=kwargs, retval=retval)
            finally:
                task.request.clear()
                if not eager:
                    try:
                        backend.process_cleanup()
                        loader.on_process_cleanup()
                    except (KeyboardInterrupt, SystemExit, MemoryError):
                        raise
                    except Exception, exc:
                        logger = current_app.log.get_default_logger()
                        logger.error("Process cleanup failed: %r", exc,
                                    exc_info=sys.exc_info())
        except Exception, exc:
            if self.propagate_internal:
                raise
            return self.report_internal_error(exc)
        return R
    execute = __call__

    def report_internal_error(self, exc):
        _type, _value, _tb = sys.exc_info()
        _value = self.task.backend.prepare_exception(exc)
        exc_info = ExceptionInfo((_type, _value, _tb))
        warnings.warn("Exception outside body: %s: %s\n%s" % tuple(
            map(str, (exc.__class__, exc, exc_info.traceback))))
        return exc_info

    def handle_trace(self, trace):
        return self._trace_handlers[trace.state](trace.retval, trace.exc_type,
                                                 trace.tb, trace.strtb)

    def handle_retry(self, exc, type_, tb, strtb):
        """Handle retry exception."""
        # Create a simpler version of the RetryTaskError that stringifies
        # the original exception instead of including the exception instance.
        # This is for reporting the retry in logs, email etc, while
        # guaranteeing pickleability.
        message, orig_exc = exc.args
        if self._store_errors:
            self.task.backend.mark_as_retry(self.task_id, orig_exc, strtb)
        expanded_msg = "%s: %s" % (message, str(orig_exc))
        einfo = ExceptionInfo((type_, type_(expanded_msg, None), tb))
        self.task.on_retry(exc, self.task_id, self.args, self.kwargs, einfo)
        return einfo

    def handle_failure(self, exc, type_, tb, strtb):
        """Handle exception."""
        if self._store_errors:
            self.task.backend.mark_as_failure(self.task_id, exc, strtb)
        exc = get_pickleable_exception(exc)
        einfo = ExceptionInfo((type_, exc, tb))
        self.task.on_failure(exc, self.task_id, self.args, self.kwargs, einfo)
        signals.task_failure.send(sender=self.task, task_id=self.task_id,
                                  exception=exc, args=self.args,
                                  kwargs=self.kwargs, traceback=tb,
                                  einfo=einfo)
        return einfo
