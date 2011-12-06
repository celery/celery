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

# ## ---
# BE WARNED: You are probably going to suffer a heartattack just
#            by looking at this code!
#
# This is the heart of the worker, the inner loop so to speak.
# It used to be split up into nice little classes and methods,
# but in the end it only resulted in bad performance, and horrible tracebacks.

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
prerun_receivers = signals.task_prerun.receivers
send_postrun = signals.task_postrun.send
postrun_receivers = signals.task_postrun.receivers
SUCCESS = states.SUCCESS
RETRY = states.RETRY
FAILURE = states.FAILURE
EXCEPTION_STATES = states.EXCEPTION_STATES
_pid = None


def getpid():
    global _pid
    if _pid is None:
        _pid = os.getpid()
    return _pid


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

    def handle_error_state(self, task, eager=False):
        store_errors = not eager
        if task.ignore_result:
            store_errors = task.store_errors_even_if_ignored

        return {
            RETRY: self.handle_retry,
            FAILURE: self.handle_failure,
        }[self.state](task, store_errors=store_errors)

    def handle_retry(self, task, store_errors=True):
        """Handle retry exception."""
        # Create a simpler version of the RetryTaskError that stringifies
        # the original exception instead of including the exception instance.
        # This is for reporting the retry in logs, email etc, while
        # guaranteeing pickleability.
        req = task.request
        exc, type_, tb = self.retval, self.exc_type, self.tb
        message, orig_exc = self.retval.args
        if store_errors:
            task.backend.mark_as_retry(req.id, orig_exc, self.strtb)
        expanded_msg = "%s: %s" % (message, str(orig_exc))
        einfo = ExceptionInfo((type_, type_(expanded_msg, None), tb))
        task.on_retry(exc, req.id, req.args, req.kwargs, einfo)
        return einfo

    def handle_failure(self, task, store_errors=True):
        """Handle exception."""
        req = task.request
        exc, type_, tb = self.retval, self.exc_type, self.tb
        if store_errors:
            task.backend.mark_as_failure(req.id, exc, self.strtb)
        exc = get_pickleable_exception(exc)
        einfo = ExceptionInfo((type_, exc, tb))
        task.on_failure(exc, req.id, req.args, req.kwargs, einfo)
        signals.task_failure.send(sender=task, task_id=req.id,
                                  exception=exc, args=req.args,
                                  kwargs=req.kwargs, traceback=tb,
                                  einfo=einfo)
        return einfo

    @property
    def strtb(self):
        if self.exc_info:
            return '\n'.join(traceback.format_exception(*self.exc_info))
        return ''

def trace_task(name, uuid, args, kwargs, task=None, request=None,
        eager=False, propagate=False, loader=None, hostname=None,
        store_errors=True, Info=TraceInfo):
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
    R = I = None
    try:
        task = task or tasks[name]
        backend = task.backend
        ignore_result = task.ignore_result
        loader = loader or current_app.loader
        hostname = hostname or socket.gethostname()
        task.request.update(request or {}, args=args,
                            called_directly=False, kwargs=kwargs)
        try:
            # -*- PRE -*-
            send_prerun(sender=task, task_id=uuid, task=task,
                        args=args, kwargs=kwargs)
            loader.on_task_init(uuid, task)
            if not eager and (task.track_started and not ignore_result):
                backend.mark_as_started(uuid, pid=getpid(),
                                        hostname=hostname)

            # -*- TRACE -*-
            try:
                R = retval = task(*args, **kwargs)
                state, einfo = SUCCESS, None
                task.on_success(retval, uuid, args, kwargs)
                if not eager and not ignore_result:
                    backend.mark_as_done(uuid, retval)
            except RetryTaskError, exc:
                I = Info(RETRY, exc, sys.exc_info())
                state, retval, einfo = I.state, I.retval, I.exc_info
                R = I.handle_error_state(task, eager=eager)
            except Exception, exc:
                if propagate:
                    raise
                I = Info(FAILURE, exc, sys.exc_info())
                state, retval, einfo = I.state, I.retval, I.exc_info
                R = I.handle_error_state(task, eager=eager)
            except BaseException, exc:
                raise
            except:
                # pragma: no cover
                # For Python2.5 where raising strings are still allowed
                # (but deprecated)
                if propagate:
                    raise
                I = Info(FAILURE, None, sys.exc_info())
                state, retval, einfo = I.state, I.retval, I.exc_info
                R = I.handle_error_state(task, eager=eager)

            # -* POST *-
            if task.request.chord:
                backend.on_chord_part_return(task)
            task.after_return(state, retval, uuid, args, kwargs, einfo)
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
        if eager:
            raise
        R = report_internal_error(task, exc)
    return R, I


def report_internal_error(task, exc):
    _type, _value, _tb = sys.exc_info()
    _value = task.backend.prepare_exception(exc)
    exc_info = ExceptionInfo((_type, _value, _tb))
    warnings.warn("Exception outside body: %s: %s\n%s" % tuple(
        map(str, (exc.__class__, exc, exc_info.traceback))))
    return exc_info
