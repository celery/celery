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
STARTED = states.STARTED
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


def build_tracer(name, task, loader=None, hostname=None, store_errors=True,
        Info=TraceInfo, eager=False, propagate=False):
    task = task or tasks[name]
    loader = loader or current_app.loader
    backend = task.backend
    ignore_result = task.ignore_result
    track_started = task.track_started
    track_started = not eager and (task.track_started and not ignore_result)
    publish_result = not eager and not ignore_result
    hostname = hostname or socket.gethostname()

    loader_task_init = loader.on_task_init
    loader_cleanup = loader.on_process_cleanup

    task_on_success = task.on_success
    task_after_return = task.after_return
    task_request = task.request

    store_result = backend.store_result
    backend_cleanup = backend.process_cleanup

    pid = os.getpid()

    update_request = task_request.update
    clear_request = task_request.clear
    on_chord_part_return = backend.on_chord_part_return

    def trace_task(uuid, args, kwargs, request=None):
        R = I = None
        try:
            update_request(request or {}, args=args,
                           called_directly=False, kwargs=kwargs)
            try:
                # -*- PRE -*-
                send_prerun(sender=task, task_id=uuid, task=task,
                            args=args, kwargs=kwargs)
                loader_task_init(uuid, task)
                if track_started:
                    store_result(uuid, {"pid": pid,
                                        "hostname": hostname}, STARTED)

                # -*- TRACE -*-
                try:
                    R = retval = task(*args, **kwargs)
                    state, einfo = SUCCESS, None
                    task_on_success(retval, uuid, args, kwargs)
                    if publish_result:
                        store_result(uuid, retval, SUCCESS)
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
                if task_request.chord:
                    on_chord_part_return(task)
                task_after_return(state, retval, uuid, args, kwargs, einfo)
                send_postrun(sender=task, task_id=uuid, task=task,
                            args=args, kwargs=kwargs, retval=retval)
            finally:
                clear_request()
                if not eager:
                    try:
                        backend_cleanup()
                        loader_cleanup()
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

    return trace_task


def trace_task(task, uuid, args, kwargs, request=None, **opts):
    try:
        if task.__tracer__ is None:
            task.__tracer__ = build_tracer(task.name, task, **opts)
        return task.__tracer__(uuid, args, kwargs, request)
    except Exception, exc:
        return report_internal_error(task, exc), None


def eager_trace_task(task, uuid, args, kwargs, request=None, **opts):
    opts.setdefault("eager", True)
    return build_tracer(task.name, task, **opts)(
            uuid, args, kwargs, request)


def report_internal_error(task, exc):
    _type, _value, _tb = sys.exc_info()
    _value = task.backend.prepare_exception(exc)
    exc_info = ExceptionInfo((_type, _value, _tb))
    warnings.warn("Exception outside body: %s: %s\n%s" % tuple(
        map(str, (exc.__class__, exc, exc_info.traceback))))
    return exc_info
