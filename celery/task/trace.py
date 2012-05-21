# -*- coding: utf-8 -*-
"""
    celery.task.trace
    ~~~~~~~~~~~~~~~~~~~~

    This module defines how the task execution is traced:
    errors are recorded, handlers are applied and so on.

    :copyright: (c) 2009 - 2012 by Ask Solem.
    :license: BSD, see LICENSE for more details.

"""
from __future__ import absolute_import

# ## ---
# This is the heart of the worker, the inner loop so to speak.
# It used to be split up into nice little classes and methods,
# but in the end it only resulted in bad performance and horrible tracebacks,
# so instead we now use one closure per task class.

import os
import socket
import sys

from warnings import warn

from kombu.utils import kwdict

from celery import current_app
from celery import states, signals
from celery.state import _task_stack
from celery.app.task import BaseTask, Context
from celery.datastructures import ExceptionInfo
from celery.exceptions import RetryTaskError
from celery.utils.serialization import get_pickleable_exception
from celery.utils.log import get_logger

_logger = get_logger(__name__)

send_prerun = signals.task_prerun.send
prerun_receivers = signals.task_prerun.receivers
send_postrun = signals.task_postrun.send
postrun_receivers = signals.task_postrun.receivers
send_success = signals.task_success.send
success_receivers = signals.task_success.receivers
STARTED = states.STARTED
SUCCESS = states.SUCCESS
RETRY = states.RETRY
FAILURE = states.FAILURE
EXCEPTION_STATES = states.EXCEPTION_STATES


def mro_lookup(cls, attr, stop=()):
    """Returns the first node by MRO order that defines an attribute.

    :keyword stop: A list of types that if reached will stop the search.

    :returns None: if the attribute was not found.

    """
    for node in cls.mro():
        if node in stop:
            return
        if attr in node.__dict__:
            return node


def defines_custom_call(task):
    """Returns true if the task or one of its bases
    defines __call__ (excluding the one in BaseTask)."""
    return mro_lookup(task.__class__, "__call__", stop=(BaseTask, object))


class TraceInfo(object):
    __slots__ = ("state", "retval")

    def __init__(self, state, retval=None):
        self.state = state
        self.retval = retval

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
        type_, _, tb = sys.exc_info()
        try:
            exc = self.retval
            message, orig_exc = exc.args
            expanded_msg = "%s: %s" % (message, str(orig_exc))
            einfo = ExceptionInfo((type_, type_(expanded_msg, None), tb))
            if store_errors:
                task.backend.mark_as_retry(req.id, orig_exc, einfo.traceback)
            task.on_retry(exc, req.id, req.args, req.kwargs, einfo)
            return einfo
        finally:
            del(tb)

    def handle_failure(self, task, store_errors=True):
        """Handle exception."""
        req = task.request
        _, type_, tb = sys.exc_info()
        try:
            exc = self.retval
            einfo = ExceptionInfo((type_, get_pickleable_exception(exc), tb))
            if store_errors:
                task.backend.mark_as_failure(req.id, exc, einfo.traceback)
            task.on_failure(exc, req.id, req.args, req.kwargs, einfo)
            signals.task_failure.send(sender=task, task_id=req.id,
                                      exception=exc, args=req.args,
                                      kwargs=req.kwargs,
                                      traceback=einfo.traceback,
                                      einfo=einfo)
            return einfo
        finally:
            del(tb)


def execute_bare(task, uuid, args, kwargs, request=None):
    R = I = None
    kwargs = kwdict(kwargs)
    try:
        try:
            R = retval = task(*args, **kwargs)
            state = SUCCESS
        except Exception, exc:
            I = Info(FAILURE, exc)
            state, retval = I.state, I.retval
            R = I.handle_error_state(task)
        except BaseException, exc:
            raise
        except:  # pragma: no cover
            # For Python2.5 where raising strings are still allowed
            # (but deprecated)
            I = Info(FAILURE, None)
            state, retval = I.state, I.retval
            R = I.handle_error_state(task, eager=eager)
    except Exception, exc:
        R = report_internal_error(task, exc)
    return R


def build_tracer(name, task, loader=None, hostname=None, store_errors=True,
        Info=TraceInfo, eager=False, propagate=False):
    # If the task doesn't define a custom __call__ method
    # we optimize it away by simply calling the run method directly,
    # saving the extra method call and a line less in the stack trace.
    fun = task if defines_custom_call(task) else task.run

    loader = loader or current_app.loader
    backend = task.backend
    ignore_result = task.ignore_result
    track_started = task.track_started
    track_started = not eager and (task.track_started and not ignore_result)
    publish_result = not eager and not ignore_result
    hostname = hostname or socket.gethostname()

    loader_task_init = loader.on_task_init
    loader_cleanup = loader.on_process_cleanup

    task_on_success = getattr(task, "on_success", None)
    task_after_return = getattr(task, "after_return", None)

    store_result = backend.store_result
    backend_cleanup = backend.process_cleanup

    pid = os.getpid()

    request_stack = task.request_stack
    push_request = request_stack.push
    pop_request = request_stack.pop
    on_chord_part_return = backend.on_chord_part_return

    from celery import canvas
    subtask = canvas.subtask

    def trace_task(uuid, args, kwargs, request=None):
        R = I = None
        kwargs = kwdict(kwargs)
        try:
            _task_stack.push(task)
            task_request = Context(request or {}, args=args,
                                   called_directly=False, kwargs=kwargs)
            push_request(task_request)
            try:
                # -*- PRE -*-
                if prerun_receivers:
                    send_prerun(sender=task, task_id=uuid, task=task,
                                args=args, kwargs=kwargs)
                loader_task_init(uuid, task)
                if track_started:
                    store_result(uuid, {"pid": pid,
                                        "hostname": hostname}, STARTED)

                # -*- TRACE -*-
                try:
                    R = retval = fun(*args, **kwargs)
                    state = SUCCESS
                except RetryTaskError, exc:
                    I = Info(RETRY, exc)
                    state, retval = I.state, I.retval
                    R = I.handle_error_state(task, eager=eager)
                except Exception, exc:
                    if propagate:
                        raise
                    I = Info(FAILURE, exc)
                    state, retval = I.state, I.retval
                    R = I.handle_error_state(task, eager=eager)
                    [subtask(errback).apply_async((uuid, ))
                        for errback in task_request.errbacks or []]
                except BaseException, exc:
                    raise
                except:  # pragma: no cover
                    # For Python2.5 where raising strings are still allowed
                    # (but deprecated)
                    if propagate:
                        raise
                    I = Info(FAILURE, None)
                    state, retval = I.state, I.retval
                    R = I.handle_error_state(task, eager=eager)
                    [subtask(errback).apply_async((uuid, ))
                        for errback in task_request.errbacks or []]
                else:
                    if publish_result:
                        store_result(uuid, retval, SUCCESS)
                    # callback tasks must be applied before the result is
                    # stored, so that result.children is populated.
                    [subtask(callback).apply_async((retval, ))
                        for callback in task_request.callbacks or []]
                    if task_on_success:
                        task_on_success(retval, uuid, args, kwargs)
                    if success_receivers:
                        send_success(sender=task, result=retval)

                # -* POST *-
                if task_request.chord:
                    on_chord_part_return(task)
                if task_after_return:
                    task_after_return(state, retval, uuid, args, kwargs, None)
                if postrun_receivers:
                    send_postrun(sender=task, task_id=uuid, task=task,
                                 args=args, kwargs=kwargs,
                                 retval=retval, state=state)
            finally:
                _task_stack.pop()
                pop_request()
                if not eager:
                    try:
                        backend_cleanup()
                        loader_cleanup()
                    except (KeyboardInterrupt, SystemExit, MemoryError):
                        raise
                    except Exception, exc:
                        _logger.error("Process cleanup failed: %r", exc,
                                      exc_info=True)
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
    try:
        _value = task.backend.prepare_exception(exc)
        exc_info = ExceptionInfo((_type, _value, _tb), internal=True)
        warn(RuntimeWarning(
            "Exception raised outside body: %r:\n%s" % (
                exc, exc_info.traceback)))
        return exc_info
    finally:
        del(_tb)
