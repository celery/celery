# -*- coding: utf-8 -*-
"""
    celery.execute.trace
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
import traceback

from warnings import warn

from kombu.utils import kwdict

from .. import app as app_module
from .. import current_app
from .. import states, signals
from ..app.task import BaseTask
from ..datastructures import ExceptionInfo
from ..exceptions import RetryTaskError
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

    task_on_success = task.on_success
    task_after_return = task.after_return
    task_request = task.request
    _tls = app_module._tls

    store_result = backend.store_result
    backend_cleanup = backend.process_cleanup

    pid = os.getpid()

    update_request = task_request.update
    clear_request = task_request.clear
    on_chord_part_return = backend.on_chord_part_return

    def trace_task(uuid, args, kwargs, request=None):
        R = I = None
        kwargs = kwdict(kwargs)
        try:
            _tls.current_task = task
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
                    R = retval = fun(*args, **kwargs)
                    state, einfo = SUCCESS, None
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
                except:  # pragma: no cover
                    # For Python2.5 where raising strings are still allowed
                    # (but deprecated)
                    if propagate:
                        raise
                    I = Info(FAILURE, None, sys.exc_info())
                    state, retval, einfo = I.state, I.retval, I.exc_info
                    R = I.handle_error_state(task, eager=eager)
                else:
                    task_on_success(retval, uuid, args, kwargs)
                    if publish_result:
                        store_result(uuid, retval, SUCCESS)

                # -* POST *-
                if task_request.chord:
                    on_chord_part_return(task)
                task_after_return(state, retval, uuid, args, kwargs, einfo)
                send_postrun(sender=task, task_id=uuid, task=task,
                            args=args, kwargs=kwargs, retval=retval)
            finally:
                _tls.current_task = None
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
