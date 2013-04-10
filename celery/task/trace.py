# -*- coding: utf-8 -*-
"""
    celery.task.trace
    ~~~~~~~~~~~~~~~~~~~~

    This module defines how the task execution is traced:
    errors are recorded, handlers are applied and so on.

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
from celery._state import _task_stack
from celery.app import set_default_app
from celery.app.task import Task as BaseTask, Context
from celery.datastructures import ExceptionInfo
from celery.exceptions import Ignore, RetryTaskError
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
IGNORED = states.IGNORED
RETRY = states.RETRY
FAILURE = states.FAILURE
EXCEPTION_STATES = states.EXCEPTION_STATES
IGNORE_STATES = frozenset([IGNORED, RETRY])

#: set by :func:`setup_worker_optimizations`
_tasks = None
_patched = {}


def mro_lookup(cls, attr, stop=(), monkey_patched=[]):
    """Returns the first node by MRO order that defines an attribute.

    :keyword stop: A list of types that if reached will stop the search.
    :keyword monkey_patched: Use one of the stop classes if the attr's
        module origin is not in this list, this to detect monkey patched
        attributes.

    :returns None: if the attribute was not found.

    """
    for node in cls.mro():
        if node in stop:
            try:
                attr = node.__dict__[attr]
                module_origin = attr.__module__
            except (AttributeError, KeyError):
                pass
            else:
                if module_origin not in monkey_patched:
                    return node
            return
        if attr in node.__dict__:
            return node


def task_has_custom(task, attr):
    """Returns true if the task or one of its bases
    defines ``attr`` (excluding the one in BaseTask)."""
    return mro_lookup(task.__class__, attr, stop=(BaseTask, object),
                      monkey_patched=['celery.app.task'])


class TraceInfo(object):
    __slots__ = ('state', 'retval')

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
        # the exception raised is the RetryTaskError semi-predicate,
        # and it's exc' attribute is the original exception raised (if any).
        req = task.request
        type_, _, tb = sys.exc_info()
        try:
            reason = self.retval
            einfo = ExceptionInfo((type_, reason, tb))
            if store_errors:
                task.backend.mark_as_retry(req.id, reason.exc, einfo.traceback)
            task.on_retry(reason.exc, req.id, req.args, req.kwargs, einfo)
            signals.task_retry.send(sender=task, request=req,
                                    reason=reason, einfo=einfo)
            return einfo
        finally:
            del(tb)

    def handle_failure(self, task, store_errors=True):
        """Handle exception."""
        req = task.request
        type_, _, tb = sys.exc_info()
        try:
            exc = self.retval
            einfo = ExceptionInfo((type_, get_pickleable_exception(exc), tb))
            if store_errors:
                task.backend.mark_as_failure(req.id, exc, einfo.traceback)
            task.on_failure(exc, req.id, req.args, req.kwargs, einfo)
            signals.task_failure.send(sender=task, task_id=req.id,
                                      exception=exc, args=req.args,
                                      kwargs=req.kwargs,
                                      traceback=tb,
                                      einfo=einfo)
            return einfo
        finally:
            del(tb)


def build_tracer(name, task, loader=None, hostname=None, store_errors=True,
                 Info=TraceInfo, eager=False, propagate=False,
                 IGNORE_STATES=IGNORE_STATES):
    """Builts a function that tracing the tasks execution; catches all
    exceptions, and saves the state and result of the task execution
    to the result backend.

    If the call was successful, it saves the result to the task result
    backend, and sets the task status to `"SUCCESS"`.

    If the call raises :exc:`~celery.exceptions.RetryTaskError`, it extracts
    the original exception, uses that as the result and sets the task status
    to `"RETRY"`.

    If the call results in an exception, it saves the exception as the task
    result, and sets the task status to `"FAILURE"`.

    Returns a function that takes the following arguments:

        :param uuid: The unique id of the task.
        :param args: List of positional args to pass on to the function.
        :param kwargs: Keyword arguments mapping to pass on to the function.
        :keyword request: Request dict.

    """
    # If the task doesn't define a custom __call__ method
    # we optimize it away by simply calling the run method directly,
    # saving the extra method call and a line less in the stack trace.
    fun = task if task_has_custom(task, '__call__') else task.run

    loader = loader or current_app.loader
    backend = task.backend
    ignore_result = task.ignore_result
    track_started = task.track_started
    track_started = not eager and (task.track_started and not ignore_result)
    publish_result = not eager and not ignore_result
    hostname = hostname or socket.gethostname()

    loader_task_init = loader.on_task_init
    loader_cleanup = loader.on_process_cleanup

    task_on_success = None
    task_after_return = None
    if task_has_custom(task, 'on_success'):
        task_on_success = task.on_success
    if task_has_custom(task, 'after_return'):
        task_after_return = task.after_return

    store_result = backend.store_result
    backend_cleanup = backend.process_cleanup

    pid = os.getpid()

    request_stack = task.request_stack
    push_request = request_stack.push
    pop_request = request_stack.pop
    push_task = _task_stack.push
    pop_task = _task_stack.pop
    on_chord_part_return = backend.on_chord_part_return

    from celery import canvas
    subtask = canvas.subtask

    def trace_task(uuid, args, kwargs, request=None):
        R = I = None
        kwargs = kwdict(kwargs)
        try:
            push_task(task)
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
                    store_result(uuid, {'pid': pid,
                                        'hostname': hostname}, STARTED)

                # -*- TRACE -*-
                try:
                    R = retval = fun(*args, **kwargs)
                    state = SUCCESS
                except Ignore, exc:
                    I, R = Info(IGNORED, exc), ExceptionInfo(internal=True)
                    state, retval = I.state, I.retval
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
                    # callback tasks must be applied before the result is
                    # stored, so that result.children is populated.
                    [subtask(callback).apply_async((retval, ))
                        for callback in task_request.callbacks or []]
                    if publish_result:
                        store_result(uuid, retval, SUCCESS)
                    if task_on_success:
                        task_on_success(retval, uuid, args, kwargs)
                    if success_receivers:
                        send_success(sender=task, result=retval)

                # -* POST *-
                if state not in IGNORE_STATES:
                    if task_request.chord:
                        on_chord_part_return(task)
                    if task_after_return:
                        task_after_return(
                            state, retval, uuid, args, kwargs, None,
                        )
                    if postrun_receivers:
                        send_postrun(sender=task, task_id=uuid, task=task,
                                     args=args, kwargs=kwargs,
                                     retval=retval, state=state)
            finally:
                pop_task()
                pop_request()
                if not eager:
                    try:
                        backend_cleanup()
                        loader_cleanup()
                    except (KeyboardInterrupt, SystemExit, MemoryError):
                        raise
                    except Exception, exc:
                        _logger.error('Process cleanup failed: %r', exc,
                                      exc_info=True)
        except Exception, exc:
            if eager:
                raise
            R = report_internal_error(task, exc)
        return R, I

    return trace_task


def trace_task(task, uuid, args, kwargs, request={}, **opts):
    try:
        if task.__trace__ is None:
            task.__trace__ = build_tracer(task.name, task, **opts)
        return task.__trace__(uuid, args, kwargs, request)[0]
    except Exception, exc:
        return report_internal_error(task, exc)


def _trace_task_ret(name, uuid, args, kwargs, request={}, **opts):
    return trace_task(current_app.tasks[name],
                      uuid, args, kwargs, request, **opts)
trace_task_ret = _trace_task_ret


def _fast_trace_task(task, uuid, args, kwargs, request={}):
    # setup_worker_optimizations will point trace_task_ret to here,
    # so this is the function used in the worker.
    return _tasks[task].__trace__(uuid, args, kwargs, request)[0]


def eager_trace_task(task, uuid, args, kwargs, request=None, **opts):
    opts.setdefault('eager', True)
    return build_tracer(task.name, task, **opts)(
        uuid, args, kwargs, request)


def report_internal_error(task, exc):
    _type, _value, _tb = sys.exc_info()
    try:
        _value = task.backend.prepare_exception(exc)
        exc_info = ExceptionInfo((_type, _value, _tb), internal=True)
        warn(RuntimeWarning(
            'Exception raised outside body: %r:\n%s' % (
                exc, exc_info.traceback)))
        return exc_info
    finally:
        del(_tb)


def setup_worker_optimizations(app):
    global _tasks
    global trace_task_ret

    # make sure custom Task.__call__ methods that calls super
    # will not mess up the request/task stack.
    _install_stack_protection()

    # all new threads start without a current app, so if an app is not
    # passed on to the thread it will fall back to the "default app",
    # which then could be the wrong app.  So for the worker
    # we set this to always return our app.  This is a hack,
    # and means that only a single app can be used for workers
    # running in the same process.
    app.set_current()
    set_default_app(app)

    # evaluate all task classes by finalizing the app.
    app.finalize()

    # set fast shortcut to task registry
    _tasks = app._tasks

    trace_task_ret = _fast_trace_task
    try:
        job = sys.modules['celery.worker.job']
    except KeyError:
        pass
    else:
        job.trace_task_ret = _fast_trace_task
        job.__optimize__()


def reset_worker_optimizations():
    global trace_task_ret
    trace_task_ret = _trace_task_ret
    try:
        delattr(BaseTask, '_stackprotected')
    except AttributeError:
        pass
    try:
        BaseTask.__call__ = _patched.pop('BaseTask.__call__')
    except KeyError:
        pass
    try:
        sys.modules['celery.worker.job'].trace_task_ret = _trace_task_ret
    except KeyError:
        pass


def _install_stack_protection():
    # Patches BaseTask.__call__ in the worker to handle the edge case
    # where people override it and also call super.
    #
    # - The worker optimizes away BaseTask.__call__ and instead
    #   calls task.run directly.
    # - so with the addition of current_task and the request stack
    #   BaseTask.__call__ now pushes to those stacks so that
    #   they work when tasks are called directly.
    #
    # The worker only optimizes away __call__ in the case
    # where it has not been overridden, so the request/task stack
    # will blow if a custom task class defines __call__ and also
    # calls super().
    if not getattr(BaseTask, '_stackprotected', False):
        _patched['BaseTask.__call__'] = orig = BaseTask.__call__

        def __protected_call__(self, *args, **kwargs):
            stack = self.request_stack
            req = stack.top
            if req and not req._protected and \
                    len(stack) == 1 and not req.called_directly:
                req._protected = 1
                return self.run(*args, **kwargs)
            return orig(self, *args, **kwargs)
        BaseTask.__call__ = __protected_call__
        BaseTask._stackprotected = True
