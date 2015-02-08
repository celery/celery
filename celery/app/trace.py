# -*- coding: utf-8 -*-
"""
    celery.app.trace
    ~~~~~~~~~~~~~~~~

    This module defines how the task execution is traced:
    errors are recorded, handlers are applied and so on.

"""
from __future__ import absolute_import

# ## ---
# This is the heart of the worker, the inner loop so to speak.
# It used to be split up into nice little classes and methods,
# but in the end it only resulted in bad performance and horrible tracebacks,
# so instead we now use one closure per task class.

import logging
import os
import socket
import sys

from collections import namedtuple
from warnings import warn

from billiard.einfo import ExceptionInfo
from kombu.exceptions import EncodeError
from kombu.serialization import loads as loads_message, prepare_accept_content
from kombu.utils.encoding import safe_repr, safe_str

from celery import current_app, group
from celery import states, signals
from celery._state import _task_stack
from celery.app import set_default_app
from celery.app.task import Task as BaseTask, Context
from celery.exceptions import Ignore, Reject, Retry, InvalidTaskError
from celery.five import monotonic
from celery.utils.log import get_logger
from celery.utils.objects import mro_lookup
from celery.utils.serialization import (
    get_pickleable_exception, get_pickled_exception, get_pickleable_etype,
)
from celery.utils.text import truncate

__all__ = ['TraceInfo', 'build_tracer', 'trace_task',
           'setup_worker_optimizations', 'reset_worker_optimizations']

logger = get_logger(__name__)
info = logger.info

#: Format string used to log task success.
LOG_SUCCESS = """\
Task %(name)s[%(id)s] succeeded in %(runtime)ss: %(return_value)s\
"""

#: Format string used to log task failure.
LOG_FAILURE = """\
Task %(name)s[%(id)s] %(description)s: %(exc)s\
"""

#: Format string used to log task internal error.
LOG_INTERNAL_ERROR = """\
Task %(name)s[%(id)s] %(description)s: %(exc)s\
"""

#: Format string used to log task ignored.
LOG_IGNORED = """\
Task %(name)s[%(id)s] %(description)s\
"""

#: Format string used to log task rejected.
LOG_REJECTED = """\
Task %(name)s[%(id)s] %(exc)s\
"""

#: Format string used to log task retry.
LOG_RETRY = """\
Task %(name)s[%(id)s] retry: %(exc)s\
"""

log_policy_t = namedtuple(
    'log_policy_t', ('format', 'description', 'severity', 'traceback', 'mail'),
)

log_policy_reject = log_policy_t(LOG_REJECTED, 'rejected', logging.WARN, 1, 1)
log_policy_ignore = log_policy_t(LOG_IGNORED, 'ignored', logging.INFO, 0, 0)
log_policy_internal = log_policy_t(
    LOG_INTERNAL_ERROR, 'INTERNAL ERROR', logging.CRITICAL, 1, 1,
)
log_policy_expected = log_policy_t(
    LOG_FAILURE, 'raised expected', logging.INFO, 0, 0,
)
log_policy_unexpected = log_policy_t(
    LOG_FAILURE, 'raised unexpected', logging.ERROR, 1, 1,
)

send_prerun = signals.task_prerun.send
send_postrun = signals.task_postrun.send
send_success = signals.task_success.send
STARTED = states.STARTED
SUCCESS = states.SUCCESS
IGNORED = states.IGNORED
REJECTED = states.REJECTED
RETRY = states.RETRY
FAILURE = states.FAILURE
EXCEPTION_STATES = states.EXCEPTION_STATES
IGNORE_STATES = frozenset({IGNORED, RETRY, REJECTED})

#: set by :func:`setup_worker_optimizations`
_localized = []
_patched = {}

trace_ok_t = namedtuple('trace_ok_t', ('retval', 'info', 'runtime', 'retstr'))


def task_has_custom(task, attr):
    """Return true if the task or one of its bases
    defines ``attr`` (excluding the one in BaseTask)."""
    return mro_lookup(task.__class__, attr, stop=(BaseTask, object),
                      monkey_patched=['celery.app.task'])


def get_log_policy(task, einfo, exc):
    if isinstance(exc, Reject):
        return log_policy_reject
    elif isinstance(exc, Ignore):
        return log_policy_ignore
    elif einfo.internal:
        return log_policy_internal
    else:
        if task.throws and isinstance(exc, task.throws):
            return log_policy_expected
        return log_policy_unexpected


class TraceInfo(object):
    __slots__ = ('state', 'retval')

    def __init__(self, state, retval=None):
        self.state = state
        self.retval = retval

    def handle_error_state(self, task, req, eager=False):
        store_errors = not eager
        if task.ignore_result:
            store_errors = task.store_errors_even_if_ignored

        return {
            RETRY: self.handle_retry,
            FAILURE: self.handle_failure,
        }[self.state](task, req, store_errors=store_errors)

    def handle_reject(self, task, req, **kwargs):
        self._log_error(task, req, ExceptionInfo())

    def handle_ignore(self, task, req, **kwargs):
        self._log_error(task, req, ExceptionInfo())

    def handle_retry(self, task, req, store_errors=True):
        """Handle retry exception."""
        # the exception raised is the Retry semi-predicate,
        # and it's exc' attribute is the original exception raised (if any).
        type_, _, tb = sys.exc_info()
        try:
            reason = self.retval
            einfo = ExceptionInfo((type_, reason, tb))
            if store_errors:
                task.backend.mark_as_retry(
                    req.id, reason.exc, einfo.traceback, request=req,
                )
            task.on_retry(reason.exc, req.id, req.args, req.kwargs, einfo)
            signals.task_retry.send(sender=task, request=req,
                                    reason=reason, einfo=einfo)
            info(LOG_RETRY, {
                'id': req.id, 'name': task.name,
                'exc': safe_repr(reason.exc),
            })
            return einfo
        finally:
            del(tb)

    def handle_failure(self, task, req, store_errors=True):
        """Handle exception."""
        type_, _, tb = sys.exc_info()
        try:
            exc = self.retval
            einfo = ExceptionInfo()
            einfo.exception = get_pickleable_exception(einfo.exception)
            einfo.type = get_pickleable_etype(einfo.type)
            if store_errors:
                task.backend.mark_as_failure(
                    req.id, exc, einfo.traceback, request=req,
                )
            task.on_failure(exc, req.id, req.args, req.kwargs, einfo)
            signals.task_failure.send(sender=task, task_id=req.id,
                                      exception=exc, args=req.args,
                                      kwargs=req.kwargs,
                                      traceback=tb,
                                      einfo=einfo)
            self._log_error(task, req, einfo)
            return einfo
        finally:
            del(tb)

    def _log_error(self, task, req, einfo):
        eobj = einfo.exception = get_pickled_exception(einfo.exception)
        exception, traceback, exc_info, sargs, skwargs = (
            safe_repr(eobj),
            safe_str(einfo.traceback),
            einfo.exc_info,
            safe_repr(req.args),
            safe_repr(req.kwargs),
        )
        policy = get_log_policy(task, einfo, eobj)

        context = {
            'hostname': req.hostname,
            'id': req.id,
            'name': task.name,
            'exc': exception,
            'traceback': traceback,
            'args': sargs,
            'kwargs': skwargs,
            'description': policy.description,
            'internal': einfo.internal,
        }

        logger.log(policy.severity, policy.format.strip(), context,
                   exc_info=exc_info if policy.traceback else None,
                   extra={'data': context})

        if policy.mail:
            task.send_error_email(context, einfo.exception)


def build_tracer(name, task, loader=None, hostname=None, store_errors=True,
                 Info=TraceInfo, eager=False, propagate=False, app=None,
                 monotonic=monotonic, truncate=truncate,
                 trace_ok_t=trace_ok_t, IGNORE_STATES=IGNORE_STATES):
    """Return a function that traces task execution; catches all
    exceptions and updates result backend with the state and result

    If the call was successful, it saves the result to the task result
    backend, and sets the task status to `"SUCCESS"`.

    If the call raises :exc:`~@Retry`, it extracts
    the original exception, uses that as the result and sets the task state
    to `"RETRY"`.

    If the call results in an exception, it saves the exception as the task
    result, and sets the task state to `"FAILURE"`.

    Return a function that takes the following arguments:

        :param uuid: The id of the task.
        :param args: List of positional args to pass on to the function.
        :param kwargs: Keyword arguments mapping to pass on to the function.
        :keyword request: Request dict.

    """
    # If the task doesn't define a custom __call__ method
    # we optimize it away by simply calling the run method directly,
    # saving the extra method call and a line less in the stack trace.
    fun = task if task_has_custom(task, '__call__') else task.run

    loader = loader or app.loader
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
    _does_info = logger.isEnabledFor(logging.INFO)

    prerun_receivers = signals.task_prerun.receivers
    postrun_receivers = signals.task_postrun.receivers
    success_receivers = signals.task_success.receivers

    from celery import canvas
    signature = canvas.maybe_signature  # maybe_ does not clone if already

    def on_error(request, exc, uuid, state=FAILURE, call_errbacks=True):
        if propagate:
            raise
        I = Info(state, exc)
        R = I.handle_error_state(task, request, eager=eager)
        if call_errbacks:
            group(
                [signature(errback, app=app)
                 for errback in request.errbacks or []], app=app,
            ).apply_async((uuid, ))
        return I, R, I.state, I.retval

    def trace_task(uuid, args, kwargs, request=None):
        # R      - is the possibly prepared return value.
        # I      - is the Info object.
        # T      - runtime
        # Rstr   - textual representation of return value
        # retval - is the always unmodified return value.
        # state  - is the resulting task state.

        # This function is very long because we have unrolled all the calls
        # for performance reasons, and because the function is so long
        # we want the main variables (I, and R) to stand out visually from the
        # the rest of the variables, so breaking PEP8 is worth it ;)
        R = I = T = Rstr = retval = state = None
        task_request = None
        time_start = monotonic()
        try:
            try:
                kwargs.items
            except AttributeError:
                raise InvalidTaskError(
                    'Task keyword arguments is not a mapping')
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
                    store_result(
                        uuid, {'pid': pid, 'hostname': hostname}, STARTED,
                        request=task_request,
                    )

                # -*- TRACE -*-
                try:
                    R = retval = fun(*args, **kwargs)
                    state = SUCCESS
                except Reject as exc:
                    I, R = Info(REJECTED, exc), ExceptionInfo(internal=True)
                    state, retval = I.state, I.retval
                    I.handle_reject(task, task_request)
                except Ignore as exc:
                    I, R = Info(IGNORED, exc), ExceptionInfo(internal=True)
                    state, retval = I.state, I.retval
                    I.handle_ignore(task, task_request)
                except Retry as exc:
                    I, R, state, retval = on_error(
                        task_request, exc, uuid, RETRY, call_errbacks=False,
                    )
                except Exception as exc:
                    I, R, state, retval = on_error(task_request, exc, uuid)
                    if task_request.chord:
                        on_chord_part_return(task, state, exc)
                except BaseException as exc:
                    raise
                else:
                    try:
                        # callback tasks must be applied before the result is
                        # stored, so that result.children is populated.

                        # groups are called inline and will store trail
                        # separately, so need to call them separately
                        # so that the trail's not added multiple times :(
                        # (Issue #1936)
                        callbacks = task.request.callbacks
                        if callbacks:
                            if len(task.request.callbacks) > 1:
                                sigs, groups = [], []
                                for sig in callbacks:
                                    sig = signature(sig, app=app)
                                    if isinstance(sig, group):
                                        groups.append(sig)
                                    else:
                                        sigs.append(sig)
                                for group_ in groups:
                                    group.apply_async((retval, ))
                                if sigs:
                                    group(sigs).apply_async((retval, ))
                            else:
                                signature(callbacks[0], app=app).delay(retval)
                        if publish_result:
                            store_result(
                                uuid, retval, SUCCESS, request=task_request,
                            )
                    except EncodeError as exc:
                        I, R, state, retval = on_error(task_request, exc, uuid)
                    else:
                        if task_request.chord:
                            on_chord_part_return(task, state, retval)
                        if task_on_success:
                            task_on_success(retval, uuid, args, kwargs)
                        if success_receivers:
                            send_success(sender=task, result=retval)
                        if _does_info:
                            T = monotonic() - time_start
                            Rstr = truncate(safe_repr(R), 256)
                            info(LOG_SUCCESS, {
                                'id': uuid, 'name': name,
                                'return_value': Rstr, 'runtime': T,
                            })

                # -* POST *-
                if state not in IGNORE_STATES:
                    if task_after_return:
                        task_after_return(
                            state, retval, uuid, args, kwargs, None,
                        )
            finally:
                try:
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
                        except Exception as exc:
                            logger.error('Process cleanup failed: %r', exc,
                                         exc_info=True)
        except MemoryError:
            raise
        except Exception as exc:
            if eager:
                raise
            R = report_internal_error(task, exc)
            if task_request is not None:
                I, _, _, _ = on_error(task_request, exc, uuid)
        return trace_ok_t(R, I, T, Rstr)

    return trace_task


def trace_task(task, uuid, args, kwargs, request={}, **opts):
    try:
        if task.__trace__ is None:
            task.__trace__ = build_tracer(task.name, task, **opts)
        return task.__trace__(uuid, args, kwargs, request)
    except Exception as exc:
        return trace_ok_t(report_internal_error(task, exc), None, 0.0, None)


def _trace_task_ret(name, uuid, request, body, content_type,
                    content_encoding, loads=loads_message, app=None,
                    **extra_request):
    app = app or current_app._get_current_object()
    embed = None
    if content_type:
        accept = prepare_accept_content(app.conf.CELERY_ACCEPT_CONTENT)
        args, kwargs, embed = loads(
            body, content_type, content_encoding, accept=accept,
        )
    else:
        args, kwargs = body
    hostname = socket.gethostname()
    request.update({
        'args': args, 'kwargs': kwargs,
        'hostname': hostname, 'is_eager': False,
    }, **embed or {})
    R, I, T, Rstr = trace_task(app.tasks[name],
                               uuid, args, kwargs, request, app=app)
    return (1, R, T) if I else (0, Rstr, T)
trace_task_ret = _trace_task_ret


def _fast_trace_task(task, uuid, request, body, content_type,
                     content_encoding, loads=loads_message, _loc=_localized,
                     hostname=None, **_):
    embed = None
    tasks, accept, hostname = _loc
    if content_type:
        args, kwargs, embed = loads(
            body, content_type, content_encoding, accept=accept,
        )
    else:
        args, kwargs = body
    request.update({
        'args': args, 'kwargs': kwargs,
        'hostname': hostname, 'is_eager': False,
    }, **embed or {})
    R, I, T, Rstr = tasks[task].__trace__(
        uuid, args, kwargs, request,
    )
    return (1, R, T) if I else (0, Rstr, T)


def report_internal_error(task, exc):
    _type, _value, _tb = sys.exc_info()
    try:
        _value = task.backend.prepare_exception(exc, 'pickle')
        exc_info = ExceptionInfo((_type, _value, _tb), internal=True)
        warn(RuntimeWarning(
            'Exception raised outside body: {0!r}:\n{1}'.format(
                exc, exc_info.traceback)))
        return exc_info
    finally:
        del(_tb)


def setup_worker_optimizations(app, hostname=None):
    global trace_task_ret

    hostname = hostname or socket.gethostname()

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
    _localized[:] = [
        app._tasks,
        prepare_accept_content(app.conf.CELERY_ACCEPT_CONTENT),
        hostname,
    ]

    trace_task_ret = _fast_trace_task
    from celery.worker import request as request_module
    request_module.trace_task_ret = _fast_trace_task
    request_module.__optimize__()


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
    from celery.worker import request as request_module
    request_module.trace_task_ret = _trace_task_ret


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
