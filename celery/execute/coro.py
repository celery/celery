import traceback
import socket
import os
import sys
import warnings
from .. import states
from .. import current_app
from .. import signals
from ..datastructures import ExceptionInfo
from ..exceptions import RetryTaskError
from ..utils.serialization import get_pickleable_exception


class Traceinfo(object):
    __slots__ = ("state", "retval", "exc_info",
                 "exc_type", "exc_value", "tb", "strtb")

    def __init__(self, state, retval, exc_info=None):
        self.state = state
        self.retval = retval
        self.exc_info = exc_info
        if exc_info:
            self.exc_type, self.exc_value, self.tb = exc_info
            self.strtb = "\n".join(traceback.format_exception(*exc_info))


def tracer(task, loader=None, hostname=None):
    hostname = hostname or socket.gethostname()
    pid = os.getpid()

    PENDING = states.PENDING
    SUCCESS = states.SUCCESS
    RETRY = states.RETRY
    FAILURE = states.FAILURE
    EXCEPTION_STATES = states.EXCEPTION_STATES

    loader = loader or current_app.loader
    on_task_init = loader.on_task_init

    task_cleanup = task.backend.process_cleanup
    loader_cleanup = loader.on_process_cleanup
    on_success = task.on_success
    on_failure = task.on_failure
    on_retry = task.on_retry
    after_return = task.after_return
    update_req = task.request.update
    clear_req = task.request.clear
    backend = task.backend
    prepare_exception = backend.prepare_exception
    mark_as_started = backend.mark_as_started
    mark_as_done = backend.mark_as_done
    mark_as_failure = backend.mark_as_failure
    mark_as_retry = backend.mark_as_retry
    ignore_result = task.ignore_result
    store_errors = True
    if ignore_result:
        store_errors = task.store_errors_even_if_ignored
    track_started = task.track_started

    send_prerun = signals.task_prerun.send
    send_postrun = signals.task_postrun.send
    send_failure = signals.task_failure.send

    @coroutine
    def task_tracer(self):

        while 1:
            X = None
            ID, ARGS, KWARGS, REQ, propagate = (yield)
            state = PENDING

            try:
                # - init
                on_task_init(ID, task)
                if track_started and not ignore_result:
                    mark_as_started(ID, pid=pid, hostname=hostname)
                update_req(REQ, args=ARGS, kwargs=KWARGS,
                           called_directly=False)
                send_prerun(sender=task, task_id=ID, task=task,
                            args=ARGS, kwargs=KWARGS)

                # - trace execution
                R = None
                try:
                    R = Traceinfo(SUCCESS, task(*ARGS, **KWARGS))
                except RetryTaskError, exc:
                    R = TraceInfo(RETRY, exc, sys.exc_info())
                except Exception, exc:
                    if propagate:
                        raise
                    R = Traceinfo(FAILURE, exc, sys.exc_info())
                except BaseException, exc:
                    raise
                except:  # pragma: no cover
                    # For Python2.5 where raising strings are still allowed
                    # (but deprecated)
                    if propagate:
                        raise
                    R = Traceinfo(FAILURE, None, sys.exc_info())

                # - report state
                state = R.state
                retval = R.retval
                if state == SUCCESS:
                    if not ignore_result:
                        mark_as_done(ID, retval)
                    on_success(retval, ID, ARGS, KWARGS)
                elif state == RETRY:
                    type_, tb = R.exc_type, R.tb
                    if not ignore_result:
                        message, orig_exc = R.exc_value.args
                    if store_errors:
                        mark_as_retry(ID, orig_exc, R.strtb)
                    expanded_msg = "%s: %s" % (message, str(orig_exc))
                    X = ExceptionInfo((type_, type_(expanded_msg, None), tb))
                    on_retry(exc, ID, ARGS, KWARGS, X)
                elif state == FAILURE:
                    if store_errors:
                        mark_as_failure(ID, exc, R.strtb)
                    exc = get_pickleable_exception(exc)
                    X = ExceptionInfo((type_, exc, tb))
                    on_failure(exc, ID, ARGS, KWARGS, X)
                    send_failure(sender=task, task_id=ID, exception=exc,
                                 args=ARGS, kwargs=KWARGS, traceback=tb,
                                 einfo=X)

                # - after return
                if state in EXCEPTION_STATES:
                    einfo = ExceptionInfo(R.exc_info)
                after_return(state, R, ID, ARGS, KWARGS, einfo)

                # - post run
                send_postrun(sender=task, task_id=ID, task=task,
                             args=ARGS, kwargs=KWARGS, retval=retval)

                yield X
            except Exception, exc:
                _type, _value, _tb = sys.exc_info()
                _value = prepare_exception(exc)
                exc_info = ExceptionInfo((_type, _value, _tb))
                warnings.warn("Exception outside body: %s: %s\n%s" % tuple(
                    map(str, (exc.__class__, exc, exc_info.traceback))))
                yield exc_info
            finally:
                clear_req()
                try:
                    task_cleanup()
                    loader_cleanup()
                except (KeyboardInterrupt, SystemExit, MemoryError):
                    raise
                except Exception, exc:
                    logger = current_app.log.get_default_logger()
                    logger.error("Process cleanup failed: %r", exc,
                                 exc_info=sys.exc_info())

    return task_tracer()
