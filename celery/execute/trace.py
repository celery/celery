import sys
import traceback

from celery import states
from celery import signals
from celery.registry import tasks
from celery.exceptions import RetryTaskError
from celery.datastructures import ExceptionInfo


class TraceInfo(object):

    def __init__(self, status=states.PENDING, retval=None, exc_info=None):
        self.status = status
        self.retval = retval
        self.exc_info = exc_info
        self.exc_type = None
        self.exc_value = None
        self.tb = None
        self.strtb = None
        if self.exc_info:
            self.exc_type, self.exc_value, self.tb = exc_info
            self.strtb = "\n".join(traceback.format_exception(*exc_info))

    @classmethod
    def trace(cls, fun, args, kwargs, propagate=False):
        """Trace the execution of a function, calling the appropiate callback
        if the function raises retry, an failure or returned successfully.

        :keyword propagate: If true, errors will propagate to the caller.

        """
        try:
            return cls(states.SUCCESS, retval=fun(*args, **kwargs))
        except (SystemExit, KeyboardInterrupt):
            raise
        except RetryTaskError, exc:
            return cls(states.RETRY, retval=exc, exc_info=sys.exc_info())
        except Exception, exc:
            if propagate:
                raise
            return cls(states.FAILURE, retval=exc, exc_info=sys.exc_info())
        except:  # pragma: no cover
            # For Python2.4 where raising strings are still allowed.
            if propagate:
                raise
            return cls(states.FAILURE, retval=None, exc_info=sys.exc_info())


class TaskTrace(object):

    def __init__(self, task_name, task_id, args, kwargs, task=None,
            request=None, propagate=None, **_):
        self.task_id = task_id
        self.task_name = task_name
        self.args = args
        self.kwargs = kwargs
        self.task = task or tasks[self.task_name]
        self.request = request or {}
        self.status = states.PENDING
        self.strtb = None
        self.propagate = propagate
        self._trace_handlers = {states.FAILURE: self.handle_failure,
                                states.RETRY: self.handle_retry,
                                states.SUCCESS: self.handle_success}

    def __call__(self):
        return self.execute()

    def execute(self):
        self.task.request.update(self.request, args=self.args,
                                               kwargs=self.kwargs)
        signals.task_prerun.send(sender=self.task, task_id=self.task_id,
                                 task=self.task, args=self.args,
                                 kwargs=self.kwargs)
        retval = self._trace()

        signals.task_postrun.send(sender=self.task, task_id=self.task_id,
                                  task=self.task, args=self.args,
                                  kwargs=self.kwargs, retval=retval)
        self.task.request.clear()
        return retval

    def _trace(self):
        trace = TraceInfo.trace(self.task, self.args, self.kwargs,
                                propagate=self.propagate)
        self.status = trace.status
        self.strtb = trace.strtb
        handler = self._trace_handlers[trace.status]
        r = handler(trace.retval, trace.exc_type, trace.tb, trace.strtb)
        self.handle_after_return(trace.status, trace.retval,
                                 trace.exc_type, trace.tb, trace.strtb)
        return r

    def handle_after_return(self, status, retval, type_, tb, strtb):
        einfo = None
        if status in states.EXCEPTION_STATES:
            einfo = ExceptionInfo((retval, type_, tb))
        self.task.after_return(status, retval, self.task_id,
                               self.args, self.kwargs, einfo=einfo)

    def handle_success(self, retval, *args):
        """Handle successful execution."""
        self.task.on_success(retval, self.task_id, self.args, self.kwargs)
        return retval

    def handle_retry(self, exc, type_, tb, strtb):
        """Handle retry exception."""

        # Create a simpler version of the RetryTaskError that stringifies
        # the original exception instead of including the exception instance.
        # This is for reporting the retry in logs, e-mail etc, while
        # guaranteeing pickleability.
        message, orig_exc = exc.args
        expanded_msg = "%s: %s" % (message, str(orig_exc))
        einfo = ExceptionInfo((type_,
                               type_(expanded_msg, None),
                               tb))
        self.task.on_retry(exc, self.task_id,
                           self.args, self.kwargs, einfo=einfo)
        return einfo

    def handle_failure(self, exc, type_, tb, strtb):
        """Handle exception."""
        einfo = ExceptionInfo((type_, exc, tb))
        self.task.on_failure(exc, self.task_id,
                             self.args, self.kwargs, einfo=einfo)
        signals.task_failure.send(sender=self.task, task_id=self.task_id,
                                  exception=exc, args=self.args,
                                  kwargs=self.kwargs, traceback=tb,
                                  einfo=einfo)
        return einfo
