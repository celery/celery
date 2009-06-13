"""celery.backends.base"""
import time

from celery.timer import TimeoutTimer
try:
    import cPickle as pickle
except ImportError:
    import pickle


def find_nearest_pickleable_exception(exc):
    """With an exception instance, iterate over its super classes (by mro)
    and find the first super exception that is pickleable. It does
    not go below :exc:`Exception` (i.e. it skips :exc:`Exception`,
    :class:`BaseException` and :class:`object`). If that happens
    you should use :exc:`UnpickleableException` instead.

    :param exc: An exception instance.

    :returns: the nearest exception if it's not :exc:`Exception` or below,
        if it is it returns ``None``.

    :rtype: :exc:`Exception`

    """
    for supercls in exc.__class__.mro():
        if supercls is Exception:
            # only BaseException and object, from here on down,
            # we don't care about these.
            return None
        try:
            superexc = supercls(*exc.args)
            pickle.dumps(superexc)
        except:
            pass
        else:
            return superexc
    return None


class UnpickleableExceptionWrapper(Exception):
    """Wraps unpickleable exceptions.

    :param exc_module: see :attr:`exc_module`.

    :param exc_cls_name: see :attr:`exc_cls_name`.

    :param exc_args: see :attr:`exc_args`

    .. attribute:: exc_module

        The module of the original exception.

    .. attribute:: exc_cls_name

        The name of the original exception class.

    .. attribute:: exc_args

        The arguments for the original exception.

    Example

        >>> try:
        ...     something_raising_unpickleable_exc()
        >>> except Exception, e:
        ...     exc = UnpickleableException(e.__class__.__module__,
        ...                                 e.__class__.__name__,
        ...                                 e.args)
        ...     pickle.dumps(exc) # Works fine.

    """

    def __init__(self, exc_module, exc_cls_name, exc_args):
        self.exc_module = exc_module
        self.exc_cls_name = exc_cls_name
        self.exc_args = exc_args
        super(Exception, self).__init__(exc_module, exc_cls_name, exc_args)


class BaseBackend(object):
    """The base backend class. All backends should inherit from this."""

    capabilities = []
    UnpickleableExceptionWrapper = UnpickleableExceptionWrapper

    def store_result(self, task_id, result, status):
        """Store the result and status of a task."""
        raise NotImplementedError(
                "store_result is not supported by this backend.")

    def mark_as_done(self, task_id, result):
        """Mark task as successfully executed."""
        return self.store_result(task_id, result, status="DONE")

    def mark_as_failure(self, task_id, exc):
        """Mark task as executed with failure. Stores the execption."""
        return self.store_result(task_id, exc, status="FAILURE")

    def create_exception_cls(self, name, module, parent=None):
        """Dynamically create an exception class."""
        if not parent:
            parent = Exception
        return type(name, (parent, ), {"__module__": module})

    def prepare_exception(self, exc):
        """Prepare exception for serialization."""
        nearest = find_nearest_pickleable_exception(exc)
        if nearest:
            return nearest

        try:
            pickle.dumps(exc)
        except pickle.PickleError:
            excwrapper = UnpickleableExceptionWrapper(
                            exc.__class__.__module__,
                            exc.__class__.__name__,
                            exc.args)
            return excwrapper
        else:
            return exc

    def exception_to_python(self, exc):
        """Convert serialized exception to Python exception."""
        if isinstance(exc, UnpickleableExceptionWrapper):
            exc_cls = self.create_exception_cls(exc.exc_cls_name,
                                                exc.exc_module)
            return exc_cls(*exc.exc_args)
        return exc

    def mark_as_retry(self, task_id, exc):
        """Mark task for retry."""
        return self.store_result(task_id, exc, status="RETRY")

    def get_status(self, task_id):
        """Get the status of a task."""
        raise NotImplementedError(
                "get_status is not supported by this backend.")

    def prepare_result(self, result):
        """Prepare result for storage."""
        if result is None:
            return True
        return result

    def get_result(self, task_id):
        """Get the result of a task."""
        raise NotImplementedError(
                "get_result is not supported by this backend.")

    def is_done(self, task_id):
        """Returns ``True`` if the task was successfully executed."""
        return self.get_status(task_id) == "DONE"

    def cleanup(self):
        """Backend cleanup. Is run by
        :class:`celery.task.DeleteExpiredTaskMetaTask`."""
        pass

    def wait_for(self, task_id, timeout=None):
        """Wait for task and return its result.

        If the task raises an exception, this exception
        will be re-raised by :func:`wait_for`.

        If ``timeout`` is not ``None``, this raises the
        :class:`celery.timer.TimeoutError` exception if the operation takes
        longer than ``timeout`` seconds.

        """
        timeout_timer = TimeoutTimer(timeout)
        while True:
            status = self.get_status(task_id)
            if status == "DONE":
                return self.get_result(task_id)
            elif status == "FAILURE":
                raise self.get_result(task_id)
            time.sleep(0.5) # avoid hammering the CPU checking status.
            timeout_timer.tick()

    def process_cleanup(self):
        """Cleanup actions to do at the end of a task worker process.

        See :func:`celery.worker.jail`.
        
        """
        pass
