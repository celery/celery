from celery import conf
from celery.schedules import crontab
from celery.serialization import pickle
from celery.task.base import Task
from celery.task.sets import TaskSet


class backend_cleanup(Task):
    name = "celery.backend_cleanup"

    def run(self):
        self.backend.cleanup()

if conf.TASK_RESULT_EXPIRES and \
        backend_cleanup.name not in conf.CELERYBEAT_SCHEDULE:
    conf.CELERYBEAT_SCHEDULE[backend_cleanup.name] = dict(
            task=backend_cleanup.name,
            schedule=crontab(minute="00", hour="04", day_of_week="*"))


DeleteExpiredTaskMetaTask = backend_cleanup         # FIXME remove in 3.0


class PingTask(Task):
    """The task used by :func:`ping`."""
    name = "celery.ping"

    def run(self, **kwargs):
        """:returns: the string `"pong"`."""
        return "pong"


def _dmap(fun, args, timeout=None):
    pickled = pickle.dumps(fun)
    arguments = [((pickled, arg, {}), {}) for arg in args]
    ts = TaskSet(ExecuteRemoteTask, arguments)
    return ts.apply_async().join(timeout=timeout)


class AsynchronousMapTask(Task):
    """Task used internally by :func:`dmap_async` and
    :meth:`TaskSet.map_async`.  """
    name = "celery.map_async"

    def run(self, serfun, args, timeout=None, **kwargs):
        return _dmap(pickle.loads(serfun), args, timeout=timeout)


class ExecuteRemoteTask(Task):
    """Execute an arbitrary function or object.

    *Note* You probably want :func:`execute_remote` instead, which this
    is an internal component of.

    The object must be pickleable, so you can't use lambdas or functions
    defined in the REPL (that is the python shell, or :program:`ipython`).

    """
    name = "celery.execute_remote"

    def run(self, ser_callable, fargs, fkwargs, **kwargs):
        """
        :param ser_callable: A pickled function or callable object.
        :param fargs: Positional arguments to apply to the function.
        :param fkwargs: Keyword arguments to apply to the function.

        """
        return pickle.loads(ser_callable)(*fargs, **fkwargs)
