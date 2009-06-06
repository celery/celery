"""

Custom Datastructures

"""
import multiprocessing
from multiprocessing.pool import RUN as POOL_STATE_RUN
import itertools
import threading
import time
import os
from UserList import UserList
from celery.timer import TimeoutTimer, TimeoutError
from celery.conf import REAP_TIMEOUT, SEND_CELERY_TASK_ERROR_EMAILS

from django.core.mail import mail_admins


class PositionQueue(UserList):
    """A positional queue of a specific length, with slots that are either
    filled or unfilled. When all of the positions are filled, the queue
    is considered :meth:`full`.

    :param length: see :attr:`length`.


    .. attribute:: length

        The number of items required for the queue to be considered full.

    """

    class UnfilledPosition(object):
        """Describes an unfilled slot."""

        def __init__(self, position):
            self.position = position

    def __init__(self, length):
        self.length = length
        self.data = map(self.UnfilledPosition, xrange(length))

    def full(self):
        """Returns ``True`` if all of the slots has been filled."""
        return len(self) >= self.length

    def __len__(self):
        """``len(self)`` -> number of slots filled with real values."""
        return len(self.filled)

    @property
    def filled(self):
        """Returns the filled slots as a list."""
        return filter(lambda v: not isinstance(v, self.UnfilledPosition),
                      self.data)


class TaskProcessQueue(object):
    """Queue of running child processes, which starts waiting for the
    processes to finish when the queue limit has been reached.

    :param limit: see :attr:`limit` attribute.

    :param logger: see :attr:`logger` attribute.

    :param done_msg: see :attr:`done_msg` attribute.


    .. attribute:: limit

        The number of processes that can run simultaneously until
        we start collecting results.

    .. attribute:: logger

        The logger used to print the :attr:`done_msg`.

    .. attribute:: done_msg

        Message logged when a tasks result has been collected.
        The message is logged with loglevel :const:`logging.INFO`.

    """

    def __init__(self, limit, reap_timeout=None,
            logger=None, done_msg=None):
        self.limit = limit
        self.logger = logger or multiprocessing.get_logger()
        self.done_msg = done_msg
        self.reap_timeout = reap_timeout
        self._process_counter = itertools.count(1)
        self._processed_total = 0
        self._data_lock = threading.Condition(threading.Lock())
        self._start()

    def _start(self):
        self._processes = {}
        self._pool = multiprocessing.Pool(processes=self.limit)

    def _terminate_and_restart(self):
        try:
            self._pool.terminate()
        except OSError:
            pass
        self._start()

    def _restart(self):
        self.logger.info("Closing and restarting the pool...")
        self._pool.close()
        timeout_thread = threading.Timer(30.0, self._terminate_and_restart)
        timeout_thread.start()
        self._pool.join()
        timeout_thread.cancel()
        self._start()

    def _pool_is_running(self):
        return self._pool._state == POOL_STATE_RUN

    def apply_async(self, target, args, kwargs, task_name, task_id):

        if not self._pool_is_running():
            self._start()

        self._processed_total = self._process_counter.next()

        on_return = lambda r: self.on_return(r, task_name, task_id)

        result = self._pool.apply_async(target, args, kwargs,
                                           callback=on_return)
        self.add(result, task_name, task_id)

        return result

    def on_return(self, ret_val, task_name, task_id):
        try:
            del(self._processes[task_id])
        except KeyError:
            pass
        else:
            self.on_ready(ret_val, task_name, task_id)

    def add(self, result, task_name, task_id):
        """Add a process to the queue.

        If the queue is full, it will wait for the first task to finish,
        collects its result and remove it from the queue, so it's ready
        to accept new processes.

        :param result: A :class:`multiprocessing.AsyncResult` instance, as
            returned by :meth:`multiprocessing.Pool.apply_async`.

        :param task_name: Name of the task executed.

        :param task_id: Id of the task executed.

        """

        self._processes[task_id] = [result, task_name]

        if self.full():
            self.wait_for_result()

    def _is_alive(self, pid):
        """Uses non-blocking ``waitpid`` to see if a process is still alive.

        :param pid: The process id of the process.

        :returns: ``True`` if the process is still running, ``False``
            otherwise.

        :rtype: bool

        """
        try:
            is_alive = os.waitpid(pid, os.WNOHANG) == (0, 0)
        except OSError, e:
            if e.errno != errno.ECHILD:
                raise
        return is_alive

    def _reap_zombies(self):
        assert hasattr(self._pool, "_pool")
        self.logger.debug("Trying to find zombies...")
        for process in self._pool._pool:
            pid = process.pid
            if not self._is_alive(pid):
                self.logger.error(
                        "Process with pid %d is dead? Restarting pool" % pid)
                self._restart()

    def full(self):
        return len(self._processes.values()) >= self.limit

    def wait_for_result(self):
        """Waits for the first process in the pool to finish.

        This operation is blocking.

        """
        while True:
            if self.reap():
                break
            #self._reap_zombies()

    def reap(self):
        self.logger.debug("Reaping processes...")
        processes_reaped = 0
        for process_no, entry in enumerate(self._processes.items()):
            task_id, process_info = entry
            result, task_name = process_info
            try:
                ret_value = result.get(timeout=0.3)
            except multiprocessing.TimeoutError:
                continue
            else:
                self.on_return(ret_value, task_name, task_id)
                processes_reaped += 1
        return processes_reaped

    def get_worker_pids(self):
        """Returns the process id's of all the pool workers.

        :rtype: list

        """
        return [process.pid for process in self._pool._pool]

    def on_ready(self, ret_value, task_name, task_id):
        """What to do when a worker returns with a result.

        If :attr:`done_msg` is defined, it will log this
        format string, with level ``logging.INFO``,
        using these format variables:

            * %(name)

                The name of the task completed

            * %(id)

                The UUID of the task completed.

            * %(return_value)

                Return value of the task function.

        """
        if self.done_msg:
            from celery.worker import ExcInfo
            msg = self.done_msg % {
                    "name": task_name,
                    "id": task_id,
                    "return_value": ret_value}
            if isinstance(ret_value, ExcInfo):
                self.logger.error(msg)
                if SEND_CELERY_TASK_ERROR_EMAILS is True:
                    mail_admins(msg, ret_value.traceback, fail_silently=True)
            else:
                self.logger.info(msg)
