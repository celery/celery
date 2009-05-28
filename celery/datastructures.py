"""

Custom Datastructures

"""
import multiprocessing
import itertools
import threading
import time
from UserList import UserList


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


class TaskWorkerPool(object):

    Process = multiprocessing.Process

    class TaskWorker(object):
        def __init__(self, process, task_name, task_id):
            self.process = process
            self.task_name = task_name
            self.task_id = task_id

    def __init__(self, limit, logger=None, done_msg=None):
        self.limit = limit
        self.logger = logger
        self.done_msg = done_msg
        self._pool = []
        self.task_counter = itertools.count(1)
        self.total_tasks_run = 0

    def add(self, target, args, kwargs=None, task_name=None, task_id=None):
        self.total_tasks_run = self.task_counter.next()
        if self._pool and len(self._pool) >= self.limit:
            self.wait_for_result()
        else:
            self.reap()

        current_worker_no = len(self._pool) + 1
        process_name = "TaskWorker-%d" % current_worker_no
        process = self.Process(target=target, args=args, kwargs=kwargs,
                               name=process_name)
        process.start()
        task = self.TaskWorker(process, task_name, task_id)
        self._pool.append(task)

    def wait_for_result(self):
        """Collect results from processes that are ready."""
        while True:
            if self.reap():
                break
            time.sleep(0.1)
            
    def reap(self):
        processed_reaped = 0
        for worker_no, worker in enumerate(self._pool):
            process = worker.process
            if not process.is_alive():
                ret_value = process.join()
                self.on_finished(ret_value, worker.task_name,
                        worker.task_id)
                del(self._pool[worker_no])
                processed_reaped += 1
        return processed_reaped

    def on_finished(self, ret_value, task_name, task_id):
        if self.done_msg and self.logger:
            self.logger.info(self.done_msg % {
                "name": task_name,
                "id": task_id,
                "return_value": ret_value})


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

    def __init__(self, limit, process_timeout=None,
            logger=None, done_msg=None):
        self.limit = limit
        self.logger = logger
        self.done_msg = done_msg
        self.process_timeout = process_timeout
        self._processes = {}
        self._process_counter = itertools.count(1)
        self._data_lock = threading.Condition(threading.Lock())
        self.pool = multiprocessing.Pool(limit)

    def apply_async(self, target, args, kwargs, task_name, task_id):
        #self._data_lock.acquire()
        try:
            _pid = self._process_counter.next()

            on_return = lambda ret_val: self.on_return(_pid, ret_val,
                                                       task_name, task_id)

            result = self.pool.apply_async(target, args, kwargs,
                                           callback=on_return)
            self.add(_pid, result, task_name, task_id)
        finally:
            pass
            #self._data_lock.release()

        return result

    def on_return(self, _pid, ret_val, task_name, task_id):
        #self._data_lock.acquire()
        try:
            del(self._processes[_pid])
        except KeyError:
            pass
        else:
            self.on_ready(ret_val, task_name, task_id)
        finally:
            pass
            #self._data_lock.acquire()

    def add(self, _pid, result, task_name, task_id):
        """Add a process to the queue.

        If the queue is full, it will wait for the first task to finish,
        collects its result and remove it from the queue, so it's ready
        to accept new processes.

        :param result: A :class:`multiprocessing.AsyncResult` instance, as
            returned by :meth:`multiprocessing.Pool.apply_async`.

        :param task_name: Name of the task executed.

        :param task_id: Id of the task executed.

        """
      
        self._processes[_pid] = [result, task_name, task_id]

        if self.full():
            self.wait_for_result()

    def full(self):
        return len(self._processes.values()) >= self.limit


    def wait_for_result(self):
        """Collect results from processes that are ready."""
        assert self.full()
        while True:
            if self.reap():
                break

    def reap(self):
        processes_reaped = 0
        for process_no, entry in enumerate(self._processes.items()):
            _pid, process_info = entry
            result, task_name, task_id = process_info
            try:
                ret_value = result.get(timeout=0.1)
            except multiprocessing.TimeoutError:
                continue
            else:
                self.on_return(_pid, ret_value, task_name, task_id)
                processes_reaped += 1
        return processes_reaped


    def on_ready(self, ret_value, task_name, task_id):
        if self.done_msg and self.logger:
            self.logger.info(self.done_msg % {
                "name": task_name,
                "id": task_id,
                "return_value": ret_value})
