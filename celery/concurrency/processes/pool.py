# -*- coding: utf-8 -*-
#
# Module providing the `Pool` class for managing a process pool
#
# multiprocessing/pool.py
#
# Copyright (c) 2007-2008, R Oudkerk --- see COPYING.txt
#
from __future__ import absolute_import

#
# Imports
#

import collections
import errno
import itertools
import logging
import os
import signal
import sys
import threading
import time
import Queue
import warnings

from multiprocessing import cpu_count, TimeoutError, Event
from multiprocessing import util
from multiprocessing.util import Finalize, debug

from celery.datastructures import ExceptionInfo
from celery.exceptions import SoftTimeLimitExceeded, TimeLimitExceeded
from celery.exceptions import WorkerLostError

from .process import Process

_Semaphore = threading._Semaphore

#
# Constants representing the state of a pool
#

RUN = 0
CLOSE = 1
TERMINATE = 2

#
# Constants representing the state of a job
#

ACK = 0
READY = 1

# Signal used for soft time limits.
SIG_SOFT_TIMEOUT = getattr(signal, "SIGUSR1", None)

#
# Miscellaneous
#

job_counter = itertools.count()


def mapstar(args):
    return map(*args)


def error(msg, *args, **kwargs):
    if util._logger:
        util._logger.error(msg, *args, **kwargs)


def safe_apply_callback(fun, *args):
    if fun:
        try:
            fun(*args)
        except BaseException, exc:
            error("Pool callback raised exception: %r", exc,
                  exc_info=sys.exc_info())


class LaxBoundedSemaphore(threading._Semaphore):
    """Semaphore that checks that # release is <= # acquires,
    but ignores if # releases >= value."""

    def __init__(self, value=1, verbose=None):
        _Semaphore.__init__(self, value, verbose)
        self._initial_value = value

    if sys.version_info >= (3, 0):

        def release(self):
            if self._value < self._initial_value:
                _Semaphore.release(self)
            if __debug__:
                self._note("%s.release: success, value=%s (unchanged)" % (
                    self, self._value))

        def clear(self):
            while self._value < self._initial_value:
                _Semaphore.release(self)
    else:

        def release(self):  # noqa
            if self._Semaphore__value < self._initial_value:
                _Semaphore.release(self)
            if __debug__:
                self._note("%s.release: success, value=%s (unchanged)" % (
                    self, self._Semaphore__value))

        def clear(self):  # noqa
            while self._Semaphore__value < self._initial_value:
                _Semaphore.release(self)

#
# Exceptions
#


class MaybeEncodingError(Exception):
    """Wraps unpickleable object."""

    def __init__(self, exc, value):
        self.exc = str(exc)
        self.value = repr(value)
        Exception.__init__(self, self.exc, self.value)

    def __repr__(self):
        return "<MaybeEncodingError: %s>" % str(self)

    def __str__(self):
        return "Error sending result: '%s'. Reason: '%s'." % (
                    self.value, self.exc)


class WorkersJoined(Exception):
    """All workers have terminated."""


def soft_timeout_sighandler(signum, frame):
    raise SoftTimeLimitExceeded()

#
# Code run by worker processes
#


def worker(inqueue, outqueue, initializer=None, initargs=(),
           maxtasks=None, sentinel=None):
    # Re-init logging system.
    # Workaround for http://bugs.python.org/issue6721#msg140215
    # Python logging module uses RLock() objects which are broken after
    # fork. This can result in a deadlock (Issue #496).
    logger_names = logging.Logger.manager.loggerDict.keys()
    logger_names.append(None)  # for root logger
    for name in logger_names:
        for handler in logging.getLogger(name).handlers:
            handler.createLock()
    logging._lock = threading.RLock()

    pid = os.getpid()
    assert maxtasks is None or (type(maxtasks) == int and maxtasks > 0)
    put = outqueue.put
    get = inqueue.get

    if hasattr(inqueue, '_reader'):

        def poll(timeout):
            if inqueue._reader.poll(timeout):
                return True, get()
            return False, None
    else:

        def poll(timeout):  # noqa
            try:
                return True, get(timeout=timeout)
            except Queue.Empty:
                return False, None

    if hasattr(inqueue, '_writer'):
        inqueue._writer.close()
        outqueue._reader.close()

    if initializer is not None:
        initializer(*initargs)

    if SIG_SOFT_TIMEOUT is not None:
        signal.signal(SIG_SOFT_TIMEOUT, soft_timeout_sighandler)

    completed = 0
    while maxtasks is None or (maxtasks and completed < maxtasks):
        if sentinel is not None and sentinel.is_set():
            debug('worker got sentinel -- exiting')
            break

        try:
            ready, task = poll(1.0)
            if not ready:
                continue
        except (EOFError, IOError):
            debug('worker got EOFError or IOError -- exiting')
            break

        if task is None:
            debug('worker got sentinel -- exiting')
            break

        job, i, func, args, kwds = task
        put((ACK, (job, i, time.time(), pid)))
        try:
            result = (True, func(*args, **kwds))
        except Exception:
            result = (False, ExceptionInfo(sys.exc_info()))
        try:
            put((READY, (job, i, result)))
        except Exception, exc:
            _, _, tb = sys.exc_info()
            wrapped = MaybeEncodingError(exc, result[1])
            einfo = ExceptionInfo((MaybeEncodingError, wrapped, tb))
            put((READY, (job, i, (False, einfo))))

        completed += 1
    debug('worker exiting after %d tasks' % completed)

#
# Class representing a process pool
#


class PoolThread(threading.Thread):

    def __init__(self, *args, **kwargs):
        threading.Thread.__init__(self)
        self._state = RUN
        self.daemon = True

    def run(self):
        try:
            return self.body()
        except Exception, exc:
            error("Thread %r crashed: %r" % (self.__class__.__name__, exc, ),
                  exc_info=sys.exc_info())
            os._exit(1)

    def terminate(self):
        self._state = TERMINATE

    def close(self):
        self._state = CLOSE


class Supervisor(PoolThread):

    def __init__(self, pool):
        self.pool = pool
        super(Supervisor, self).__init__()

    def body(self):
        debug('worker handler starting')
        while self._state == RUN and self.pool._state == RUN:
            self.pool._maintain_pool()
            time.sleep(0.8)
        debug('worker handler exiting')


class TaskHandler(PoolThread):

    def __init__(self, taskqueue, put, outqueue, pool):
        self.taskqueue = taskqueue
        self.put = put
        self.outqueue = outqueue
        self.pool = pool
        super(TaskHandler, self).__init__()

    def body(self):
        taskqueue = self.taskqueue
        outqueue = self.outqueue
        put = self.put
        pool = self.pool

        for taskseq, set_length in iter(taskqueue.get, None):
            i = -1
            for i, task in enumerate(taskseq):
                if self._state:
                    debug('task handler found thread._state != RUN')
                    break
                try:
                    put(task)
                except IOError:
                    debug('could not put task on queue')
                    break
            else:
                if set_length:
                    debug('doing set_length()')
                    set_length(i + 1)
                continue
            break
        else:
            debug('task handler got sentinel')

        try:
            # tell result handler to finish when cache is empty
            debug('task handler sending sentinel to result handler')
            outqueue.put(None)

            # tell workers there is no more work
            debug('task handler sending sentinel to workers')
            for p in pool:
                put(None)
        except IOError:
            debug('task handler got IOError when sending sentinels')

        debug('task handler exiting')


class TimeoutHandler(PoolThread):

    def __init__(self, processes, cache, t_soft, t_hard):
        self.processes = processes
        self.cache = cache
        self.t_soft = t_soft
        self.t_hard = t_hard
        super(TimeoutHandler, self).__init__()

    def body(self):
        processes = self.processes
        cache = self.cache
        t_hard, t_soft = self.t_hard, self.t_soft
        dirty = set()

        def _process_by_pid(pid):
            for index, process in enumerate(processes):
                if process.pid == pid:
                    return process, index
            return None, None

        def _timed_out(start, timeout):
            if not start or not timeout:
                return False
            if time.time() >= start + timeout:
                return True

        def _on_soft_timeout(job, i, soft_timeout):
            debug('soft time limit exceeded for %i' % i)
            process, _index = _process_by_pid(job._worker_pid)
            if not process:
                return

            # Run timeout callback
            if job._timeout_callback is not None:
                job._timeout_callback(soft=True, timeout=soft_timeout)

            try:
                os.kill(job._worker_pid, SIG_SOFT_TIMEOUT)
            except OSError, exc:
                if exc.errno == errno.ESRCH:
                    pass
                else:
                    raise

            dirty.add(i)

        def _on_hard_timeout(job, i, hard_timeout):
            if job.ready():
                return
            debug('hard time limit exceeded for %i', i)
            # Remove from cache and set return value to an exception
            exc_info = None
            try:
                raise TimeLimitExceeded(hard_timeout)
            except TimeLimitExceeded:
                exc_info = sys.exc_info()
            job._set(i, (False, ExceptionInfo(exc_info)))

            # Remove from _pool
            process, _index = _process_by_pid(job._worker_pid)

            # Run timeout callback
            if job._timeout_callback is not None:
                job._timeout_callback(soft=False, timeout=hard_timeout)
            if process:
                process.terminate()

        # Inner-loop
        while self._state == RUN:

            # Remove dirty items not in cache anymore
            if dirty:
                dirty = set(k for k in dirty if k in cache)

            for i, job in cache.items():
                ack_time = job._time_accepted
                soft_timeout = job._soft_timeout
                if soft_timeout is None:
                    soft_timeout = t_soft
                hard_timeout = job._timeout
                if hard_timeout is None:
                    hard_timeout = t_hard
                if _timed_out(ack_time, hard_timeout):
                    _on_hard_timeout(job, i, hard_timeout)
                elif i not in dirty and _timed_out(ack_time, soft_timeout):
                    _on_soft_timeout(job, i, soft_timeout)

            time.sleep(0.5)                     # Don't waste CPU cycles.

        debug('timeout handler exiting')


class ResultHandler(PoolThread):

    def __init__(self, outqueue, get, cache, poll,
            join_exited_workers, putlock):
        self.outqueue = outqueue
        self.get = get
        self.cache = cache
        self.poll = poll
        self.join_exited_workers = join_exited_workers
        self.putlock = putlock
        super(ResultHandler, self).__init__()

    def body(self):
        get = self.get
        outqueue = self.outqueue
        cache = self.cache
        poll = self.poll
        join_exited_workers = self.join_exited_workers
        putlock = self.putlock

        def on_ack(job, i, time_accepted, pid):
            try:
                cache[job]._ack(i, time_accepted, pid)
            except (KeyError, AttributeError):
                # Object gone or doesn't support _ack (e.g. IMAPIterator).
                pass

        def on_ready(job, i, obj):
            try:
                item = cache[job]
            except KeyError:
                return
            if not item.ready():
                if putlock is not None:
                    putlock.release()
            try:
                item._set(i, obj)
            except KeyError:
                pass

        state_handlers = {ACK: on_ack, READY: on_ready}

        def on_state_change(task):
            state, args = task
            try:
                state_handlers[state](*args)
            except KeyError:
                debug("Unknown job state: %s (args=%s)" % (state, args))

        debug('result handler starting')
        while 1:
            try:
                ready, task = poll(1.0)
            except (IOError, EOFError), exc:
                debug('result handler got %r -- exiting' % (exc, ))
                return

            if self._state:
                assert self._state == TERMINATE
                debug('result handler found thread._state=TERMINATE')
                break

            if ready:
                if task is None:
                    debug('result handler got sentinel')
                    break

                on_state_change(task)

        time_terminate = None
        while cache and self._state != TERMINATE:
            try:
                ready, task = poll(1.0)
            except (IOError, EOFError), exc:
                debug('result handler got %r -- exiting' % (exc, ))
                return

            if ready:
                if task is None:
                    debug('result handler ignoring extra sentinel')
                    continue

                on_state_change(task)
            try:
                join_exited_workers(shutdown=True)
            except WorkersJoined:
                now = time.time()
                if not time_terminate:
                    time_terminate = now
                else:
                    if now - time_terminate > 5.0:
                        debug('result handler exiting: timed out')
                        break
                    debug('result handler: all workers terminated, '
                          'timeout in %ss' % (
                              abs(min(now - time_terminate - 5.0, 0))))

        if hasattr(outqueue, '_reader'):
            debug('ensuring that outqueue is not full')
            # If we don't make room available in outqueue then
            # attempts to add the sentinel (None) to outqueue may
            # block.  There is guaranteed to be no more than 2 sentinels.
            try:
                for i in range(10):
                    if not outqueue._reader.poll():
                        break
                    get()
            except (IOError, EOFError):
                pass

        debug('result handler exiting: len(cache)=%s, thread._state=%s',
              len(cache), self._state)


class Pool(object):
    '''
    Class which supports an async version of the `apply()` builtin
    '''
    Process = Process
    Supervisor = Supervisor
    TaskHandler = TaskHandler
    TimeoutHandler = TimeoutHandler
    ResultHandler = ResultHandler
    SoftTimeLimitExceeded = SoftTimeLimitExceeded

    def __init__(self, processes=None, initializer=None, initargs=(),
            maxtasksperchild=None, timeout=None, soft_timeout=None,
            force_execv=False):
        self._setup_queues()
        self._taskqueue = Queue.Queue()
        self._cache = {}
        self._state = RUN
        self.timeout = timeout
        self.soft_timeout = soft_timeout
        self._maxtasksperchild = maxtasksperchild
        self._initializer = initializer
        self._initargs = initargs
        self._force_execv = force_execv

        if soft_timeout and SIG_SOFT_TIMEOUT is None:
            warnings.warn(UserWarning("Soft timeouts are not supported: "
                    "on this platform: It does not have the SIGUSR1 signal."))
            soft_timeout = None

        if processes is None:
            try:
                processes = cpu_count()
            except NotImplementedError:
                processes = 1
        self._processes = processes

        if initializer is not None and not hasattr(initializer, '__call__'):
            raise TypeError('initializer must be a callable')

        self._pool = []
        self._poolctrl = {}
        for i in range(processes):
            self._create_worker_process()

        self._worker_handler = self.Supervisor(self)
        self._worker_handler.start()

        self._putlock = LaxBoundedSemaphore(self._processes)
        self._task_handler = self.TaskHandler(self._taskqueue,
                                              self._quick_put,
                                              self._outqueue,
                                              self._pool)
        self._task_handler.start()

        # Thread killing timedout jobs.
        self._timeout_handler = None
        self._timeout_handler_mutex = threading.Lock()
        if self.timeout is not None or self.soft_timeout is not None:
            self._start_timeout_handler()

        # Thread processing results in the outqueue.
        self._result_handler = self.ResultHandler(self._outqueue,
                                        self._quick_get, self._cache,
                                        self._poll_result,
                                        self._join_exited_workers,
                                        self._putlock)
        self._result_handler.start()

        self._terminate = Finalize(
            self, self._terminate_pool,
            args=(self._taskqueue, self._inqueue, self._outqueue,
                  self._pool, self._worker_handler, self._task_handler,
                  self._result_handler, self._cache,
                  self._timeout_handler),
            exitpriority=15,
            )

    def _create_worker_process(self):
        sentinel = Event()
        w = self.Process(
            force_execv=self._force_execv,
            target=worker,
            args=(self._inqueue, self._outqueue,
                    self._initializer, self._initargs,
                    self._maxtasksperchild,
                    sentinel),
            )
        self._pool.append(w)
        w.name = w.name.replace('Process', 'PoolWorker')
        w.daemon = True
        w.start()
        self._poolctrl[w.pid] = sentinel
        return w

    def _join_exited_workers(self, shutdown=False):
        """Cleanup after any worker processes which have exited due to
        reaching their specified lifetime. Returns True if any workers were
        cleaned up.
        """
        now = None
        # The worker may have published a result before being terminated,
        # but we have no way to accurately tell if it did.  So we wait for
        # _lost_worker_timeout seconds before we mark the job with
        # WorkerLostError.
        for job in [job for job in self._cache.values()
                if not job.ready() and job._worker_lost]:
            now = now or time.time()
            if now - job._worker_lost > job._lost_worker_timeout:
                exc_info = None
                try:
                    raise WorkerLostError("Worker exited prematurely.")
                except WorkerLostError:
                    exc_info = ExceptionInfo(sys.exc_info())
                job._set(None, (False, exc_info))

        if shutdown and not len(self._pool):
            raise WorkersJoined()

        cleaned = []
        for i in reversed(range(len(self._pool))):
            worker = self._pool[i]
            if worker.exitcode is not None:
                # worker exited
                debug('Supervisor: cleaning up worker %d' % i)
                worker.join()
                debug('Supervisor: worked %d joined' % i)
                cleaned.append(worker.pid)
                del self._pool[i]
                del self._poolctrl[worker.pid]
        if cleaned:
            for job in self._cache.values():
                for worker_pid in job.worker_pids():
                    if worker_pid in cleaned and not job.ready():
                        job._worker_lost = time.time()
                        continue
            if self._putlock is not None:
                for worker in cleaned:
                    self._putlock.release()
            return True
        return False

    def shrink(self, n=1):
        for i, worker in enumerate(self._iterinactive()):
            self._processes -= 1
            if self._putlock:
                self._putlock._initial_value -= 1
                self._putlock.acquire()
            worker.terminate()
            if i == n - 1:
                return
        raise ValueError("Can't shrink pool. All processes busy!")

    def grow(self, n=1):
        for i in xrange(n):
            #assert len(self._pool) == self._processes
            self._processes += 1
            if self._putlock:
                cond = self._putlock._Semaphore__cond
                cond.acquire()
                try:
                    self._putlock._initial_value += 1
                    self._putlock._Semaphore__value += 1
                    cond.notify()
                finally:
                    cond.release()

    def _iterinactive(self):
        for worker in self._pool:
            if not self._worker_active(worker):
                yield worker
        raise StopIteration()

    def _worker_active(self, worker):
        for job in self._cache.values():
            if worker.pid in job.worker_pids():
                return True
        return False

    def _repopulate_pool(self):
        """Bring the number of pool processes up to the specified number,
        for use after reaping workers which have exited.
        """
        for i in range(self._processes - len(self._pool)):
            if self._state != RUN:
                return
            self._create_worker_process()
            debug('added worker')

    def _maintain_pool(self):
        """"Clean up any exited workers and start replacements for them.
        """
        self._join_exited_workers()
        self._repopulate_pool()

    def _setup_queues(self):
        from multiprocessing.queues import SimpleQueue
        self._inqueue = SimpleQueue()
        self._outqueue = SimpleQueue()
        self._quick_put = self._inqueue._writer.send
        self._quick_get = self._outqueue._reader.recv

        def _poll_result(timeout):
            if self._outqueue._reader.poll(timeout):
                return True, self._quick_get()
            return False, None
        self._poll_result = _poll_result

    def _start_timeout_handler(self):
        # ensure more than one thread does not start the timeout handler
        # thread at once.
        self._timeout_handler_mutex.acquire()
        try:
            if self._timeout_handler is None:
                self._timeout_handler = self.TimeoutHandler(
                        self._pool, self._cache,
                        self.soft_timeout, self.timeout)
                self._timeout_handler.start()
        finally:
            self._timeout_handler_mutex.release()

    def apply(self, func, args=(), kwds={}):
        '''
        Equivalent of `apply()` builtin
        '''
        assert self._state == RUN
        return self.apply_async(func, args, kwds).get()

    def map(self, func, iterable, chunksize=None):
        '''
        Equivalent of `map()` builtin
        '''
        assert self._state == RUN
        return self.map_async(func, iterable, chunksize).get()

    def imap(self, func, iterable, chunksize=1, lost_worker_timeout=10.0):
        '''
        Equivalent of `itertools.imap()` -- can be MUCH slower
        than `Pool.map()`
        '''
        assert self._state == RUN
        if chunksize == 1:
            result = IMapIterator(self._cache,
                                  lost_worker_timeout=lost_worker_timeout)
            self._taskqueue.put((((result._job, i, func, (x,), {})
                         for i, x in enumerate(iterable)), result._set_length))
            return result
        else:
            assert chunksize > 1
            task_batches = Pool._get_tasks(func, iterable, chunksize)
            result = IMapIterator(self._cache,
                                  lost_worker_timeout=lost_worker_timeout)
            self._taskqueue.put((((result._job, i, mapstar, (x,), {})
                     for i, x in enumerate(task_batches)), result._set_length))
            return (item for chunk in result for item in chunk)

    def imap_unordered(self, func, iterable, chunksize=1,
                       lost_worker_timeout=10.0):
        '''
        Like `imap()` method but ordering of results is arbitrary
        '''
        assert self._state == RUN
        if chunksize == 1:
            result = IMapUnorderedIterator(self._cache,
                    lost_worker_timeout=lost_worker_timeout)
            self._taskqueue.put((((result._job, i, func, (x,), {})
                         for i, x in enumerate(iterable)), result._set_length))
            return result
        else:
            assert chunksize > 1
            task_batches = Pool._get_tasks(func, iterable, chunksize)
            result = IMapUnorderedIterator(self._cache,
                    lost_worker_timeout=lost_worker_timeout)
            self._taskqueue.put((((result._job, i, mapstar, (x,), {})
                     for i, x in enumerate(task_batches)), result._set_length))
            return (item for chunk in result for item in chunk)

    def apply_async(self, func, args=(), kwds={},
            callback=None, accept_callback=None, timeout_callback=None,
            waitforslot=False, error_callback=None,
            soft_timeout=None, timeout=None):
        '''
        Asynchronous equivalent of `apply()` builtin.

        Callback is called when the functions return value is ready.
        The accept callback is called when the job is accepted to be executed.

        Simplified the flow is like this:

            >>> if accept_callback:
            ...     accept_callback()
            >>> retval = func(*args, **kwds)
            >>> if callback:
            ...     callback(retval)

        '''
        assert self._state == RUN
        if soft_timeout and SIG_SOFT_TIMEOUT is None:
            warnings.warn(UserWarning("Soft timeouts are not supported: "
                    "on this platform: It does not have the SIGUSR1 signal."))
            soft_timeout = None
        if waitforslot and self._putlock is not None and self._state == RUN:
            self._putlock.acquire()
        if self._state == RUN:
            result = ApplyResult(self._cache, callback,
                                 accept_callback, timeout_callback,
                                 error_callback, soft_timeout, timeout)
            if timeout or soft_timeout:
                # start the timeout handler thread when required.
                self._start_timeout_handler()
            self._taskqueue.put(([(result._job, None,
                                   func, args, kwds)], None))
            return result

    def map_async(self, func, iterable, chunksize=None, callback=None):
        '''
        Asynchronous equivalent of `map()` builtin
        '''
        assert self._state == RUN
        if not hasattr(iterable, '__len__'):
            iterable = list(iterable)

        if chunksize is None:
            chunksize, extra = divmod(len(iterable), len(self._pool) * 4)
            if extra:
                chunksize += 1
        if len(iterable) == 0:
            chunksize = 0

        task_batches = Pool._get_tasks(func, iterable, chunksize)
        result = MapResult(self._cache, chunksize, len(iterable), callback)
        self._taskqueue.put((((result._job, i, mapstar, (x,), {})
                              for i, x in enumerate(task_batches)), None))
        return result

    @staticmethod
    def _get_tasks(func, it, size):
        it = iter(it)
        while 1:
            x = tuple(itertools.islice(it, size))
            if not x:
                return
            yield (func, x)

    def __reduce__(self):
        raise NotImplementedError(
              'pool objects cannot be passed between '
              'processes or pickled')

    def close(self):
        debug('closing pool')
        if self._state == RUN:
            self._state = CLOSE
            self._worker_handler.close()
            self._worker_handler.join()
            self._taskqueue.put(None)
            if self._putlock:
                self._putlock.clear()

    def terminate(self):
        debug('terminating pool')
        self._state = TERMINATE
        self._worker_handler.terminate()
        self._terminate()

    def join(self):
        assert self._state in (CLOSE, TERMINATE)
        debug('joining worker handler')
        self._worker_handler.join()
        debug('joining task handler')
        self._task_handler.join()
        debug('joining result handler')
        self._result_handler.join()
        debug('result handler joined')
        for i, p in enumerate(self._pool):
            debug('joining worker %s/%s (%r)' % (i, len(self._pool), p, ))
            p.join()

    def restart(self):
        for e in self._poolctrl.itervalues():
            e.set()

    @staticmethod
    def _help_stuff_finish(inqueue, task_handler, size):
        # task_handler may be blocked trying to put items on inqueue
        debug('removing tasks from inqueue until task handler finished')
        inqueue._rlock.acquire()
        while task_handler.is_alive() and inqueue._reader.poll():
            inqueue._reader.recv()
            time.sleep(0)

    @classmethod
    def _terminate_pool(cls, taskqueue, inqueue, outqueue, pool,
                        worker_handler, task_handler,
                        result_handler, cache, timeout_handler):

        # this is guaranteed to only be called once
        debug('finalizing pool')

        worker_handler.terminate()

        task_handler.terminate()
        taskqueue.put(None)                 # sentinel

        debug('helping task handler/workers to finish')
        cls._help_stuff_finish(inqueue, task_handler, len(pool))

        result_handler.terminate()
        outqueue.put(None)                  # sentinel

        if timeout_handler is not None:
            timeout_handler.terminate()

        # Terminate workers which haven't already finished
        if pool and hasattr(pool[0], 'terminate'):
            debug('terminating workers')
            for p in pool:
                if p.exitcode is None:
                    p.terminate()

        debug('joining task handler')
        task_handler.join(1e100)

        debug('joining result handler')
        result_handler.join(1e100)

        if timeout_handler is not None:
            debug('joining timeout handler')
            timeout_handler.join(1e100)

        if pool and hasattr(pool[0], 'terminate'):
            debug('joining pool workers')
            for p in pool:
                if p.is_alive():
                    # worker has not yet exited
                    debug('cleaning up worker %d' % p.pid)
                    p.join()
            debug('pool workers joined')
DynamicPool = Pool

#
# Class whose instances are returned by `Pool.apply_async()`
#


class ApplyResult(object):
    _worker_lost = None

    def __init__(self, cache, callback, accept_callback=None,
            timeout_callback=None, error_callback=None, soft_timeout=None,
            timeout=None, lost_worker_timeout=10.0):
        self._mutex = threading.Lock()
        self._cond = threading.Condition(threading.Lock())
        self._job = job_counter.next()
        self._cache = cache
        self._ready = False
        self._callback = callback
        self._accept_callback = accept_callback
        self._errback = error_callback
        self._timeout_callback = timeout_callback
        self._timeout = timeout
        self._soft_timeout = soft_timeout
        self._lost_worker_timeout = lost_worker_timeout

        self._accepted = False
        self._worker_pid = None
        self._time_accepted = None
        cache[self._job] = self

    def ready(self):
        return self._ready

    def accepted(self):
        return self._accepted

    def successful(self):
        assert self._ready
        return self._success

    def worker_pids(self):
        return filter(None, [self._worker_pid])

    def wait(self, timeout=None):
        self._cond.acquire()
        try:
            if not self._ready:
                self._cond.wait(timeout)
        finally:
            self._cond.release()

    def get(self, timeout=None):
        self.wait(timeout)
        if not self._ready:
            raise TimeoutError
        if self._success:
            return self._value
        else:
            raise self._value

    def _set(self, i, obj):
        self._mutex.acquire()
        try:
            self._success, self._value = obj
            self._cond.acquire()
            try:
                self._ready = True
                self._cond.notify()
            finally:
                self._cond.release()
            if self._accepted:
                self._cache.pop(self._job, None)

            # apply callbacks last
            if self._callback and self._success:
                safe_apply_callback(
                    self._callback, self._value)
            if self._errback and not self._success:
                safe_apply_callback(
                    self._errback, self._value)
        finally:
            self._mutex.release()

    def _ack(self, i, time_accepted, pid):
        self._mutex.acquire()
        try:
            self._accepted = True
            self._time_accepted = time_accepted
            self._worker_pid = pid
            if self._ready:
                self._cache.pop(self._job, None)
            if self._accept_callback:
                safe_apply_callback(
                    self._accept_callback, pid, time_accepted)
        finally:
            self._mutex.release()

#
# Class whose instances are returned by `Pool.map_async()`
#


class MapResult(ApplyResult):

    def __init__(self, cache, chunksize, length, callback):
        ApplyResult.__init__(self, cache, callback)
        self._success = True
        self._length = length
        self._value = [None] * length
        self._accepted = [False] * length
        self._worker_pid = [None] * length
        self._time_accepted = [None] * length
        self._chunksize = chunksize
        if chunksize <= 0:
            self._number_left = 0
            self._ready = True
        else:
            self._number_left = length // chunksize + bool(length % chunksize)

    def _set(self, i, success_result):
        success, result = success_result
        if success:
            self._value[i * self._chunksize:(i + 1) * self._chunksize] = result
            self._number_left -= 1
            if self._number_left == 0:
                if self._callback:
                    self._callback(self._value)
                if self._accepted:
                    self._cache.pop(self._job, None)
                self._cond.acquire()
                try:
                    self._ready = True
                    self._cond.notify()
                finally:
                    self._cond.release()

        else:
            self._success = False
            self._value = result
            if self._accepted:
                self._cache.pop(self._job, None)
            self._cond.acquire()
            try:
                self._ready = True
                self._cond.notify()
            finally:
                self._cond.release()

    def _ack(self, i, time_accepted, pid):
        start = i * self._chunksize
        stop = (i + 1) * self._chunksize
        for j in range(start, stop):
            self._accepted[j] = True
            self._worker_pid[j] = pid
            self._time_accepted[j] = time_accepted
        if self._ready:
            self._cache.pop(self._job, None)

    def accepted(self):
        return all(self._accepted)

    def worker_pids(self):
        return filter(None, self._worker_pid)

#
# Class whose instances are returned by `Pool.imap()`
#


class IMapIterator(object):
    _worker_lost = None

    def __init__(self, cache, lost_worker_timeout=10.0):
        self._cond = threading.Condition(threading.Lock())
        self._job = job_counter.next()
        self._cache = cache
        self._items = collections.deque()
        self._index = 0
        self._length = None
        self._ready = False
        self._unsorted = {}
        self._worker_pids = []
        self._lost_worker_timeout = lost_worker_timeout
        cache[self._job] = self

    def __iter__(self):
        return self

    def next(self, timeout=None):
        self._cond.acquire()
        try:
            try:
                item = self._items.popleft()
            except IndexError:
                if self._index == self._length:
                    self._ready = True
                    raise StopIteration
                self._cond.wait(timeout)
                try:
                    item = self._items.popleft()
                except IndexError:
                    if self._index == self._length:
                        self._ready = True
                        raise StopIteration
                    raise TimeoutError
        finally:
            self._cond.release()

        success, value = item
        if success:
            return value
        raise Exception(value)

    __next__ = next                    # XXX

    def _set(self, i, obj):
        self._cond.acquire()
        try:
            if self._index == i:
                self._items.append(obj)
                self._index += 1
                while self._index in self._unsorted:
                    obj = self._unsorted.pop(self._index)
                    self._items.append(obj)
                    self._index += 1
                self._cond.notify()
            else:
                self._unsorted[i] = obj

            if self._index == self._length:
                self._ready = True
                del self._cache[self._job]
        finally:
            self._cond.release()

    def _set_length(self, length):
        self._cond.acquire()
        try:
            self._length = length
            if self._index == self._length:
                self._ready = True
                self._cond.notify()
                del self._cache[self._job]
        finally:
            self._cond.release()

    def _ack(self, i, time_accepted, pid):
        self._worker_pids.append(pid)

    def ready(self):
        return self._ready

    def worker_pids(self):
        return self._worker_pids

#
# Class whose instances are returned by `Pool.imap_unordered()`
#


class IMapUnorderedIterator(IMapIterator):

    def _set(self, i, obj):
        self._cond.acquire()
        try:
            self._items.append(obj)
            self._index += 1
            self._cond.notify()
            if self._index == self._length:
                self._ready = True
                del self._cache[self._job]
        finally:
            self._cond.release()

#
#
#


class ThreadPool(Pool):

    from multiprocessing.dummy import Process as DummyProcess
    Process = DummyProcess

    def __init__(self, processes=None, initializer=None, initargs=()):
        Pool.__init__(self, processes, initializer, initargs)

    def _setup_queues(self):
        self._inqueue = Queue.Queue()
        self._outqueue = Queue.Queue()
        self._quick_put = self._inqueue.put
        self._quick_get = self._outqueue.get

        def _poll_result(timeout):
            try:
                return True, self._quick_get(timeout=timeout)
            except Queue.Empty:
                return False, None
        self._poll_result = _poll_result

    @staticmethod
    def _help_stuff_finish(inqueue, task_handler, size):
        # put sentinels at head of inqueue to make workers finish
        inqueue.not_empty.acquire()
        try:
            inqueue.queue.clear()
            inqueue.queue.extend([None] * size)
            inqueue.not_empty.notify_all()
        finally:
            inqueue.not_empty.release()
