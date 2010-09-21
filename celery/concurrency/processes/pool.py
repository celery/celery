#
# Module providing the `Pool` class for managing a process pool
#
# multiprocessing/pool.py
#
# Copyright (c) 2007-2008, R Oudkerk --- see COPYING.txt
#

__all__ = ['Pool']

#
# Imports
#

import os
import errno
import threading
import Queue
import itertools
import collections
import time
import signal

from multiprocessing import Process, cpu_count, TimeoutError
from multiprocessing.util import Finalize, debug

from celery.exceptions import SoftTimeLimitExceeded, TimeLimitExceeded

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
# Exceptions
#

class WorkerLostError(Exception):
    """The worker processing a job has exited prematurely."""
    pass

#
# Miscellaneous
#

job_counter = itertools.count()

def mapstar(args):
    return map(*args)

#
# Code run by worker processes
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


def soft_timeout_sighandler(signum, frame):
    raise SoftTimeLimitExceeded()


def worker(inqueue, outqueue, initializer=None, initargs=(), maxtasks=None):
    assert maxtasks is None or (type(maxtasks) == int and maxtasks > 0)
    pid = os.getpid()
    put = outqueue.put
    get = inqueue.get
    if hasattr(inqueue, '_writer'):
        inqueue._writer.close()
        outqueue._reader.close()

    if initializer is not None:
        initializer(*initargs)

    if SIG_SOFT_TIMEOUT is not None:
        signal.signal(SIG_SOFT_TIMEOUT, soft_timeout_sighandler)

    completed = 0
    while maxtasks is None or (maxtasks and completed < maxtasks):
        try:
            task = get()
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
        except Exception, e:
            result = (False, e)
        try:
            put((READY, (job, i, result)))
        except Exception, exc:
            wrapped = MaybeEncodingError(exc, result[1])
            put((READY, (job, i, (False, wrapped))))

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

    def terminate(self):
        self._state = TERMINATE

    def close(self):
        self._state = CLOSE


class Supervisor(PoolThread):

    def __init__(self, pool):
        self.pool = pool
        super(Supervisor, self).__init__()

    def run(self):
        debug('worker handler starting')
        while self._state == RUN and self.pool._state == RUN:
            self.pool._maintain_pool()
            time.sleep(0.1)
        debug('worker handler exiting')


class TaskHandler(PoolThread):

    def __init__(self, taskqueue, put, outqueue, pool):
        self.taskqueue = taskqueue
        self.put = put
        self.outqueue = outqueue
        self.pool = pool
        super(TaskHandler, self).__init__()

    def run(self):
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
                    set_length(i+1)
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

    def run(self):
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

        def _on_soft_timeout(job, i):
            debug('soft time limit exceeded for %i' % i)
            process, _index = _process_by_pid(job._worker_pid)
            if not process:
                return

            # Run timeout callback
            if job._timeout_callback is not None:
                job._timeout_callback(soft=True)

            try:
                os.kill(job._worker_pid, SIG_SOFT_TIMEOUT)
            except OSError, exc:
                if exc.errno == errno.ESRCH:
                    pass
                else:
                    raise

            dirty.add(i)

        def _on_hard_timeout(job, i):
            debug('hard time limit exceeded for %i', i)
            # Remove from _pool
            process, _index = _process_by_pid(job._worker_pid)
            # Remove from cache and set return value to an exception
            job._set(i, (False, TimeLimitExceeded()))
            # Run timeout callback
            if job._timeout_callback is not None:
                job._timeout_callback(soft=False)
            if not process:
                return
            # Terminate the process
            process.terminate()

        # Inner-loop
        while self._state == RUN:

            # Remove dirty items not in cache anymore
            if dirty:
                dirty = set(k for k in dirty if k in cache)

            for i, job in cache.items():
                ack_time = job._time_accepted
                if _timed_out(ack_time, t_hard):
                    _on_hard_timeout(job, i)
                elif i not in dirty and _timed_out(ack_time, t_soft):
                    _on_soft_timeout(job, i)

            time.sleep(0.5) # Don't waste CPU cycles.

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

    def run(self):
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
                cache[job]._set(i, obj)
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
                ready, task = poll(0.2)
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

                if putlock is not None:
                    try:
                        putlock.release()
                    except ValueError:
                        pass

                on_state_change(task)


        if putlock is not None:
            try:
                putlock.release()
            except ValueError:
                pass

        while cache and self._state != TERMINATE:
            try:
                ready, task = poll(0.2)
            except (IOError, EOFError), exc:
                debug('result handler got %r -- exiting' % (exc, ))
                return

            if ready:
                if task is None:
                    debug('result handler ignoring extra sentinel')
                    continue

                on_state_change(task)
            join_exited_workers()

            job, i, obj = task
            try:
                cache[job]._set(i, obj)
            except KeyError:
                pass

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
            maxtasksperchild=None, timeout=None, soft_timeout=None):
        self._setup_queues()
        self._taskqueue = Queue.Queue()
        self._cache = {}
        self._state = RUN
        self.timeout = timeout
        self.soft_timeout = soft_timeout
        self._maxtasksperchild = maxtasksperchild
        self._initializer = initializer
        self._initargs = initargs

        if self.soft_timeout and SIG_SOFT_TIMEOUT is None:
            raise NotImplementedError("Soft timeouts not supported: "
                    "Your platform does not have the SIGUSR1 signal.")

        if processes is None:
            try:
                processes = cpu_count()
            except NotImplementedError:
                processes = 1
        self._processes = processes

        if initializer is not None and not hasattr(initializer, '__call__'):
            raise TypeError('initializer must be a callable')

        self._pool = []
        for i in range(processes):
            self._create_worker_process()

        self._worker_handler = self.Supervisor(self)
        self._worker_handler.start()

        self._putlock = threading.BoundedSemaphore(self._processes)

        self._task_handler = self.TaskHandler(self._taskqueue,
                                              self._quick_put,
                                              self._outqueue,
                                              self._pool)
        self._task_handler.start()

        # Thread killing timedout jobs.
        if self.timeout or self.soft_timeout:
            self._timeout_handler = self.TimeoutHandler(
                    self._pool, self._cache,
                    self.soft_timeout, self.timeout)
            self._timeout_handler.start()
        else:
            self._timeout_handler = None

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
        w = self.Process(
            target=worker,
            args=(self._inqueue, self._outqueue,
                    self._initializer, self._initargs,
                    self._maxtasksperchild),
            )
        self._pool.append(w)
        w.name = w.name.replace('Process', 'PoolWorker')
        w.daemon = True
        w.start()
        return w

    def _join_exited_workers(self):
        """Cleanup after any worker processes which have exited due to
        reaching their specified lifetime. Returns True if any workers were
        cleaned up.
        """
        cleaned = []
        for i in reversed(range(len(self._pool))):
            worker = self._pool[i]
            if worker.exitcode is not None:
                # worker exited
                debug('cleaning up worker %d' % i)
                if self._putlock is not None:
                    try:
                        self._putlock.release()
                    except ValueError:
                        pass
                worker.join()
                cleaned.append(worker.pid)
                del self._pool[i]
        if cleaned:
            for job in self._cache.values():
                for worker_pid in job.worker_pids():
                    if worker_pid in cleaned:
                        err = WorkerLostError("Worker exited prematurely.")
                        job._set(None, (False, err))
                        continue
            return True
        return False

    def _repopulate_pool(self):
        """Bring the number of pool processes up to the specified number,
        for use after reaping workers which have exited.
        """
        debug('repopulating pool')
        for i in range(self._processes - len(self._pool)):
            if self._state != RUN:
                return
            self._create_worker_process()
            debug('added worker')

    def _maintain_pool(self):
        """"Clean up any exited workers and start replacements for them.
        """
        if self._join_exited_workers():
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

    def imap(self, func, iterable, chunksize=1):
        '''
        Equivalent of `itertools.imap()` -- can be MUCH slower
        than `Pool.map()`
        '''
        assert self._state == RUN
        if chunksize == 1:
            result = IMapIterator(self._cache)
            self._taskqueue.put((((result._job, i, func, (x,), {})
                         for i, x in enumerate(iterable)), result._set_length))
            return result
        else:
            assert chunksize > 1
            task_batches = Pool._get_tasks(func, iterable, chunksize)
            result = IMapIterator(self._cache)
            self._taskqueue.put((((result._job, i, mapstar, (x,), {})
                     for i, x in enumerate(task_batches)), result._set_length))
            return (item for chunk in result for item in chunk)

    def imap_unordered(self, func, iterable, chunksize=1):
        '''
        Like `imap()` method but ordering of results is arbitrary
        '''
        assert self._state == RUN
        if chunksize == 1:
            result = IMapUnorderedIterator(self._cache)
            self._taskqueue.put((((result._job, i, func, (x,), {})
                         for i, x in enumerate(iterable)), result._set_length))
            return result
        else:
            assert chunksize > 1
            task_batches = Pool._get_tasks(func, iterable, chunksize)
            result = IMapUnorderedIterator(self._cache)
            self._taskqueue.put((((result._job, i, mapstar, (x,), {})
                     for i, x in enumerate(task_batches)), result._set_length))
            return (item for chunk in result for item in chunk)

    def apply_async(self, func, args=(), kwds={},
            callback=None, accept_callback=None, timeout_callback=None,
            waitforslot=False, error_callback=None):
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
        result = ApplyResult(self._cache, callback,
                             accept_callback, timeout_callback,
                             error_callback)
        if waitforslot:
            self._putlock.acquire()
        self._taskqueue.put(([(result._job, None, func, args, kwds)], None))
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
        for i, p in enumerate(self._pool):
            debug('joining worker %s/%s (%r)' % (i, len(self._pool), p, ))
            p.join()

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

        assert result_handler.is_alive() or len(cache) == 0

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
DynamicPool = Pool

#
# Class whose instances are returned by `Pool.apply_async()`
#

class ApplyResult(object):

    def __init__(self, cache, callback, accept_callback=None,
            timeout_callback=None, error_callback=None):
        self._cond = threading.Condition(threading.Lock())
        self._job = job_counter.next()
        self._cache = cache
        self._ready = False
        self._callback = callback
        self._accept_callback = accept_callback
        self._errback = error_callback
        self._timeout_callback = timeout_callback

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
        self._success, self._value = obj
        if self._callback and self._success:
            self._callback(self._value)
        if self._errback and not self._success:
            self._errback(self._value)
        self._cond.acquire()
        try:
            self._ready = True
            self._cond.notify()
        finally:
            self._cond.release()
        if self._accepted:
            self._cache.pop(self._job, None)

    def _ack(self, i, time_accepted, pid):
        self._accepted = True
        self._time_accepted = time_accepted
        self._worker_pid = pid
        if self._accept_callback:
            self._accept_callback()
        if self._ready:
            self._cache.pop(self._job, None)

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
            self._number_left = length//chunksize + bool(length % chunksize)

    def _set(self, i, success_result):
        success, result = success_result
        if success:
            self._value[i*self._chunksize:(i+1)*self._chunksize] = result
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

    def __init__(self, cache):
        self._cond = threading.Condition(threading.Lock())
        self._job = job_counter.next()
        self._cache = cache
        self._items = collections.deque()
        self._index = 0
        self._length = None
        self._unsorted = {}
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
                    raise StopIteration
                self._cond.wait(timeout)
                try:
                    item = self._items.popleft()
                except IndexError:
                    if self._index == self._length:
                        raise StopIteration
                    raise TimeoutError
        finally:
            self._cond.release()

        success, value = item
        if success:
            return value
        raise value

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
                del self._cache[self._job]
        finally:
            self._cond.release()

    def _set_length(self, length):
        self._cond.acquire()
        try:
            self._length = length
            if self._index == self._length:
                self._cond.notify()
                del self._cache[self._job]
        finally:
            self._cond.release()

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
