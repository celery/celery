# -*- coding: utf-8 -*-
"""
    celery.concurrency.processes
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    Pool implementation using :mod:`multiprocessing`.

    We use the billiard fork of multiprocessing which contains
    numerous improvements.

"""
from __future__ import absolute_import

import errno
import os
import select
import socket
import struct

from collections import deque
from pickle import HIGHEST_PROTOCOL
from time import sleep, time

from billiard import forking_enable
from billiard import pool as _pool
from billiard.exceptions import WorkerLostError
from billiard.pool import RUN, CLOSE, TERMINATE, WorkersJoined, CoroStop
from billiard.queues import _SimpleQueue
from kombu.serialization import pickle as _pickle
from kombu.utils import fxrange
from kombu.utils.compat import get_errno
from kombu.utils.eventio import SELECT_BAD_FD

from celery import platforms
from celery import signals
from celery._state import set_default_app
from celery.concurrency.base import BasePool
from celery.five import items
from celery.task import trace
from celery.utils.log import get_logger
from celery.worker.hub import READ, WRITE, ERR

#: List of signals to reset when a child process starts.
WORKER_SIGRESET = frozenset(['SIGTERM',
                             'SIGHUP',
                             'SIGTTIN',
                             'SIGTTOU',
                             'SIGUSR1'])

#: List of signals to ignore when a child process starts.
WORKER_SIGIGNORE = frozenset(['SIGINT'])

UNAVAIL = frozenset([errno.EAGAIN, errno.EINTR, errno.EBADF])

MAXTASKS_NO_BILLIARD = """\
    maxtasksperchild enabled but billiard C extension not installed!
    This may lead to a deadlock, please install the billiard C extension.
"""

#: Constant sent by child process when started (ready to accept work)
WORKER_UP = 15

logger = get_logger(__name__)
warning, debug = logger.warning, logger.debug


def process_initializer(app, hostname):
    """Pool child process initializer."""
    platforms.signals.reset(*WORKER_SIGRESET)
    platforms.signals.ignore(*WORKER_SIGIGNORE)
    platforms.set_mp_process_title('celeryd', hostname=hostname)
    # This is for Windows and other platforms not supporting
    # fork(). Note that init_worker makes sure it's only
    # run once per process.
    app.loader.init_worker()
    app.loader.init_worker_process()
    app.log.setup(int(os.environ.get('CELERY_LOG_LEVEL', 0)),
                  os.environ.get('CELERY_LOG_FILE') or None,
                  bool(os.environ.get('CELERY_LOG_REDIRECT', False)),
                  str(os.environ.get('CELERY_LOG_REDIRECT_LEVEL')))
    if os.environ.get('FORKED_BY_MULTIPROCESSING'):
        # pool did execv after fork
        trace.setup_worker_optimizations(app)
    else:
        app.set_current()
        set_default_app(app)
        app.finalize()
        trace._tasks = app._tasks  # enables fast_trace_task optimization.
    from celery.task.trace import build_tracer
    for name, task in items(app.tasks):
        task.__trace__ = build_tracer(name, task, app.loader, hostname)
    signals.worker_process_init.send(sender=None)


def _select(self, readers=None, writers=None, err=None, timeout=0):
    readers = set() if readers is None else readers
    writers = set() if writers is None else writers
    err = set() if err is None else err
    try:
        r, w, e = select.select(readers, writers, err, timeout)
        if e:
            _seen = set()
            r = [f for f in r + e if f not in _seen and not _seen.add(f)]
        return r, w, 0
    except (select.error, socket.error) as exc:
        if get_errno(exc) == errno.EINTR:
            return
        elif get_errno(exc) in SELECT_BAD_FD:
            for fd in readers | writers | err:
                try:
                    select.select([fd], [], [], 0)
                except (select.error, socket.error) as exc:
                    if get_errno(exc) not in SELECT_BAD_FD:
                        raise
                    readers.discard(fd)
                    writers.discard(fd)
                    err.discard(fd)
            return [], [], 1


class promise(object):

    def __init__(self, fun, *partial_args, **partial_kwargs):
        self.fun = fun
        self.args = partial_args
        self.kwargs = partial_kwargs
        self.ready = False

    def __call__(self, *args, **kwargs):
        try:
            return self.fun(*tuple(self.args) + tuple(args),
                            **dict(self.kwargs, **kwargs))
        finally:
            self.ready = True


class Worker(_pool.Worker):

    def on_loop_start(self, pid):
        self.outq.put((WORKER_UP, (pid, )))


class ResultHandler(_pool.ResultHandler):

    def __init__(self, *args, **kwargs):
        self.fileno_to_outq = kwargs.pop('fileno_to_outq')
        self.on_worker_alive = kwargs.pop('on_worker_alive')
        super(ResultHandler, self).__init__(*args, **kwargs)
        self.state_handlers[WORKER_UP] = self.on_worker_alive

    def _process_result(self):
        fileno_to_outq = self.fileno_to_outq
        on_state_change = self.on_state_change

        while 1:
            fileno = (yield)
            proc = fileno_to_outq[fileno]
            reader = proc.outq._reader

            try:
                if reader.poll(0):
                    ready, task = True, reader.recv()
                else:
                    ready, task = False, None
            except (IOError, EOFError) as exc:
                debug('result handler got %r -- exiting' % (exc, ))
                raise CoroStop()

            if self._state:
                assert self._state == TERMINATE
                debug('result handler found thread._state==TERMINATE')
                raise CoroStop()

            if ready:
                if task is None:
                    debug('result handler got sentinel -- exiting')
                    raise CoroStop()
                on_state_change(task)

    def handle_event(self, fileno=None, event=None):
        if self._state == RUN:
            it = self._it
            if it is None:
                it = self._it = self._process_result()
                next(it)
            try:
                it.send(fileno)
            except (StopIteration, CoroStop):
                self._it = None

    def on_stop_not_started(self):
        cache = self.cache
        check_timeouts = self.check_timeouts
        fileno_to_outq = self.fileno_to_outq
        on_state_change = self.on_state_change
        join_exited_workers = self.join_exited_workers

        outqueues = set(fileno_to_outq)
        while cache and outqueues and self._state != TERMINATE:
            if check_timeouts is not None:
                check_timeouts()
            _dirty = set()
            for fd in outqueues:
                try:
                    proc = fileno_to_outq[fd]
                except KeyError:
                    outqueues.discard(fd)
                    break

                reader = proc.outq._reader
                try:
                    if reader.poll(0):
                        task = reader.recv()
                    else:
                        task = None
                        sleep(0.5)
                except (IOError, EOFError):
                    outqueues.discard(fd)
                    break
                else:
                    if task:
                        on_state_change(task)
                try:
                    join_exited_workers(shutdown=True)
                except WorkersJoined:
                    debug('result handler: all workers terminated')
                    return


class AsynPool(_pool.Pool):
    ResultHandler = ResultHandler
    Worker = Worker

    def __init__(self, processes=None, *args, **kwargs):
        processes = self.cpu_count() if processes is None else processes
        self._queuepairs = dict((self.create_process_queuepair(), None)
                                for _ in range(processes))
        self._fileno_to_inq = {}
        self._fileno_to_outq = {}
        self._all_inqueues = set()
        super(AsynPool, self).__init__(processes, *args, **kwargs)

        for proc in self._pool:
            self._fileno_to_inq[proc.inqW_fd] = proc
            self._fileno_to_outq[proc.outqR_fd] = proc

    def _finalize_args(self):
        orig = super(AsynPool, self)._finalize_args()
        return (self._fileno_to_inq, orig)

    def get_process_queuepair(self):
        return next(pair for pair, owner in items(self._queuepairs)
                    if owner is None)

    def create_process_queuepair(self):
        inq, outq = _SimpleQueue(), _SimpleQueue()
        inq._writer.setblocking(0)
        return inq, outq

    def on_worker_alive(self, pid):
        try:
            proc = next(w for w in self._pool if w.pid == pid)
        except StopIteration:
            return
        self._fileno_to_inq[proc.inqW_fd] = proc
        self._all_inqueues.add(proc.inqW_fd)

    def on_job_process_down(self, job):
        if not job._accepted and job._write_to:
            self.on_partial_read(job, job._write_to)

    def on_job_process_lost(self, job, pid, exitcode):
        self.mark_as_worker_lost(job, exitcode)

    def _process_cleanup_queuepair(self, proc):
        try:
            self._queuepairs[self._find_worker_queuepair(proc)] = None
        except (KeyError, ValueError):
            pass

    @staticmethod
    def _stop_task_handler(task_handler):
        for proc in task_handler.pool:
            proc.inq._writer.setblocking(1)
            proc.inq.put(None)

    def create_result_handler(self):
        return super(AsynPool, self).create_result_handler(
            fileno_to_outq=self._fileno_to_outq,
            on_worker_alive=self.on_worker_alive,
        )

    def _process_register_queuepair(self, proc, pair):
        self._queuepairs[pair] = proc

    def _find_worker_queuepair(self, proc):
        for pair, owner in items(self._queuepairs):
            if owner == proc:
                return pair
        raise ValueError(proc)

    def _setup_queues(self):
        self._inqueue = self._outqueue = \
            self._quick_put = self._quick_get = self._poll_result = None

    def on_partial_read(self, job, proc):
        resq = proc.outq._reader
        # empty result queue buffer
        while resq.poll(0):
            self.handle_result_event(resq.fileno())

        # worker terminated by signal:
        # we cannot reuse the sockets again, because we don't know if
        # the process wrote/read anything frmo them, and if so we cannot
        # restore the message boundaries.
        if proc.exitcode < 0:
            # job was not acked, so find another worker to send it to.
            if not job._accepted:
                self._put_back(job)

            for conn in (proc.inq, proc.outq):
                for sock in (conn._reader, conn._writer):
                    if not sock.closed:
                        os.close(sock.fileno())
            self._queuepairs[(proc.inq, proc.outq)] = \
                self._queuepairs[self.create_process_queuepair()] = None

    @classmethod
    def _set_result_sentinel(cls, _outqueue, _pool):
        pass

    def _help_stuff_finish_args(self):
        return (self._pool, )

    @classmethod
    def _help_stuff_finish(cls, pool):
        # task_handler may be blocked trying to put items on inqueue
        debug(
            'removing tasks from inqueue until task handler finished',
        )
        fileno_to_proc = dict((w.inq._reader.fileno(), w) for w in pool)
        inqR = set(fileno_to_proc)
        while inqR:
            readable, _, again = _select(inqR, timeout=0.5)
            if again:
                continue
            if not readable:
                break
            for fd in readable:
                fileno_to_proc[fd]._reader.recv()
            sleep(0)


class TaskPool(BasePool):
    """Multiprocessing Pool implementation."""
    Pool = AsynPool
    BlockingPool = _pool.Pool

    uses_semaphore = True

    def on_start(self):
        """Run the task pool.

        Will pre-fork all workers so they're ready to accept tasks.

        """
        if self.options.get('maxtasksperchild'):
            try:
                import _billiard  # noqa
                _billiard.Connection.send_offset
            except (ImportError, AttributeError):
                # billiard C extension not installed
                warning(MAXTASKS_NO_BILLIARD)

        forking_enable(self.forking_enable)
        Pool = (self.BlockingPool if self.options.get('threads', True)
                else self.Pool)
        P = self._pool = Pool(processes=self.limit,
                              initializer=process_initializer,
                              **self.options)
        self.on_apply = P.apply_async
        self.on_soft_timeout = P._timeout_handler.on_soft_timeout
        self.on_hard_timeout = P._timeout_handler.on_hard_timeout
        self.maintain_pool = P.maintain_pool
        self.terminate_job = self._pool.terminate_job
        self.grow = self._pool.grow
        self.shrink = self._pool.shrink
        self.restart = self._pool.restart
        self.maybe_handle_result = P._result_handler.handle_event
        self.outbound_buffer = deque()
        self.handle_result_event = P.handle_result_event
        self._active_writes = set()
        self._active_writers = set()

    def did_start_ok(self):
        return self._pool.did_start_ok()

    def on_stop(self):
        """Gracefully stop the pool."""
        if self._pool is not None and self._pool._state in (RUN, CLOSE):
            self._pool.close()
            self._pool.join()
            self._pool = None

    def on_terminate(self):
        """Force terminate the pool."""
        if self._pool is not None:
            self._pool.terminate()
            self._pool = None

    def on_close(self):
        if self._pool is not None and self._pool._state == RUN:
            self._pool.close()

    def _get_info(self):
        return {'max-concurrency': self.limit,
                'processes': [p.pid for p in self._pool._pool],
                'max-tasks-per-child': self._pool._maxtasksperchild,
                'put-guarded-by-semaphore': self.putlocks,
                'timeouts': (self._pool.soft_timeout, self._pool.timeout)}

    def on_poll_init(self, w, hub,
                     now=time, protocol=HIGHEST_PROTOCOL, pack=struct.pack,
                     dumps=_pickle.dumps):
        pool = self._pool
        apply_after = hub.timer.apply_after
        apply_at = hub.timer.apply_at
        maintain_pool = self.maintain_pool
        on_soft_timeout = self.on_soft_timeout
        on_hard_timeout = self.on_hard_timeout
        outbound = self.outbound_buffer
        pop_message = outbound.popleft
        put_message = outbound.append
        fileno_to_inq = pool._fileno_to_inq
        fileno_to_outq = pool._fileno_to_outq
        all_inqueues = pool._all_inqueues
        active_writes = self._active_writes
        diff = all_inqueues.difference
        hub_add, hub_remove = hub.add, hub.remove
        mark_write_fd_as_active = active_writes.add
        mark_write_gen_as_active = self._active_writers.add
        write_generator_gone = self._active_writers.discard
        get_job = pool._cache.__getitem__
        pool._put_back = put_message

        # did_start_ok will verify that pool processes were able to start,
        # but this will only work the first time we start, as
        # maxtasksperchild will mess up metrics.
        if not w.consumer.restart_count and not pool.did_start_ok():
            raise WorkerLostError('Could not start worker processes')

        hub_add(pool.process_sentinels, self.maintain_pool, READ | ERR)
        hub_add(fileno_to_outq, self.handle_result_event, READ | ERR)
        for handler, interval in items(self.timers):
            hub.timer.apply_interval(interval * 1000.0, handler)

        # need to handle pool results before every task
        # since multiple tasks can be received in a single poll()
        # XXX do we need this now?!?
        # hub.on_task.append(pool.maybe_handle_result)

        def on_timeout_set(R, soft, hard):

            def _on_soft_timeout():
                if hard:
                    R._tref = apply_at(now() + (hard - soft),
                                       on_hard_timeout, (R, ))
                on_soft_timeout(R)
            if soft:
                R._tref = apply_after(soft * 1000.0, _on_soft_timeout)
            elif hard:
                R._tref = apply_after(hard * 1000.0,
                                      on_hard_timeout, (R, ))
        self._pool.on_timeout_set = on_timeout_set

        def on_timeout_cancel(result):
            try:
                result._tref.cancel()
                delattr(result, '_tref')
            except AttributeError:
                pass
        self._pool.on_timeout_cancel = on_timeout_cancel

        def on_process_up(proc):
            fileno_to_outq[proc.outqR_fd] = proc
            hub_add(proc.sentinel, maintain_pool, READ | ERR)
            hub_add(proc.outqR_fd, pool.handle_result_event, READ | ERR)
        self._pool.on_process_up = on_process_up

        def on_process_down(proc):
            fileno_to_outq.pop(proc.outqR_fd, None)
            fileno_to_inq.pop(proc.inqW_fd, None)
            all_inqueues.discard(proc.inqW_fd)
            hub_remove(proc.sentinel)
            hub_remove(proc.outqR_fd)
        self._pool.on_process_down = on_process_down

        def _write_to(fd, job, callback=None):
            header, body, body_size = job._payload
            try:
                try:
                    proc = fileno_to_inq[fd]
                except KeyError:
                    put_message(job)
                    raise StopIteration()
                send_offset = proc.inq._writer.send_offset
                # job result keeps track of what process the job is sent to.
                job._write_to = proc

                Hw = Bw = 0
                while Hw < 4:
                    try:
                        Hw += send_offset(header, Hw)
                    except Exception as exc:
                        if get_errno(exc) not in UNAVAIL:
                            raise
                        # suspend until more data
                        yield
                while Bw < body_size:
                    try:
                        Bw += send_offset(body, Bw)
                    except Exception as exc:
                        if get_errno(exc) not in UNAVAIL:
                            raise
                        # suspend until more data
                        yield
            finally:
                if callback:
                    callback()
                active_writes.discard(fd)

        def schedule_writes(ready_fd, events):
            try:
                job = pop_message()
            except IndexError:
                for inqfd in diff(active_writes):
                    hub_remove(inqfd)
            else:
                if not job._accepted:
                    callback = promise(write_generator_gone)
                    cor = _write_to(ready_fd, job, callback=callback)
                    mark_write_gen_as_active(cor)
                    mark_write_fd_as_active(ready_fd)
                    callback.args = (cor, )  # tricky as we need to pass ref
                    hub_add((ready_fd, ), cor, WRITE)

        def on_poll_start(hub):
            if outbound:
                hub_add(diff(active_writes), schedule_writes, hub.WRITE)
        self.on_poll_start = on_poll_start

        def quick_put(tup):
            body = dumps(tup, protocol=protocol)
            body_size = len(body)
            header = pack('>I', body_size)
            # index 0 is the job ID.
            job = get_job(tup[0])
            job._payload = header, buffer(body), body_size
            put_message(job)
        self._pool._quick_put = quick_put

    def handle_timeouts(self):
        if self._pool._timeout_handler:
            self._pool._timeout_handler.handle_event()

    def flush(self):
        if self.outbound_buffer:
            self.outbound_buffer.clear()
        try:
            if self._pool._state == RUN:
                # flush outgoing buffers
                intervals = fxrange(0.01, 0.1, 0.01, repeatlast=True)
                while self._active_writers:
                    writers = list(self._active_writers)
                    for gen in writers:
                        if gen.gi_frame.f_lasti != -1:  # generator started?
                            try:
                                next(gen)
                            except StopIteration:
                                self._active_writers.discard(gen)
                    # workers may have exited in the meantime.
                    self.maintain_pool()
                    sleep(next(intervals))  # don't busyloop
        finally:
            self.outbound_buffer.clear()
            self._active_writers.clear()

    @property
    def num_processes(self):
        return self._pool._processes

    @property
    def timers(self):
        return {self.maintain_pool: 5.0}
