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

from collections import deque, namedtuple
from pickle import HIGHEST_PROTOCOL
from time import sleep, time
from weakref import ref

from amqp.utils import promise
from billiard import forking_enable
from billiard import pool as _pool
from billiard.exceptions import WorkerLostError
from billiard.pool import (
    RUN, CLOSE, TERMINATE, ACK, NACK, EX_RECYCLE, WorkersJoined, CoroStop,
)
from billiard.queues import _SimpleQueue
from kombu.serialization import pickle as _pickle
from kombu.utils import fxrange
from kombu.utils.compat import get_errno
from kombu.utils.eventio import SELECT_BAD_FD

from celery import platforms
from celery import signals
from celery._state import set_default_app
from celery.concurrency.base import BasePool
from celery.five import Counter, items, values
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

Ack = namedtuple('Ack', ('id', 'fd', 'payload'))


def gen_not_started(gen):
    # gi_frame is None when generator stopped.
    return gen.gi_frame and gen.gi_frame.f_lasti == -1


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
    app.log.setup(int(os.environ.get('CELERY_LOG_LEVEL', 0) or 0),
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


def _select(readers=None, writers=None, err=None, timeout=0):
    readers = set() if readers is None else readers
    writers = set() if writers is None else writers
    err = set() if err is None else err
    try:
        r, w, e = select.select(readers, writers, err, timeout)
        if e:
            r = list(set(r) | set(e))
        return r, w, 0
    except (select.error, socket.error) as exc:
        if get_errno(exc) == errno.EINTR:
            return [], [], 1
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
        else:
            raise


class Worker(_pool.Worker):

    def on_loop_start(self, pid):
        self.outq.put((WORKER_UP, (pid, )))


class ResultHandler(_pool.ResultHandler):

    def __init__(self, *args, **kwargs):
        self.fileno_to_outq = kwargs.pop('fileno_to_outq')
        self.on_process_alive = kwargs.pop('on_process_alive')
        super(ResultHandler, self).__init__(*args, **kwargs)
        self.state_handlers[WORKER_UP] = self.on_process_alive

    def _process_result(self):
        fileno_to_outq = self.fileno_to_outq
        on_state_change = self.on_state_change

        while 1:
            fileno = (yield)
            try:
                proc = fileno_to_outq[fileno]
            except KeyError:
                continue
            reader = proc.outq._reader

            try:
                if reader.poll(0):
                    ready, task = True, reader.recv()
                else:
                    ready, task = False, None
            except (IOError, EOFError) as exc:
                debug('result handler got %r -- exiting', exc)
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

    def __init__(self, processes=None, synack=False, *args, **kwargs):
        processes = self.cpu_count() if processes is None else processes
        self.synack = synack
        self._queues = dict((self.create_process_queues(), None)
                            for _ in range(processes))
        self._fileno_to_inq = {}
        self._fileno_to_outq = {}
        self._fileno_to_synq = {}
        self._all_inqueues = set()
        super(AsynPool, self).__init__(processes, *args, **kwargs)

        for proc in self._pool:
            self._fileno_to_inq[proc.inqW_fd] = proc
            self._fileno_to_outq[proc.outqR_fd] = proc
            self._fileno_to_synq[proc.synqW_fd] = proc

    def _finalize_args(self):
        orig = super(AsynPool, self)._finalize_args()
        return (self._fileno_to_inq, orig)

    def get_process_queues(self):
        return next(q for q, owner in items(self._queues)
                    if owner is None)

    def on_grow(self, n):
        diff = max(self._processes - len(self._queues), 0)
        if diff:
            self._queues.update(
                dict((self.create_process_queues(), None) for _ in range(diff))
            )

    def on_shrink(self, n):
        pass

    def create_process_queues(self):
        inq, outq, synq = _SimpleQueue(), _SimpleQueue(), None
        inq._writer.setblocking(0)
        if self.synack:
            synq = _SimpleQueue()
            synq._writer.setblocking(0)
        return inq, outq, synq

    def on_process_alive(self, pid):
        try:
            proc = next(w for w in self._pool if w.pid == pid)
        except StopIteration:
            return
        self._fileno_to_inq[proc.inqW_fd] = proc
        self._fileno_to_synq[proc.synqW_fd] = proc
        self._all_inqueues.add(proc.inqW_fd)

    def on_job_process_down(self, job, pid_gone):
        if job._write_to:
            self.on_partial_read(job, job._write_to)
        elif job._scheduled_for:
            self._put_back(job)

    def on_job_process_lost(self, job, pid, exitcode):
        self.mark_as_worker_lost(job, exitcode)

    def _process_cleanup_queues(self, proc):
        try:
            self._queues[self._find_worker_queues(proc)] = None
        except (KeyError, ValueError):
            pass

    @staticmethod
    def _stop_task_handler(task_handler):
        for proc in task_handler.pool:
            proc.inq._writer.setblocking(1)
            try:
                proc.inq.put(None)
            except OSError as exc:
                if get_errno(exc) != errno.EBADF:
                    raise

    def create_result_handler(self):
        return super(AsynPool, self).create_result_handler(
            fileno_to_outq=self._fileno_to_outq,
            on_process_alive=self.on_process_alive,
        )

    def _process_register_queues(self, proc, queues):
        assert queues in self._queues
        b = len(self._queues)
        self._queues[queues] = proc
        assert b == len(self._queues)

    def _find_worker_queues(self, proc):
        try:
            return next(q for q, owner in items(self._queues)
                        if owner == proc)
        except StopIteration:
            raise ValueError(proc)

    def _setup_queues(self):
        self._inqueue = self._outqueue = \
            self._quick_put = self._quick_get = self._poll_result = None

    def process_flush_queues(self, proc):
        resq = proc.outq._reader
        on_state_change = self._result_handler.on_state_change
        while not resq.closed and resq.poll(0) and self._state != TERMINATE:
            try:
                task = resq.recv()
            except (IOError, EOFError) as exc:
                debug('got %r while flushing process %r',
                      exc, proc, exc_info=1)
                break
            else:
                if task is not None:
                    on_state_change(task)
                else:
                    debug('got sentinel while flushing process %r', proc)

    def on_partial_read(self, job, proc):
        # worker terminated by signal:
        # we cannot reuse the sockets again, because we don't know if
        # the process wrote/read anything frmo them, and if so we cannot
        # restore the message boundaries.
        if proc.exitcode != EX_RECYCLE:
            # job was not acked, so find another worker to send it to.
            if not job._accepted:
                self._put_back(job)
            writer = getattr(job, '_writer')
            writer = writer and writer() or None
            if writer:
                self._active_writers.discard(writer)

            # Replace queues to avoid reuse
            before = len(self._queues)
            try:
                queues = self._find_worker_queues(proc)
                if self.destroy_queues(queues):
                    self._queues[self.create_process_queues()] = None
            except ValueError:
                # Not in queue map, make sure sockets are closed.
                self.destroy_queues((proc.inq, proc.outq, proc.synq))
            assert len(self._queues) == before

    def destroy_queues(self, queues):
        removed = 1
        try:
            self._queues.pop(queues)
        except KeyError:
            removed = 0
        try:
            self.on_inqueue_close(queues[0]._writer.fileno())
        except IOError:
            pass
        for queue in queues:
            if queue:
                for sock in (queue._reader, queue._writer):
                    if not sock.closed:
                        try:
                            sock.close()
                        except (IOError, OSError):
                            pass
        return removed

    def _create_payload(self, type_, args,
                        dumps=_pickle.dumps, pack=struct.pack,
                        protocol=HIGHEST_PROTOCOL):
        body = dumps((type_, args), protocol=protocol)
        size = len(body)
        header = pack('>I', size)
        return header, body, size

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
        fileno_to_proc = {}
        inqR = set()
        for w in pool:
            try:
                fd = w.inq._reader.fileno()
                inqR.add(fd)
                fileno_to_proc[fd] = w
            except IOError:
                pass
        while inqR:
            readable, _, again = _select(inqR, timeout=0.5)
            if again:
                continue
            if not readable:
                break
            for fd in readable:
                fileno_to_proc[fd].inq._reader.recv()
            sleep(0)


class TaskPool(BasePool):
    """Multiprocessing Pool implementation."""
    Pool = AsynPool
    BlockingPool = _pool.Pool

    uses_semaphore = True
    write_stats = None

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
                              synack=False,
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
        return {
            'max-concurrency': self.limit,
            'processes': [p.pid for p in self._pool._pool],
            'max-tasks-per-child': self._pool._maxtasksperchild or 'N/A',
            'put-guarded-by-semaphore': self.putlocks,
            'timeouts': (self._pool.soft_timeout or 0,
                         self._pool.timeout or 0),
            'writes': self.human_write_stats(),
        }

    def human_write_stats(self):
        if self.write_stats is None:
            return 'N/A'
        vals = list(values(self.write_stats))
        total = sum(vals)

        def per(v, total):
            return '{0:.2f}%'.format((float(v) / total) * 100.0 if v else 0)

        return {
            'total': total,
            'avg': per(total / len(self.write_stats) if total else 0, total),
            'all': ', '.join(per(v, total) for v in vals)
        }

    def _create_timelimit_handlers(self, hub, now=time):
        apply_after = hub.timer.apply_after
        apply_at = hub.timer.apply_at
        on_soft_timeout = self.on_soft_timeout
        on_hard_timeout = self.on_hard_timeout

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

    def _create_process_handlers(self, hub, READ=READ, ERR=ERR):
        pool = self._pool
        hub_add, hub_remove = hub.add, hub.remove
        all_inqueues = self._pool._all_inqueues
        fileno_to_inq = self._pool._fileno_to_inq
        fileno_to_outq = self._pool._fileno_to_outq
        fileno_to_synq = self._pool._fileno_to_synq
        maintain_pool = self._pool.maintain_pool
        handle_result_event = self._pool.handle_result_event
        process_flush_queues = self._pool.process_flush_queues

        def on_process_up(proc):
            # If we got the same fd as a previous process then we will also
            # receive jobs in the old buffer, so we need to reset the
            # _write_to and _scheduled_for tracking values used to recover
            # message boundaries when processes exit.
            infd = proc.inqW_fd
            for job in values(pool._cache):
                if job._write_to and job._write_to.inqW_fd == infd:
                    job._write_to = proc
                if job._scheduled_for and job._scheduled_for.inqW_fd == infd:
                    job._scheduled_for = proc
            fileno_to_outq[proc.outqR_fd] = proc
            hub_add(proc.sentinel, maintain_pool, READ | ERR)
            hub_add(proc.outqR_fd, handle_result_event, READ | ERR)
        self._pool.on_process_up = on_process_up

        def on_process_down(proc):
            process_flush_queues(proc)
            fileno_to_outq.pop(proc.outqR_fd, None)
            fileno_to_inq.pop(proc.inqW_fd, None)
            fileno_to_synq.pop(proc.synqW_fd, None)
            all_inqueues.discard(proc.inqW_fd)
            hub_remove(proc.sentinel)
            hub_remove(proc.outqR_fd)
        self._pool.on_process_down = on_process_down

    def _create_write_handlers(self, hub,
                               pack=struct.pack, dumps=_pickle.dumps,
                               protocol=HIGHEST_PROTOCOL):
        pool = self._pool
        fileno_to_inq = pool._fileno_to_inq
        fileno_to_synq = pool._fileno_to_synq
        outbound = self.outbound_buffer
        pop_message = outbound.popleft
        put_message = outbound.append
        all_inqueues = pool._all_inqueues
        active_writes = self._active_writes
        diff = all_inqueues.difference
        hub_add, hub_remove = hub.add, hub.remove
        mark_write_fd_as_active = active_writes.add
        mark_write_gen_as_active = self._active_writers.add
        write_generator_done = self._active_writers.discard
        get_job = pool._cache.__getitem__
        pool._put_back = put_message
        precalc = {ACK: pool._create_payload(ACK, (0, )),
                   NACK: pool._create_payload(NACK, (0, ))}

        def on_poll_start(hub):
            # called for every eventloop iteration, and if there
            # are messages pending this will schedule writing one message
            # by registering the 'schedule_writes' function for all currently
            # inactive inqueues (not already being written to)
            if outbound:
                hub_add(diff(active_writes), schedule_writes, WRITE | ERR)
        self.on_poll_start = on_poll_start

        def on_inqueue_close(fd):
            # Makes sure the fd is removed from tracking when
            # the connection is closed, this is essential as
            # fds may be reused.
            active_writes.discard(fd)
            all_inqueues.discard(fd)
        self._pool.on_inqueue_close = on_inqueue_close

        def schedule_writes(ready_fd, events):
            # Schedule write operation to ready file descriptor.
            # The file descriptor is writeable, but that does not
            # mean the process is currently reading from the socket.
            # The socket is buffered so writeable simply means that
            # the buffer can accept at least 1 byte of data.
            if ready_fd in active_writes:
                # already writing to this fd
                return
            try:
                job = pop_message()
            except IndexError:
                # no more messages, remove all inactive fds from the hub.
                # this is important since the fds are always writeable
                # as long as there's 1 byte left in the buffer, and so
                # this may create a spinloop where the eventloop always wakes
                # up.
                for inqfd in diff(active_writes):
                    hub_remove(inqfd)
            else:
                if not job._accepted:  # job not accepted by another worker
                    try:
                        # keep track of what process the write operation
                        # was scheduled for.
                        proc = job._scheduled_for = fileno_to_inq[ready_fd]
                    except KeyError:
                        # write was scheduled for this fd but the process
                        # has since exited and the message must be sent to
                        # another process.
                        return put_message(job)
                    cor = _write_job(proc, ready_fd, job)
                    job._writer = ref(cor)
                    mark_write_gen_as_active(cor)
                    mark_write_fd_as_active(ready_fd)

                    # Try to write immediately, in case there's an error.
                    try:
                        next(cor)
                        hub_add((ready_fd, ), cor, WRITE | ERR)
                    except StopIteration:
                        pass

        def send_job(tup):
            # Schedule writing job request for when one of the process
            # inqueues are writable.
            body = dumps(tup, protocol=protocol)
            body_size = len(body)
            header = pack('>I', body_size)
            # index 1,0 is the job ID.
            job = get_job(tup[1][0])
            job._payload = header, body, body_size
            put_message(job)
        self._pool._quick_put = send_job

        write_stats = self.write_stats = Counter()

        def on_not_recovering(proc):
            # XXX Theoretically a possibility, but maybe terminate the
            # process in this case to attempt to recover that way.
            raise Exception(
                'Process writable but cannot write. Contact support!')

        def _write_job(proc, fd, job):
            # writes job to the worker process.
            # Operation must complete if more than one byte of data
            # was written.  If the broker connection is lost
            # and no data was written the operation shall be cancelled.
            header, body, body_size = job._payload
            errors = 0
            try:
                # job result keeps track of what process the job is sent to.
                job._write_to = proc
                send = proc.send_job_offset

                Hw = Bw = 0
                while Hw < 4:
                    try:
                        Hw += send(header, Hw)
                    except Exception as exc:
                        if get_errno(exc) not in UNAVAIL:
                            raise
                        # suspend until more data
                        errors += 1
                        if errors > 100:
                            on_not_recovering(proc)
                            raise StopIteration()
                        yield
                    errors = 0
                while Bw < body_size:
                    try:
                        Bw += send(body, Bw)
                    except Exception as exc:
                        if get_errno(exc) not in UNAVAIL:
                            raise
                        # suspend until more data
                        errors += 1
                        if errors > 100:
                            on_not_recovering(proc)
                            raise StopIteration()
                        yield
                    errors = 0
            finally:
                write_stats[proc.index] += 1
                # message written, so this fd is now available
                active_writes.discard(fd)
                write_generator_done(job._writer())  # is a weakref

        def send_ack(response, pid, job, fd, WRITE=WRITE, ERR=ERR):
            # Schedule writing ack response for when the fd is writeable.
            msg = Ack(job, fd, precalc[response])
            callback = promise(write_generator_done)
            cor = _write_ack(fd, msg, callback=callback)
            mark_write_gen_as_active(cor)
            mark_write_fd_as_active(fd)
            callback.args = (cor, )
            hub_add((fd, ), cor, WRITE | ERR)
        self._pool.send_ack = send_ack

        def _write_ack(fd, ack, callback=None):
            # writes ack back to the worker if synack enabled.
            # this operation *MUST* complete, otherwise
            # the worker process will hang waiting for the ack.
            header, body, body_size = ack[2]
            try:
                try:
                    proc = fileno_to_synq[fd]
                except KeyError:
                    # process died, we can safely discard the ack at this
                    # point.
                    raise StopIteration()
                send = proc.send_syn_offset

                Hw = Bw = 0
                while Hw < 4:
                    try:
                        Hw += send(header, Hw)
                    except Exception as exc:
                        if get_errno(exc) not in UNAVAIL:
                            raise
                        yield
                while Bw < body_size:
                    try:
                        Bw += send(body, Bw)
                    except Exception as exc:
                        if get_errno(exc) not in UNAVAIL:
                            raise
                        # suspend until more data
                        yield
            finally:
                if callback:
                    callback()
                # message written, so this fd is now available
                active_writes.discard(fd)

    def on_poll_init(self, w, hub):
        pool = self._pool
        pool._active_writers = self._active_writers

        self._create_timelimit_handlers(hub)
        self._create_process_handlers(hub)
        self._create_write_handlers(hub)

        # did_start_ok will verify that pool processes were able to start,
        # but this will only work the first time we start, as
        # maxtasksperchild will mess up metrics.
        if not w.consumer.restart_count and not pool.did_start_ok():
            raise WorkerLostError('Could not start worker processes')

        # Maintain_pool is called whenever a process exits.
        hub.add(pool.process_sentinels, self.maintain_pool, READ | ERR)
        # Handle_result_event is called whenever one of the
        # result queues are readable.
        hub.add(pool._fileno_to_outq, self.handle_result_event, READ | ERR)

        # Timers include calling maintain_pool at a regular interval
        # to be certain processes are restarted.
        for handler, interval in items(self.timers):
            hub.timer.apply_interval(interval * 1000.0, handler)

    def flush(self):
        if self._pool._state == TERMINATE:
            return
        # cancel all tasks that have not been accepted so that NACK is sent.
        for job in values(self._pool._cache):
            if not job._accepted:
                job._cancel()

        # clear the outgoing buffer as the tasks will be redelivered by
        # the broker anyway.
        if self.outbound_buffer:
            self.outbound_buffer.clear()
        try:
            # ...but we must continue writing the payloads we already started
            # to keep message boundaries.
            # The messages may be NACK'ed later if synack is enabled.
            if self._pool._state == RUN:
                # flush outgoing buffers
                intervals = fxrange(0.01, 0.1, 0.01, repeatlast=True)
                while self._active_writers:
                    writers = list(self._active_writers)
                    for gen in writers:
                        if (gen.__name__ == '_write_job' and
                                gen_not_started(gen)):
                            # has not started writing the job so can
                            # discard the task, but we must also remove
                            # it from the Pool._cache.
                            job_to_discard = None
                            for job in values(self._pool._cache):
                                if job._writer() is gen:  # _writer is saferef
                                    # removes from Pool._cache
                                    job_to_discard = job
                                    break
                            if job_to_discard:
                                job_to_discard.discard()
                            self._active_writers.discard(gen)
                        else:
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
