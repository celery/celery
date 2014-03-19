import errno
import logging
import os
import signal
import sys
import threading
import time
from itertools import count
from logging import getLogger

import signalfd
from billiard import util
from billiard.exceptions import WorkerLostError
from billiard.process import _maybe_flush
from billiard.process import _set_current_process
from celery.concurrency.base import BasePool
from celery.concurrency.prefork import _set_task_join_will_block
from celery.concurrency.prefork import platforms, process_destructor
from celery.concurrency.prefork import WORKER_SIGIGNORE
from celery.concurrency.prefork import WORKER_SIGRESET
from celery.five import monotonic

from ctypes import c_char
from ctypes import c_int32
from ctypes import c_uint32
from ctypes import c_uint64
from ctypes import Structure

logger = getLogger(__name__)

class signalfd_siginfo(Structure):
    _fields_ = (
        ('ssi_signo', c_uint32),    # Signal number
        ('ssi_errno', c_int32),     # Error number (unused)
        ('ssi_code', c_int32),      # Signal code
        ('ssi_pid', c_uint32),      # PID of sender
        ('ssi_uid', c_uint32),      # Real UID of sender
        ('ssi_fd', c_int32),        # File descriptor (SIGIO)
        ('ssi_tid', c_uint32),      # Kernel timer ID (POSIX timers)
        ('ssi_band', c_uint32),     # Band event (SIGIO)
        ('ssi_overrun', c_uint32),  # POSIX timer overrun count
        ('ssi_trapno', c_uint32),   # Trap number that caused signal
        ('ssi_status', c_int32),    # Exit status or signal (SIGCHLD)
        ('ssi_int', c_int32),       # Integer sent by sigqueue(2)
        ('ssi_ptr', c_uint64),      # Pointer sent by sigqueue(2)
        ('ssi_utime', c_uint64),    # User CPU time consumed (SIGCHLD)
        ('ssi_stime', c_uint64),    # System CPU time consumed (SIGCHLD)
        ('ssi_addr', c_uint64),     # Address that generated signal
                                    # (for hardware-generated signals)
        ('_padding', c_char * 46),  # Pad size to 128 bytes (allow for
                                    # additional fields in the future)
    )

class ExcInfo(object):
    internal = False
    tb = traceback = None

    def __init__(self, exc):
        self.type = type(exc)
        self.exception = exc

    @property
    def exc_info(self):
        return self.type, self.exception, None


class Workhorse(object):
    _counter = count(1)
    _children = ()

    def __init__(self, target, args, kwargs):
        self._name = self.name = 'Workhorse-%s' % Workhorse._counter.next()
        sys.stdout.flush()
        sys.stderr.flush()
        self.pid = os.fork()
        if self.pid == 0:
            try:
                platforms.signals.reset(*WORKER_SIGRESET)
                platforms.signals.ignore(*WORKER_SIGIGNORE)
                _set_task_join_will_block(True)

                if 'random' in sys.modules:
                    import random
                    random.seed()
                if sys.stdin is not None:
                    try:
                        sys.stdin.close()
                        sys.stdin = open(os.devnull)
                    except (OSError, ValueError):
                        pass
                _set_current_process(self)

                # Re-init logging system.
                # Workaround for http://bugs.python.org/issue6721/#msg140215
                # Python logging module uses RLock() objects which are broken
                # after fork. This can result in a deadlock (Celery Issue #496).
                loggerDict = logging.Logger.manager.loggerDict
                logger_names = list(loggerDict.keys())
                logger_names.append(None)  # for root logger
                for name in logger_names:
                    if not name or not isinstance(loggerDict[name],
                                                  logging.PlaceHolder):
                        for handler in logging.getLogger(name).handlers:
                            handler.createLock()
                logging._lock = threading.RLock()

                util._finalizer_registry.clear()
                util._run_after_forkers()
                try:
                    target(*args, **kwargs)
                    exitcode = 0
                finally:
                    util._exit_function()
            except SystemExit as exc:
                if not exc.args:
                    exitcode = 1
                elif isinstance(exc.args[0], int):
                    exitcode = exc.args[0]
                else:
                    sys.stderr.write(str(exc.args[0]) + '\n')
                    exitcode = 0 if isinstance(exc.args[0], str) else 1
            except:
                exitcode = 1
                if not util.error('Process %s', self.name, exc_info=True):
                    import traceback
                    sys.stderr.write('Process %s:\n' % self.name)
                    traceback.print_exc()
            finally:
                util.info('Process %s exiting with exitcode %d', self.pid, exitcode)
                _maybe_flush(sys.stdout)
                _maybe_flush(sys.stderr)
                _maybe_flush(sys.__stdout__)
                _maybe_flush(sys.__stderr__)
                os._exit(exitcode)

class TaskPool(BasePool):
    sigfd = None
    sigfh = None
    workers = ()
    uses_semaphore = True
    sem = None

    def on_start(self):
        self.sem = self.options['semaphore']
        self.workers = {}
        self.sigfd = signalfd.signalfd(0, [signal.SIGCHLD], signalfd.SFD_NONBLOCK | signalfd.SFD_CLOEXEC)
        self.sigfh = os.fdopen(self.sigfd, 'rb')
        signalfd.sigprocmask(signalfd.SIG_BLOCK, [signal.SIGCHLD])

    def register_with_event_loop(self, hub):
        hub.add_reader(self.sigfd, self.on_sigchld)

    def on_sigchld(self):
        pending = {}

        si = signalfd_siginfo()
        while True:
            try:
                self.sigfh.readinto(si)
            except IOError as exc:
                if exc.errno != errno.EAGAIN:
                    raise
                break
            else:
                assert si.ssi_signo == signal.SIGCHLD
                pending[si.ssi_pid] = si.ssi_status
        while True:
            try:
                pid, exit_code = os.waitpid(0, os.WNOHANG)
            except OSError as exc:
                if exc.errno != 10:
                    raise
                break
            else:
                if not pid:
                    break
                if pid not in pending:
                    if os.WIFEXITED(exit_code):
                        pending[pid] = os.WEXITSTATUS(exit_code)
                    elif os.WIFSIGNALED(exit_code):
                        pending[pid] = os.WTERMSIG(exit_code)
                    elif os.WIFSTOPPED(exit_code):
                        pending[pid] = os.WSTOPSIG(exit_code)

        for pid, exit_code in pending.iteritems():
            self.on_worker_exit(pid, exit_code)

    def on_worker_exit(self, pid, exit_code):
        if pid in self.workers:
            options = self.workers.pop(pid)
            if exit_code == 0:
                options['callback'](None)
            else:
                logger.warn('Got SIGCHLD with exit_code:%r for pid:%r and task_id:%r', exit_code, pid, options['correlation_id'])
                options['error_callback'](ExcInfo(WorkerLostError(exit_code)))
            if self.active:
                self.sem.release()
            process_destructor(pid, exit_code)

    @staticmethod
    def terminate_job(pid, signum):
        logger.warn("Killing pid:%s with signum:%s", pid, signum)
        try:
            os.kill(pid, signum)
        except OSError as exc:
            if exc.errno != errno.ESRCH:
                raise

    def on_apply(self, target, args, kwargs, **options):
        accept_callback = options['accept_callback']

        process = Workhorse(target, args, kwargs)
        self.workers[process.pid] = options
        if accept_callback:
            accept_callback(process.pid, monotonic())

    def on_stop(self):
        try:
            for pid in list(self.workers):
                try:
                    pid, exit_code = os.waitpid(pid, 0)
                except OSError as exc:
                    if exc.errno != errno.ECHILD:
                        logger.warn("Failed to wait for child process %s: %s", pid, exc)
                    continue
                else:
                    self.on_worker_exit(pid, exit_code)
        except:
            self.terminate()
            raise
    on_close = on_stop

    def terminate(self, timeout=5):
        for pid in self.workers:
            self.terminate_job(pid, signal.SIGTERM)

        while self.workers and timeout > 0:
            self.on_sigchld()
            time.sleep(1)
            timeout -= 1

        for pid in self.workers:
            self.terminate_job(pid, signal.SIGKILL)

    def _get_info(self):
        return {
            'max-concurrency': self.limit,
            'processes': [pid for pid in self.workers],
            'max-tasks-per-child': 1,
            'put-guarded-by-semaphore': self.putlocks,
            'timeouts': 0, # TODO: support timeouts
        }

    # TODO: missing restart method !?
