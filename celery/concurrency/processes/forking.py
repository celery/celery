"""
    Module for starting a process object using os.fork() or CreateProcess()

    multiprocessing/forking.py

    Copyright (c) 2006-2008, R Oudkerk
    All rights reserved.

    Redistribution and use in source and binary forms, with or without
    modification, are permitted provided that the following conditions
    are met:

    1. Redistributions of source code must retain the above copyright
    notice, this list of conditions and the following disclaimer.
    2. Redistributions in binary form must reproduce the above copyright
    notice, this list of conditions and the following disclaimer in the
    documentation and/or other materials provided with the distribution.
    3. Neither the name of author nor the names of any contributors may be
    used to endorse or promote products derived from this software
    without specific prior written permission.

    THIS SOFTWARE IS PROVIDED BY THE AUTHOR AND CONTRIBUTORS "AS IS" AND
    ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
    IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
    ARE DISCLAIMED.  IN NO EVENT SHALL THE AUTHOR OR CONTRIBUTORS BE LIABLE
    FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
    DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
    OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
    HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
    LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
    OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
    SUCH DAMAGE.
"""
from __future__ import absolute_import

import os
import sys

from multiprocessing import current_process
from multiprocessing import forking as _forking
from multiprocessing import process
from pickle import load, HIGHEST_PROTOCOL

Popen = _forking.Popen


def dump(obj, file, protocol=None):
    _forking.ForkingPickler(file, protocol).dump(obj)


if sys.platform != "win32":
    import threading

    class Popen(_forking.Popen):  # noqa
        _tls = threading.local()
        returncode = None

        def __init__(self, process_obj):
            self.force_execv = getattr(process_obj, "force_execv", False)

            if self.force_execv:
                sys.stdout.flush()
                sys.stderr.flush()
                r, w = os.pipe()
                self.sentinel = r

                from_parent_fd, to_child_fd = os.pipe()
                cmd = get_command_line() + [str(from_parent_fd)]

                self.pid = os.fork()
                if self.pid == 0:
                    os.close(r)
                    os.close(to_child_fd)
                    os.execv(sys.executable, cmd)

                # send information to child
                prep_data = get_preparation_data(process_obj._name)
                os.close(from_parent_fd)
                to_child = os.fdopen(to_child_fd, 'wb')
                Popen._tls.process_handle = self.pid
                try:
                    dump(prep_data, to_child, HIGHEST_PROTOCOL)
                    dump(process_obj, to_child, HIGHEST_PROTOCOL)
                finally:
                    del(Popen._tls.process_handle)
                    to_child.close()
            else:
                super(Popen, self).__init__(process_obj)

        @staticmethod
        def thread_is_spawning():
            return getattr(Popen._tls, "process_handle", None) is not None

        @staticmethod
        def duplicate_for_child(handle):
            return handle

    def is_forking(argv):
        if len(argv) >= 2 and argv[1] == '--multiprocessing-fork':
            assert len(argv) == 3
            return True
        return False

    def freeze_support():
        if is_forking(sys.argv):
            main()
            sys.exit()

    def get_command_line():
        if current_process()._identity == () and is_forking(sys.argv):
            raise RuntimeError(
                "Can't start new process while bootstrapping another")
        if getattr(sys, "frozen", False):
            return [sys.executable, '--multiprocessing-fork']
        else:
            prog = """\
from celery.concurrency.processes.forking import main; main()"""
            return [sys.executable, '-c', prog, '--multiprocessing-fork']

    def main():
        assert is_forking(sys.argv)
        fd = int(sys.argv[-1])
        from_parent = os.fdopen(fd, 'rb')
        current_process()._inheriting = True
        preparation_data = load(from_parent)
        _forking.prepare(preparation_data)

        # Huge hack to make logging before Process.run work.
        loglevel = os.environ.get("_MP_FORK_LOGLEVEL_")
        logfile = os.environ.get("_MP_FORK_LOGFILE_") or None
        format = os.environ.get("_MP_FORK_LOGFORMAT_")
        if loglevel:
            from multiprocessing import util
            import logging
            logger = util.get_logger()
            logger.setLevel(int(loglevel))
            if not logger.handlers:
                logger._rudimentary_setup = True
                logfile = logfile or sys.__stderr__
                if hasattr(logfile, "write"):
                    handler = logging.StreamHandler(logfile)
                else:
                    handler = logging.FileHandler(logfile)
                formatter = logging.Formatter(
                        format or util.DEFAULT_LOGGING_FORMAT)
                handler.setFormatter(formatter)
                logger.addHandler(handler)

        self = load(from_parent)
        current_process()._inheriting = False

        exitcode = self._bootstrap()
        exit(exitcode)

    def get_preparation_data(name):
        from multiprocessing.util import _logger, _log_to_stderr
        d = dict(name=name,
                 sys_path=sys.path,
                 sys_argv=sys.argv,
                 log_to_stderr=_log_to_stderr,
                 orig_dir=process.ORIGINAL_DIR,
                 authkey=process.current_process().authkey)
        if _logger is not None:
            d["log_level"] = _logger.getEffectiveLevel()
        main_path = getattr(sys.modules['__main__'], '__file__', None)
        if not main_path and sys.argv[0] not in ('', '-c'):
            main_path = sys.argv[0]
        if main_path is not None:
            if not os.path.isabs(main_path) \
                    and process.ORIGINAL_DIR is not None:
                main_path = os.path.join(process.ORIGINAL_DIR, main_path)
            d["main_path"] = os.path.normpath(main_path)
        return d

    from _multiprocessing import Connection

    def reduce_connection(conn):
        if not Popen.thread_is_spawning():
            raise RuntimeError("blabla")
        return type(conn), (Popen.duplicate_for_child(conn.fileno()),
                            conn.readable, conn.writable)
    _forking.ForkingPickler.register(Connection, reduce_connection)

    _forking.Popen = Popen
else:
    from multiprocessing.forking import freeze_support  # noqa
