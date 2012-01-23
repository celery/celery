from __future__ import absolute_import

import os
import sys

from multiprocessing import current_process
from multiprocessing import forking as _forking
from multiprocessing import process
from pickle import load, dump as _dump, HIGHEST_PROTOCOL

Popen = _forking.Popen


def dump(obj, file, protocol=None):
    _forking.ForkingPickler(file, protocol).dump(obj)


if sys.platform != "win32":
    import threading

    class Popen(_forking.Popen):  # noqa
        _tls = threading.local()
        returncode = None

        def __init__(self, process_obj):
            self.should_fork = process_obj.should_fork

            if not self.should_fork:
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
    from multiprocessing.forking import freeze_support



