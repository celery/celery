"""
Remote debugger for Celery tasks running in multiprocessing pool workers.

Inspired by http://snippets.dzone.com/posts/show/7248

**Usage**

.. code-block:: python

    from celery.contrib import rdb
    from celery.decorators import task

    @task
    def add(x, y):
        result = x + y
        rdb.set_trace()
        return result



"""
import bdb
import errno
import os
import socket
import sys

from pdb import Pdb

default_port = 6899

CELERY_RDB_HOST = os.environ.get("CELERY_RDB_HOST") or socket.gethostname()
CELERY_RDB_PORT = int(os.environ.get("CELERY_RDB_PORT") or default_port)

#: Holds the currently active debugger.
_current = [None]

_frame = getattr(sys, "_getframe")


class Rdb(Pdb):
    me = "Remote Debugger"
    _prev_outs = None
    _sock = None

    def __init__(self, host=CELERY_RDB_HOST, port=CELERY_RDB_PORT,
            port_search_limit=100, port_skew=+0):
        self.active = True

        try:
            from multiprocessing import current_process
            _, port_skew = current_process().name.split('-')
        except (ImportError, ValueError):
            pass
        port_skew = int(port_skew)

        self._prev_handles = sys.stdin, sys.stdout
        this_port = None
        for i in xrange(port_search_limit):
            self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            this_port = port + port_skew + i
            try:
                self._sock.bind((host, this_port))
            except socket.error, exc:
                if exc.errno in [errno.EADDRINUSE, errno.EINVAL]:
                    continue
                raise
            else:
                break
        else:
            raise Exception(
                "%s: Could not find available port. Please set using "
                "environment variable CELERY_RDB_PORT" % (self.me, ))


        self._sock.listen(1)
        me = "%s:%s" % (self.me, this_port)
        context = self.context = {"me": me, "host": host, "port": this_port}
        print("%(me)s: Please telnet %(host)s %(port)s."
              "  Type `exit` in session to continue." % context)
        print("%(me)s: Waiting for client..." % context)

        self._client, address = self._sock.accept()
        context["remote_addr"] = ":".join(map(str, address))
        print("%(me)s: In session with %(remote_addr)s" % context)
        self._handle = sys.stdin = sys.stdout = self._client.makefile("rw")
        Pdb.__init__(self, completekey="tab",
                           stdin=self._handle, stdout=self._handle)

    def _close_session(self):
        self.stdin, self.stdout = sys.stdin, sys.stdout = self._prev_handles
        self._handle.close()
        self._client.close()
        self._sock.close()
        self.active = False
        print("%(me)s: Session %(remote_addr)s ended." % self.context)

    def do_continue(self, arg):
        self._close_session()
        self.set_continue()
        return 1
    do_c = do_cont = do_continue

    def do_quit(self, arg):
        self._close_session()
        self.set_quit()
        return 1
    do_q = do_exit = do_quit

    def set_trace(self, frame=None):
        if frame is None:
            frame = _frame().f_back
        try:
            Pdb.set_trace(self, frame)
        except socket.error, exc:
            # connection reset by peer.
            if socket.errno != ECONNRESET:
                raise

    def set_quit(self):
        # this raises a BdbQuit exception that we are unable to catch.
        sys.settrace(None)



def debugger():
    rdb = _current[0]
    if rdb is None or not rdb.active:
        rdb = _current[0] = Rdb()
    return rdb


def set_trace():
    return debugger().set_trace(_frame().f_back)
