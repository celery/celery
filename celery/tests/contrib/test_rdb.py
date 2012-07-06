from __future__ import absolute_import
from __future__ import with_statement

import errno
import socket

from mock import Mock, patch

from celery.contrib.rdb import (
    Rdb,
    debugger,
    set_trace,
)
from celery.tests.utils import Case, WhateverIO, skip_if_pypy


class test_Rdb(Case):

    @patch('celery.contrib.rdb.Rdb')
    def test_debugger(self, Rdb):
        x = debugger()
        self.assertTrue(x)
        self.assertIs(x, debugger())

    @patch('celery.contrib.rdb.debugger')
    @patch('celery.contrib.rdb._frame')
    def test_set_trace(self, _frame, debugger):
        self.assertTrue(set_trace(Mock()))
        self.assertTrue(set_trace())
        self.assertTrue(debugger.return_value.set_trace.called)

    @patch('celery.contrib.rdb.Rdb.get_avail_port')
    @skip_if_pypy
    def test_rdb(self, get_avail_port):
        sock = Mock()
        get_avail_port.return_value = (sock, 8000)
        sock.accept.return_value = (Mock(), ['helu'])
        out = WhateverIO()
        rdb = Rdb(out=out)
        self.assertTrue(get_avail_port.called)
        self.assertIn('helu', out.getvalue())

        # set_quit
        with patch('sys.settrace') as settrace:
            rdb.set_quit()
            settrace.assert_called_with(None)

        # set_trace
        with patch('celery.contrib.rdb.Pdb.set_trace') as pset:
            with patch('celery.contrib.rdb._frame'):
                rdb.set_trace()
                rdb.set_trace(Mock())
                pset.side_effect = socket.error
                pset.side_effect.errno = errno.ECONNRESET
                rdb.set_trace()
                pset.side_effect.errno = errno.ENOENT
                with self.assertRaises(socket.error):
                    rdb.set_trace()

        # _close_session
        rdb._close_session()

        # do_continue
        rdb.set_continue = Mock()
        rdb.do_continue(Mock())
        rdb.set_continue.assert_called_with()

        # do_quit
        rdb.set_quit = Mock()
        rdb.do_quit(Mock())
        rdb.set_quit.assert_called_with()

    @patch('socket.socket')
    @skip_if_pypy
    def test_get_avail_port(self, sock):
        out = WhateverIO()
        sock.return_value.accept.return_value = (Mock(), ['helu'])
        Rdb(out=out)

        with patch('celery.contrib.rdb.current_process') as curproc:
            curproc.return_value.name = 'PoolWorker-10'
            Rdb(out=out)

        err = sock.return_value.bind.side_effect = socket.error()
        err.errno = errno.ENOENT
        with self.assertRaises(socket.error):
            Rdb(out=out)
        err.errno = errno.EADDRINUSE
        with self.assertRaises(Exception):
            Rdb(out=out)
        called = [0]

        def effect(*a, **kw):
            try:
                if called[0] > 50:
                    return True
                raise err
            finally:
                called[0] += 1
        sock.return_value.bind.side_effect = effect
        Rdb(out=out)
