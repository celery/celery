from __future__ import absolute_import, unicode_literals

import errno
import socket

import pytest

from case import Mock, patch, skip
from celery.contrib.rdb import Rdb, debugger, set_trace
from celery.five import WhateverIO


class SockErr(socket.error):
    errno = None


class test_Rdb:

    @patch('celery.contrib.rdb.Rdb')
    def test_debugger(self, Rdb):
        x = debugger()
        assert x
        assert x is debugger()

    @patch('celery.contrib.rdb.debugger')
    @patch('celery.contrib.rdb._frame')
    def test_set_trace(self, _frame, debugger):
        assert set_trace(Mock())
        assert set_trace()
        debugger.return_value.set_trace.assert_called()

    @patch('celery.contrib.rdb.Rdb.get_avail_port')
    @skip.if_pypy()
    def test_rdb(self, get_avail_port):
        sock = Mock()
        get_avail_port.return_value = (sock, 8000)
        sock.accept.return_value = (Mock(), ['helu'])
        out = WhateverIO()
        with Rdb(out=out) as rdb:
            get_avail_port.assert_called()
            assert 'helu' in out.getvalue()

            # set_quit
            with patch('sys.settrace') as settrace:
                rdb.set_quit()
                settrace.assert_called_with(None)

            # set_trace
            with patch('celery.contrib.rdb.Pdb.set_trace') as pset:
                with patch('celery.contrib.rdb._frame'):
                    rdb.set_trace()
                    rdb.set_trace(Mock())
                    pset.side_effect = SockErr
                    pset.side_effect.errno = errno.ENOENT
                    with pytest.raises(SockErr):
                        rdb.set_trace()

            # _close_session
            rdb._close_session()
            rdb.active = True
            rdb._handle = None
            rdb._client = None
            rdb._sock = None
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
    @skip.if_pypy()
    def test_get_avail_port(self, sock):
        out = WhateverIO()
        sock.return_value.accept.return_value = (Mock(), ['helu'])
        with Rdb(out=out):
            pass

        with patch('celery.contrib.rdb.current_process') as curproc:
            curproc.return_value.name = 'PoolWorker-10'
            with Rdb(out=out):
                pass

        err = sock.return_value.bind.side_effect = SockErr()
        err.errno = errno.ENOENT
        with pytest.raises(SockErr):
            with Rdb(out=out):
                pass
        err.errno = errno.EADDRINUSE
        with pytest.raises(Exception):
            with Rdb(out=out):
                pass
        called = [0]

        def effect(*a, **kw):
            try:
                if called[0] > 50:
                    return True
                raise err
            finally:
                called[0] += 1
        sock.return_value.bind.side_effect = effect
        with Rdb(out=out):
            pass
