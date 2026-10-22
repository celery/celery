import socket
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from celery.utils.threads import (Local, LocalManager, _FastLocalStack, _LocalStack, bgThread,
                                  bound_open_broker_sockets, default_socket_timeout)
from t.unit import conftest


class test_default_socket_timeout:

    @pytest.fixture(autouse=True)
    def _restore_default_timeout(self):
        prev = socket.getdefaulttimeout()
        socket.setdefaulttimeout(None)
        yield
        socket.setdefaulttimeout(prev)

    def test_sets_and_restores_timeout(self):
        with default_socket_timeout(5.0):
            assert socket.getdefaulttimeout() == 5.0
        assert socket.getdefaulttimeout() is None

    def test_restores_timeout_when_body_raises(self):
        # Without try/finally the timeout leaks out of the block and every
        # socket created later in the process is silently bounded by it.
        with pytest.raises(ValueError):
            with default_socket_timeout(5.0):
                raise ValueError('boom')
        assert socket.getdefaulttimeout() is None


class test_bound_open_broker_sockets:
    """Bounding sockets that are already connected (GH-9705, celery#975)."""

    def _sock(self):
        sock, peer = socket.socketpair()
        self._open.extend((sock, peer))
        return sock

    @pytest.fixture(autouse=True)
    def _close_sockets(self):
        self._open = []
        yield
        for sock in self._open:
            sock.close()

    def test_bounds_virtual_transport_sockets(self):
        # redis and the other virtual transports keep a redis-py style
        # client per channel, read from during Channel.close().
        client, sub = self._sock(), self._sock()
        channel = SimpleNamespace(
            client=SimpleNamespace(connection=SimpleNamespace(_sock=client)),
            subclient=SimpleNamespace(connection=SimpleNamespace(_sock=sub)),
        )
        connection = SimpleNamespace(
            transport=SimpleNamespace(channels=[channel]), _connection=None)

        bound_open_broker_sockets(connection, 7.5)

        assert client.gettimeout() == 7.5
        assert sub.gettimeout() == 7.5

    def test_bounds_amqp_transport_socket(self):
        # py-amqp keeps one socket on the connection's transport; this is the
        # read that hangs in basic_cancel during shutdown (celery#975).
        sock = self._sock()
        connection = SimpleNamespace(
            transport=SimpleNamespace(channels=None),
            _connection=SimpleNamespace(_transport=SimpleNamespace(sock=sock)),
        )

        bound_open_broker_sockets(connection, 3.0)

        assert sock.gettimeout() == 3.0

    def test_never_touches_the_reconnecting_connection_property(self):
        # kombu's public ``Connection.connection`` property calls
        # _ensure_connection(), which would dial the dead broker and block -
        # worse than the bug being fixed.  Only ``_connection`` is safe.
        class Connection:
            transport = SimpleNamespace(channels=None)
            _connection = None

            @property
            def connection(self):
                raise AssertionError('must not reconnect to a dead broker')

        bound_open_broker_sockets(Connection(), 5.0)

    def test_never_touches_the_reconnecting_amqp_transport_property(self):
        # py-amqp's ``Connection.transport`` property calls self.connect()
        # when _transport is None, and _transport IS None after
        # amqp's _on_close_ok() -> collect(), which is exactly the
        # server-initiated close that lands us here.  Reading it would dial
        # the broker we are abandoning.
        class AmqpConnection:
            _transport = None

            @property
            def transport(self):
                raise AssertionError('must not reconnect to a dead broker')

        bound_open_broker_sockets(
            SimpleNamespace(transport=SimpleNamespace(channels=None),
                            _connection=AmqpConnection()), 5.0)

    def test_does_not_open_a_new_client(self):
        # The broker is already known to be unresponsive; going through the
        # ``client`` property would dial it again and block.
        class ExplodingChannel:
            @property
            def client(self):
                raise AssertionError('must not open a new connection')

        bound_open_broker_sockets(
            SimpleNamespace(
                transport=SimpleNamespace(channels=[ExplodingChannel()]),
                _connection=None),
            5.0)

    def test_ignores_connection_without_sockets(self):
        bound_open_broker_sockets(
            SimpleNamespace(transport=SimpleNamespace(channels=None),
                            _connection=None), 5.0)

    def test_survives_unusable_connection(self):
        # Best effort: teardown must carry on to close() regardless.
        class Unusable:
            @property
            def transport(self):
                raise OSError('connection is gone')

        bound_open_broker_sockets(Unusable(), 5.0)

    def test_survives_closed_socket(self):
        sock, peer = socket.socketpair()
        sock.close()
        peer.close()
        connection = SimpleNamespace(
            transport=SimpleNamespace(channels=[SimpleNamespace(
                client=SimpleNamespace(
                    connection=SimpleNamespace(_sock=sock)))]),
            _connection=None)

        bound_open_broker_sockets(connection, 5.0)


class test_bgThread:

    def test_crash(self):

        class T(bgThread):

            def body(self):
                raise KeyError()

        with patch('os._exit') as _exit:
            with conftest.stdouts():
                _exit.side_effect = ValueError()
                t = T()
                with pytest.raises(ValueError):
                    t.run()
                _exit.assert_called_with(1)

    def test_interface(self):
        x = bgThread()
        with pytest.raises(NotImplementedError):
            x.body()


class test_Local:

    def test_iter(self):
        x = Local()
        x.foo = 'bar'
        ident = x.__ident_func__()
        assert (ident, {'foo': 'bar'}) in list(iter(x))

        delattr(x, 'foo')
        assert (ident, {'foo': 'bar'}) not in list(iter(x))
        with pytest.raises(AttributeError):
            delattr(x, 'foo')

        assert x(lambda: 'foo') is not None


class test_LocalStack:

    def test_stack(self):
        x = _LocalStack()
        assert x.pop() is None
        x.__release_local__()
        ident = x.__ident_func__
        x.__ident_func__ = ident

        with pytest.raises(RuntimeError):
            x()[0]

        x.push(['foo'])
        assert x()[0] == 'foo'
        x.pop()
        with pytest.raises(RuntimeError):
            x()[0]


class test_FastLocalStack:

    def test_stack(self):
        x = _FastLocalStack()
        x.push(['foo'])
        x.push(['bar'])
        assert x.top == ['bar']
        assert len(x) == 2
        x.pop()
        assert x.top == ['foo']
        x.pop()
        assert x.top is None


class test_LocalManager:

    def test_init(self):
        x = LocalManager()
        assert x.locals == []
        assert x.ident_func

        def ident():
            return 1
        loc = Local()
        x = LocalManager([loc], ident_func=ident)
        assert x.locals == [loc]
        x = LocalManager(loc, ident_func=ident)
        assert x.locals == [loc]
        assert x.ident_func is ident
        assert x.locals[0].__ident_func__ is ident
        assert x.get_ident() == 1

        with patch('celery.utils.threads.release_local') as release:
            x.cleanup()
            release.assert_called_with(loc)

        assert repr(x)
