"""Teardown of a half-open broker connection must be bounded.

Two long-standing hangs share one cause: a read issued on a socket that is
already open, against a peer that has gone silent without sending RST - a
suspended VM, or a firewall dropping the flow.

* GH-9705 (reconnect): ``on_connection_error_after_connected()`` wedged
  because the redis transport drains a pending ``BRPOP`` in
  ``Channel.close()``, so the reconnect was never reached.
* celery#975 (shutdown): ``_shutdown()`` wedged waiting for a reply during
  connection close.

Neither ``socket.setdefaulttimeout()`` nor kombu's
``Connection.collect(socket_timeout=...)`` can bound those reads - both only
affect sockets created *afterwards*.

Reproducing this needs a peer that goes *silent*: killing the broker is not
enough, because the peer then sends FIN/RST and the read returns immediately.
So a proxy sits between the worker and the broker and simply stops relaying
while holding both sockets open.
"""
import socket
import threading
import time
from unittest.mock import Mock
from urllib.parse import urlparse

import pytest
from kombu import Connection, Consumer, Queue
from kombu.common import ignore_errors

from celery.utils.threads import bound_open_broker_sockets
from celery.worker.consumer.consumer import COLLECT_SOCKET_TIMEOUT
from celery.worker.consumer.consumer import Consumer as WorkerConsumer
from celery.worker.worker import SHUTDOWN_SOCKET_TIMEOUT

from .conftest import TEST_BROKER

#: Teardown must finish within its own bound; allow generous slack for slow
#: CI, while still failing fast if it blocks forever.
CLEANUP_DEADLINE = COLLECT_SOCKET_TIMEOUT * 4
SHUTDOWN_DEADLINE = SHUTDOWN_SOCKET_TIMEOUT * 4

#: Long BRPOP, so the broker cannot answer during the test and the reply
#: cannot already be sitting in the socket buffer when we go silent.
POLLING_INTERVAL = 60

#: Exactly ``redis``: the proxy relays plaintext TCP, so it cannot stand in
#: for ``rediss`` (TLS) or ``redis+socket`` (unix socket), both of which would
#: otherwise satisfy a ``startswith('redis')`` check.
pytestmark = pytest.mark.skipif(
    urlparse(TEST_BROKER).scheme != 'redis',
    reason='half-open cleanup path needs a plaintext TCP redis broker',
)


class BlackholeProxy:
    """TCP proxy that can go silent without tearing the connection down."""

    def __init__(self, upstream):
        self.upstream = upstream
        self._silent = threading.Event()
        self._closed = threading.Event()
        self._socks = []
        self._threads = []
        self._server = socket.socket()
        self._server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server.bind(('127.0.0.1', 0))
        self._server.listen(16)
        self.port = self._server.getsockname()[1]
        self._spawn(self._accept_loop)

    def _spawn(self, target, *args):
        thread = threading.Thread(target=target, args=args, daemon=True)
        self._threads.append(thread)
        thread.start()

    def _accept_loop(self):
        while not self._closed.is_set():
            try:
                client, _ = self._server.accept()
                server = socket.create_connection(self.upstream)
            except OSError:
                return
            self._socks += [client, server]
            for src, dst in ((client, server), (server, client)):
                self._spawn(self._pump, src, dst)

    def _pump(self, src, dst):
        src.settimeout(0.2)
        while not self._closed.is_set():
            if self._silent.is_set():
                time.sleep(0.1)     # hold the socket open, relay nothing
                continue
            try:
                data = src.recv(65536)
            except TimeoutError:
                continue
            except OSError:
                return
            if not data:
                return
            try:
                dst.sendall(data)
            except OSError:
                return

    def blackhole(self):
        """Go silent: no more bytes either way, but no FIN and no RST."""
        self._silent.set()

    def close(self):
        """Stop relaying, close every socket and join the threads."""
        self._closed.set()
        for sock in [self._server, *self._socks]:
            sock.close()
        for thread in self._threads:
            thread.join(timeout=2)


def _proxied_broker_url(port):
    """TEST_BROKER with only the host:port swapped for the proxy's.

    Credentials, db index and query params have to survive, or the test dials
    an authed broker with no password, or exercises the wrong db.
    """
    url = urlparse(TEST_BROKER)
    userinfo = ''
    if url.username or url.password:
        userinfo = f'{url.username or ""}:{url.password or ""}@'
    return url._replace(netloc=f'{userinfo}127.0.0.1:{port}').geturl()


@pytest.fixture
def blackhole_proxy():
    url = urlparse(TEST_BROKER)
    proxy = BlackholeProxy((url.hostname or 'localhost', url.port or 6379))
    yield proxy
    proxy.close()


def _consumer_with_connection(app, connection):
    pool = Mock(name='pool')
    # A real int: the handler computes max_prefetch_count as
    # ``pool.num_processes * prefetch_multiplier``, and a bare Mock makes that
    # raise TypeError, so the rest of the handler would never run.
    pool.num_processes = 2
    consumer = WorkerConsumer(
        on_task_request=Mock(), init_callback=Mock(), pool=pool, app=app,
        timer=Mock(), controller=Mock(), hub=None,
    )
    consumer.blueprint = Mock(name='blueprint')
    consumer.connection = connection
    consumer.conninfo = connection
    return consumer


@pytest.fixture
def half_open_connection(blackhole_proxy):
    """A connected broker connection with a BRPOP outstanding, gone silent."""
    connection = Connection(
        _proxied_broker_url(blackhole_proxy.port),
        transport_options={'polling_interval': POLLING_INTERVAL},
    )
    connection.connect()

    consumer = Consumer(connection, queues=[Queue('t_gh9705')],
                        accept=['json'], callbacks=[lambda b, m: m.ack()])
    consumer.consume()

    # Arm the BRPOP that teardown will later try to drain.
    try:
        connection.drain_events(timeout=1.0)
    except Exception:
        pass
    assert consumer.channel._in_poll, 'BRPOP was not armed, test is inconclusive'

    blackhole_proxy.blackhole()
    yield connection

    # Teardown against a silenced peer is expected to raise, and a test that
    # failed may leave the connection wedged: drop it either way so its
    # sockets and poller fd do not outlive the test.
    connection._closed = True
    for channel in list(getattr(connection.transport, 'channels', None) or ()):
        client = channel.__dict__.get('client')
        conn = getattr(client, 'connection', None)
        if conn is not None:
            ignore_errors(connection, conn.disconnect)


def _run_bounded(target, deadline, what, expected=()):
    """Run ``target`` off-thread and assert it finishes within ``deadline``.

    Teardown against a silent peer is *expected* to surface a timeout once the
    socket is bounded, so those are tolerated via ``expected``.  Anything else
    is re-raised: swallowing every exception would let the target crash
    instantly and still look "bounded".
    """
    done = threading.Event()
    raised = []

    def run():
        try:
            target()
        except BaseException as exc:  # re-raised below
            raised.append(exc)
        finally:
            done.set()

    threading.Thread(target=run, daemon=True).start()
    assert done.wait(deadline), (
        f'{what} still blocked after {deadline}s on a half-open connection; '
        f'the worker would hang here forever'
    )
    if raised and not isinstance(raised[0], expected):
        raise AssertionError(
            f'{what} returned quickly only because it crashed: '
            f'{type(raised[0]).__name__}: {raised[0]}'
        ) from raised[0]


def test_reconnect_cleanup_is_bounded(celery_session_app, half_open_connection):
    """GH-9705: the error handler must not wedge before blueprint.restart()."""
    consumer = _consumer_with_connection(celery_session_app,
                                         half_open_connection)

    _run_bounded(
        lambda: consumer.on_connection_error_after_connected(OSError('gone')),
        CLEANUP_DEADLINE, 'on_connection_error_after_connected()')

    assert consumer.connection is None, (
        'the broken connection must be released before blueprint.restart()')


def test_cold_shutdown_teardown_is_bounded(half_open_connection):
    """celery#975: a cold shutdown must not wedge closing the connection.

    ``on_cold_shutdown`` bounds the socket, then cancels the consumer and
    closes the connection, which for redis runs the same ``Channel.close()``
    drain.  Warm shutdown deliberately does *not* bound (it still has acks to
    flush); that guarantee is pinned in ``t/unit/worker/test_worker.py``.
    """
    bound_open_broker_sockets(half_open_connection, SHUTDOWN_SOCKET_TIMEOUT)

    _run_bounded(lambda: half_open_connection.close(),
                 SHUTDOWN_DEADLINE, 'Connection.close()')
