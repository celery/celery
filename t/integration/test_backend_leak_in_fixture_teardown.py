import weakref

import pytest

from celery.backends.redis import RedisBackend
from celery.contrib.testing.app import TestApp, setup_default_app
from celery.contrib.testing.worker import start_worker

from .conftest import TEST_BACKEND


def backend_socket_refs(backend):
    # Capture backend sockets opened by this run without preventing GC from reclaiming them.
    pool = backend.client.connection_pool
    connections = [*pool._available_connections, *pool._in_use_connections]
    return [
        weakref.ref(connection._sock)
        for connection in connections
        if connection._sock is not None and connection._sock.fileno() >= 0
    ]


def socket_is_open(socket_ref):
    # The socket may have been reclaimed by GC during the run.
    sock = socket_ref()
    return sock is not None and sock.fileno() >= 0


def run_one_fixture_cycle():
    app = TestApp(config={'result_backend': TEST_BACKEND})

    @app.task
    def store_result():
        return 'stored'

    with setup_default_app(app):
        if not isinstance(app.backend, RedisBackend):
            pytest.skip('Requires redis result backend.')
        with start_worker(app):
            # The task ensures the connection to the backend is opened.
            result = store_result.delay()
            assert result.get(timeout=10) == 'stored'
            sockets = backend_socket_refs(app.backend)
    return app, sockets


def test_fixture_teardown_releases_backend_connections():
    iterations = 20

    apps = []
    backend_sockets = []

    for _ in range(iterations):
        test_app, sockets = run_one_fixture_cycle()
        apps.append(test_app)
        backend_sockets.extend(sockets)

    # Across these repeated cycles, automatic GC is expected to run and reclaim
    # some backend sockets. Retained backend references would keep them all open.
    open_sockets = sum(socket_is_open(socket_ref) for socket_ref in backend_sockets)
    assert open_sockets < len(backend_sockets)
