"""Smoke tests for a gevent client on the Redis result backend.

Regression coverage for the hang introduced by #10671: with a
monkey-patched client, ``result.get()`` never returned and ignored its
timeout, because the drainer greenlet kept the pubsub lock while it
waited for messages.
"""

from __future__ import annotations

import subprocess
import sys

import pytest
from pytest_celery import (RESULT_TIMEOUT, CeleryBackendCluster, CeleryBrokerCluster, CeleryTestSetup,
                           RedisTestBackend, RedisTestBroker)

pytest.importorskip("gevent")

TASKS = 20
PENDING_TIMEOUT = 2

# Runs in its own interpreter: monkey patching is process-wide and
# cannot be undone, so it must not happen in the pytest process.
CLIENT = """
from gevent import monkey
monkey.patch_all()

import sys
import time

import gevent

from celery import Celery, uuid
from celery.exceptions import TimeoutError

broker, backend, queue, tasks, result_timeout, pending_timeout = sys.argv[1:]
app = Celery(broker=broker, backend=backend)

results = [
    app.send_task("t.integration.tasks.add", args=(i, i), queue=queue)
    for i in range(int(tasks))
]
jobs = [gevent.spawn(r.get, timeout=float(result_timeout)) for r in results]
gevent.joinall(jobs, raise_error=True)
assert [job.value for job in jobs] == [i + i for i in range(int(tasks))]

start = time.monotonic()
try:
    app.AsyncResult(uuid()).get(timeout=float(pending_timeout))
except TimeoutError:
    print(time.monotonic() - start)
"""


@pytest.fixture
def celery_broker_cluster(celery_redis_broker: RedisTestBroker) -> CeleryBrokerCluster:
    cluster = CeleryBrokerCluster(celery_redis_broker)
    yield cluster
    cluster.teardown()


@pytest.fixture
def celery_backend_cluster(celery_redis_backend: RedisTestBackend) -> CeleryBackendCluster:
    cluster = CeleryBackendCluster(celery_redis_backend)
    yield cluster
    cluster.teardown()


class test_redis_backend_gevent:
    def test_get_under_gevent(self, celery_setup: CeleryTestSetup):
        conf = celery_setup.app.conf
        args = [
            conf.broker_url,
            conf.result_backend,
            celery_setup.worker.worker_queue,
            str(TASKS),
            str(RESULT_TIMEOUT),
            str(PENDING_TIMEOUT),
        ]
        try:
            client = subprocess.run(
                [sys.executable, "-c", CLIENT, *args],
                capture_output=True,
                text=True,
                timeout=RESULT_TIMEOUT * 2,
            )
        except subprocess.TimeoutExpired:
            pytest.fail(f"gevent client did not finish in {RESULT_TIMEOUT * 2}s")

        assert client.returncode == 0, client.stderr
        assert client.stdout, "get() on a pending task did not raise TimeoutError"
        # get() on a task that never runs must honour its timeout.  The
        # slack covers the drainer's one second tick and the host lookup
        # of the new connection, which gevent's resolver can make slow.
        assert PENDING_TIMEOUT <= float(client.stdout) < PENDING_TIMEOUT + 15
