"""Smoke tests for garbage-collected results on the Redis result backend.

Regression coverage for #10326: the garbage collector can run
``AsyncResult.__del__`` -> ``cancel_for`` while the same thread is in the
middle of a pubsub SUBSCRIBE, UNSUBSCRIBE or ``get_message()``.  The nested
UNSUBSCRIBE deadlocked on redis-py < 6.4, and on later versions could be
written into the middle of the outer command.
"""

from __future__ import annotations

import subprocess
import sys

import pytest
from pytest_celery import (RESULT_TIMEOUT, CeleryBackendCluster, CeleryBrokerCluster, CeleryTestSetup,
                           RedisTestBackend, RedisTestBroker)

TASKS = 20
DROPPED_PER_TASK = 5

# Runs in its own interpreter: it patches redis-py and, with gevent,
# monkey patches the whole process.
CLIENT = """
import sys

concurrency, broker, backend, queue, tasks, dropped, result_timeout = sys.argv[1:]
if concurrency == "gevent":
    from gevent import monkey
    monkey.patch_all()

import gc
import threading
import time

import redis.client
import redis.connection

from celery import Celery, uuid

app = Celery(broker=broker, backend=backend)

# Record any call on a pubsub connection that is already busy, from any
# thread or greenlet: its operations must run one at a time.
in_flight = {}
overlapping = []
# set while this thread (or greenlet) is inside a pubsub call
state = threading.local()


def tracked(name, call):
    def wrapper(self, *args, **kwargs):
        if in_flight.get(id(self)):
            overlapping.append(name)
        in_flight[id(self)] = in_flight.get(id(self), 0) + 1
        depth = getattr(state, "depth", 0)
        state.depth = depth + 1
        try:
            return call(self, *args, **kwargs)
        finally:
            state.depth = depth
            in_flight[id(self)] -= 1
    return wrapper


redis.client.PubSub.execute_command = tracked(
    "execute_command", redis.client.PubSub.execute_command)
redis.client.PubSub.get_message = tracked(
    "get_message", redis.client.PubSub.get_message)

# Run the garbage collector in the middle of every pubsub command and read,
# where finalizers of dropped results fire in production.
Connection = redis.connection.Connection
send_packed_command = Connection.send_packed_command
read_response = Connection.read_response


def send_and_collect(self, *args, **kwargs):
    result = send_packed_command(self, *args, **kwargs)
    if getattr(state, "depth", 0):
        gc.collect()
    return result


def collect_and_read(self, *args, **kwargs):
    if getattr(state, "depth", 0):
        gc.collect()
    return read_response(self, *args, **kwargs)


Connection.send_packed_command = send_and_collect
Connection.read_response = collect_and_read


class Cycle:
    # only the cyclic garbage collector can free this, and the result in it
    def __init__(self, result):
        self.result = result
        self.self = self


def drop_pending_results(n):
    for _ in range(n):
        result = app.AsyncResult(uuid())
        # subscribes, and the backend only keeps a weak reference to it
        result.then(lambda *args: None, weak=True)
        Cycle(result)


def run(i):
    drop_pending_results(int(dropped))
    result = app.send_task("t.integration.tasks.add", args=(i, i), queue=queue)
    return result.get(timeout=float(result_timeout))


if concurrency == "gevent":
    import gevent
    jobs = [gevent.spawn(run, i) for i in range(int(tasks))]
    gevent.joinall(jobs, raise_error=True)
    values = [job.value for job in jobs]
else:
    values = [run(i) for i in range(int(tasks))]
assert values == [i + i for i in range(int(tasks))], values

gc.collect()
app.backend.result_consumer.drain_events(timeout=0.1)
assert not overlapping, f"overlapping pubsub calls: {overlapping}"

# every channel is released on the server once its result is gone.
client = app.backend.client
deadline = time.monotonic() + 10
while client.pubsub_channels("celery-task-meta-*"):
    assert time.monotonic() < deadline, client.pubsub_channels("celery-task-meta-*")
    time.sleep(0.1)
print("ok")
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


class test_redis_backend_reentrant:
    @pytest.mark.parametrize("concurrency", ["threads", "gevent"])
    def test_results_collected_during_pubsub_calls(self, celery_setup: CeleryTestSetup, concurrency: str):
        if concurrency == "gevent":
            pytest.importorskip("gevent")
        conf = celery_setup.app.conf
        args = [
            concurrency,
            conf.broker_url,
            conf.result_backend,
            celery_setup.worker.worker_queue,
            str(TASKS),
            str(DROPPED_PER_TASK),
            str(RESULT_TIMEOUT),
        ]
        try:
            client = subprocess.run(
                [sys.executable, "-c", CLIENT, *args],
                capture_output=True,
                text=True,
                timeout=RESULT_TIMEOUT * 2,
            )
        except subprocess.TimeoutExpired:
            pytest.fail(f"client did not finish in {RESULT_TIMEOUT * 2}s: pubsub deadlock")

        assert client.returncode == 0, client.stderr
        assert client.stdout.strip() == "ok"
