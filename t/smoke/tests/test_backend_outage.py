"""A short result backend outage doesn't lose tasks or spin the worker (#8588)."""
from __future__ import annotations

from time import monotonic, sleep

import pytest
from pytest_celery import (CeleryBackendCluster, CeleryBrokerCluster, CeleryTestSetup, RabbitMQTestBroker,
                           RedisTestBackend)

from celery import Celery, states, uuid
from t.integration.tasks import get_redis_connection, redis_echo

TASKS = 5
# Shorter than result_backend_store_retry_timeout, 20 s by default.
OUTAGE = 15


@pytest.fixture
def celery_broker_cluster(celery_rabbitmq_broker: RabbitMQTestBroker) -> CeleryBrokerCluster:
    cluster = CeleryBrokerCluster(celery_rabbitmq_broker)
    yield cluster
    cluster.teardown()


@pytest.fixture
def celery_backend_cluster(celery_redis_backend: RedisTestBackend) -> CeleryBackendCluster:
    cluster = CeleryBackendCluster(celery_redis_backend)
    yield cluster
    cluster.teardown()


@pytest.fixture
def default_worker_app(default_worker_app: Celery) -> Celery:
    app = default_worker_app
    app.conf.task_acks_late = True
    app.conf.task_track_started = True
    app.conf.worker_prefetch_multiplier = 1
    app.conf.worker_concurrency = 1
    return app


def run_tasks_through_backend_outage(celery_setup: CeleryTestSetup) -> None:
    key = f'backend-outage-{uuid()}'
    # Sending needs no result backend, so this client has none.
    client = Celery('client', broker=celery_setup.app.conf.broker_url, set_as_current=False)
    client.conf.task_default_queue_type = celery_setup.app.conf.task_default_queue_type
    celery_setup.backend.kill()
    try:
        ids = [
            client.send_task(redis_echo.name, (str(i),), {'redis_key': key},
                             queue=celery_setup.worker.worker_queue).id
            for i in range(TASKS)
        ]
        sleep(OUTAGE)
    finally:
        celery_setup.backend.restart()

    redis = get_redis_connection()
    deadline = monotonic() + 30
    while redis.llen(key) < TASKS and monotonic() < deadline:
        sleep(0.5)
    ran = sorted(value.decode() for value in redis.lrange(key, 0, -1))
    assert ran == [str(i) for i in range(TASKS)]

    # The backend's host port changes when it restarts.
    reader = Celery('reader', backend=celery_setup.backend.config()['host_url'], set_as_current=False)
    assert [reader.AsyncResult(i).state for i in ids] == [states.SUCCESS] * TASKS

    logs = celery_setup.worker.logs()
    receipts = sum(logs.count(f'[{task_id}] received') for task_id in ids)
    # Waiting out the outage shouldn't hand each task back hundreds of times.
    assert receipts <= 3 * TASKS


class test_backend_outage:
    def test_tasks_survive_a_short_backend_outage(self, celery_setup: CeleryTestSetup):
        run_tasks_through_backend_outage(celery_setup)
