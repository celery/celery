"""Smoke test for acknowledgements sent during a prefork warm shutdown.

Regression coverage for https://github.com/celery/celery/issues/3802:
with ``task_acks_late`` the ack of a task that finishes while the pool is
being joined used to sit in the event loop's ready queue until
``hub.close()``, which only runs after the join. A worker killed before a
longer task completed therefore got every task finished during the drain
redelivered. The test runs against a real RabbitMQ broker and reads the
queue depth from its management API after the kill.
"""

from __future__ import annotations

from time import sleep

import pytest
from pytest_celery import RABBITMQ_PORTS, RESULT_TIMEOUT, CeleryBrokerCluster, CeleryTestSetup, RabbitMQContainer
from tenacity import retry, stop_after_attempt, wait_fixed

from celery import Celery
from t.smoke.conftest import RabbitMQManagementBroker, SuiteOperations, WorkerKill
from t.smoke.tasks import long_running_task


@pytest.fixture
def default_rabbitmq_broker_image() -> str:
    return "rabbitmq:management"


@pytest.fixture
def default_rabbitmq_broker_ports() -> dict:
    ports = RABBITMQ_PORTS.copy()
    ports.update({"15672/tcp": None})
    return ports


@pytest.fixture
def celery_rabbitmq_broker(default_rabbitmq_broker: RabbitMQContainer) -> RabbitMQManagementBroker:
    broker = RabbitMQManagementBroker(default_rabbitmq_broker)
    yield broker
    broker.teardown()


@pytest.fixture
def celery_broker_cluster(celery_rabbitmq_broker: RabbitMQManagementBroker) -> CeleryBrokerCluster:
    # the queue depth after the kill is read from the RabbitMQ management API
    cluster = CeleryBrokerCluster(celery_rabbitmq_broker)
    yield cluster
    cluster.teardown()


@retry(stop=stop_after_attempt(RESULT_TIMEOUT), wait=wait_fixed(1), reraise=True)
def wait_for_requeue(broker: RabbitMQManagementBroker, queue: str) -> dict:
    """Poll until the broker has noticed the dead connection and requeued
    what the worker never acked."""
    counts = broker.get_queue_messages(queue)
    assert counts["messages_ready"] >= 1, counts
    return counts


class test_acks_during_warm_shutdown(SuiteOperations):
    @pytest.fixture
    def default_worker_app(self, default_worker_app: Celery) -> Celery:
        app = default_worker_app
        app.conf.task_acks_late = True
        app.conf.worker_concurrency = 2
        app.conf.worker_prefetch_multiplier = 1
        return app

    def test_task_finished_during_drain_is_acked_before_kill(self, celery_setup: CeleryTestSetup):
        queue = celery_setup.worker.worker_queue
        worker = celery_setup.worker
        broker: RabbitMQManagementBroker = celery_setup.broker

        long_task = long_running_task.si(420, verbose=True).set(queue=queue).delay()
        short_task = long_running_task.si(3).set(queue=queue).delay()
        worker.assert_log_exists(f"long_running_task[{long_task.id}] received")
        worker.assert_log_exists(f"long_running_task[{short_task.id}] received")
        worker.assert_log_exists("Sleeping: 0")

        self.kill_worker(worker, WorkerKill.Method.SIGTERM)
        worker.assert_log_exists("worker: Warm shutdown (MainProcess)")
        worker.assert_log_exists(f"long_running_task[{short_task.id}] succeeded")
        assert short_task.get(RESULT_TIMEOUT)
        # the shutdown timer thread runs the queued acks every 0.5 seconds
        sleep(2)

        self.kill_worker(worker, WorkerKill.Method.DOCKER_KILL)

        counts = wait_for_requeue(broker, queue)
        assert counts["messages"] == 1, (
            f"only the task still running at the kill should be redelivered, got {counts}"
        )
