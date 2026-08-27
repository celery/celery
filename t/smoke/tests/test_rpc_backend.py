"""Smoke tests for the RPC result backend.

Regression coverage for https://github.com/celery/celery/issues/4830:
polling the RPC backend for a completed task used to requeue the final
result message instead of acking it, so final results recirculated on
the reply queue on every state poll. These tests run against a real
RabbitMQ broker and verify both the result round-trip and that repeated
polling leaves no messages behind on the broker.
"""

from __future__ import annotations

import base64
import json
import urllib.request

import pytest
from pytest_celery import (RABBITMQ_PORTS, RESULT_TIMEOUT, CeleryBrokerCluster, CeleryTestSetup, RabbitMQContainer,
                           RabbitMQTestBroker)
from tenacity import retry, stop_after_attempt, wait_fixed

from celery import Celery, states


class RabbitMQManagementBroker(RabbitMQTestBroker):
    """RabbitMQ broker with the management API exposed.

    Used to inspect queue depths directly, which is how recirculating
    result messages are detected.
    """

    def get_management_url(self) -> str:
        ports = self.container.attrs["NetworkSettings"]["Ports"]
        ip = ports["15672/tcp"][0]["HostIp"]
        port = ports["15672/tcp"][0]["HostPort"]
        return f"http://{ip}:{port}"

    def get_total_ready_messages(self) -> int:
        """Total messages sitting ready on all queues of the default vhost."""
        url = f"{self.get_management_url()}/api/queues/%2F"
        request = urllib.request.Request(url)
        credentials = base64.b64encode(b"guest:guest").decode()
        request.add_header("Authorization", f"Basic {credentials}")
        with urllib.request.urlopen(request, timeout=10) as response:
            queues = json.loads(response.read())
        return sum(queue.get("messages_ready", 0) for queue in queues)


@pytest.fixture
def default_rabbitmq_broker_image() -> str:
    return "rabbitmq:management"


@pytest.fixture
def default_rabbitmq_broker_ports() -> dict:
    # expose the management UI/API port as well
    ports = RABBITMQ_PORTS.copy()
    ports.update({"15672/tcp": None})
    return ports


@pytest.fixture
def celery_rabbitmq_broker(default_rabbitmq_broker: RabbitMQContainer) -> RabbitMQManagementBroker:
    broker = RabbitMQManagementBroker(default_rabbitmq_broker)
    yield broker
    broker.teardown()


@pytest.fixture
def celery_broker_cluster(celery_rabbitmq_broker: RabbitMQTestBroker) -> CeleryBrokerCluster:
    # the RPC result backend only works on top of the AMQP broker
    cluster = CeleryBrokerCluster(celery_rabbitmq_broker)
    yield cluster
    cluster.teardown()


@pytest.fixture
def celery_backend_cluster() -> None:
    # the RPC backend stores results in the AMQP broker itself,
    # so no dedicated backend container is needed.
    return None


@pytest.fixture
def celery_setup_config(celery_setup_config: dict) -> dict:
    celery_setup_config["result_backend"] = "rpc://"
    return celery_setup_config


@pytest.fixture
def default_worker_app(default_worker_app: Celery) -> Celery:
    app = default_worker_app
    app.conf.worker_prefetch_multiplier = 1
    app.conf.worker_concurrency = 1
    # direct attribute assignment lands in conf.changes, which is what
    # pytest-celery serializes into the worker container config; the
    # celery_setup_config dict alone only configures the client side.
    app.conf.result_backend = "rpc://"
    return app


class test_rpc_backend:
    def test_sanity(self, celery_setup: CeleryTestSetup):
        assert celery_setup.app.conf.result_backend == "rpc://"
        res = celery_setup.app.send_task(
            "t.integration.tasks.identity",
            args=("test_sanity",),
            queue=celery_setup.worker.worker_queue,
        )
        assert res.get(RESULT_TIMEOUT) == "test_sanity"

    def test_repeated_final_polls_do_not_recirculate(self, celery_setup: CeleryTestSetup):
        # regression test for #4830: polling many completed tasks must
        # leave no result messages behind on the broker.
        app = celery_setup.app
        queue = celery_setup.worker.worker_queue

        # publish through the setup app itself so the result reply queue
        # is the same queue app.backend polls below.
        results = [
            app.send_task("t.integration.tasks.add", args=(i, i), queue=queue)
            for i in range(3)
        ]

        @retry(
            stop=stop_after_attempt(int(RESULT_TIMEOUT)),
            wait=wait_fixed(1),
            reraise=True,
        )
        def poll_until_ready(task_id):
            meta = app.backend.get_task_meta(task_id)
            assert meta["status"] in states.READY_STATES, meta
            return meta

        # poll each task through the backend until it completes: this is
        # the exact poll path that used to requeue the final result
        # message on every call.
        for i, res in enumerate(results):
            meta = poll_until_ready(res.id)
            assert meta["status"] == states.SUCCESS
            assert meta["result"] == i + i

        # further polls are served from the cache without putting the
        # final result message back on the reply queue.
        for res in results:
            for _ in range(5):
                meta = app.backend.get_task_meta(res.id)
                assert meta["status"] == states.SUCCESS, meta

        broker = celery_setup.broker

        @retry(
            stop=stop_after_attempt(30),
            wait=wait_fixed(0.2),
            reraise=True,
        )
        def assert_broker_drained():
            # before the fix every poll requeued the final message, so
            # the reply queue never stayed empty here.
            assert broker.get_total_ready_messages() == 0

        assert_broker_drained()
