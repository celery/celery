from __future__ import annotations

import pytest
from pytest_celery import CeleryBrokerCluster, CeleryTestSetup, RabbitMQTestBroker
from tenacity import retry, stop_after_attempt, wait_fixed

from celery import Celery

HEARTBEAT = 10


# gets the connection status from the rabbitmq side.
def list_connections(broker: RabbitMQTestBroker) -> dict[str, int]:
    _, output = broker.container.exec_run(["rabbitmqctl", "list_connections", "name", "timeout", "--quiet"])
    connections = {}
    for line in output.decode().splitlines():
        name, _, timeout = line.rpartition("\t")
        if timeout == "timeout":
            continue
        connections[name] = int(timeout)
    return connections


@pytest.fixture
def celery_broker_cluster(celery_rabbitmq_broker: RabbitMQTestBroker) -> CeleryBrokerCluster:
    cluster = CeleryBrokerCluster(celery_rabbitmq_broker)
    yield cluster
    cluster.teardown()


@pytest.fixture
def celery_backend_cluster() -> None:
    return None


@pytest.fixture
def default_worker_app(default_worker_app: Celery) -> Celery:
    app = default_worker_app
    app.conf.broker_heartbeat = HEARTBEAT
    return app


class test_worker_heartbeats:

    # asserts the main connection and the event dispatcher connection
    # get heartbeat working; there is a third connection which has no
    # heartbeat for now.
    def test_dead_worker_connections_are_closed(self, celery_setup: CeleryTestSetup):
        broker = celery_setup.broker

        @retry(stop=stop_after_attempt(30), wait=wait_fixed(1), reraise=True)
        def worker_connections() -> dict[str, int]:
            connections = list_connections(broker)
            assert len(connections) == 3
            return connections

        connections = worker_connections()
        with_heartbeat = {
            name for name, timeout in connections.items()
            if timeout == HEARTBEAT
        }
        no_heartbeat = {
            name for name, timeout in connections.items()
            if timeout == 0
        }
        assert len(with_heartbeat) == 2
        assert len(no_heartbeat) == 1

        # both the main connection and the event dispatcher connection
        # will go away with the heartbeat.
        celery_setup.worker.container.kill(signal="SIGSTOP")
        try:
            @retry(stop=stop_after_attempt(HEARTBEAT * 5), wait=wait_fixed(1), reraise=True)
            def assert_only_the_no_heartbeat_connection_is_left():
                assert set(list_connections(broker)) == no_heartbeat

            assert_only_the_no_heartbeat_connection_is_left()
        finally:
            celery_setup.worker.container.kill(signal="SIGCONT")
