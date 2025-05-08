import time
from datetime import datetime, timedelta
from datetime import timezone as datetime_timezone

import pytest
import requests
from pytest_celery import CeleryTestSetup
from requests.auth import HTTPBasicAuth

from celery import Celery, chain
from t.smoke.tasks import add, noop
from t.smoke.tests.quorum_queues.conftest import RabbitMQManagementBroker


@pytest.fixture
def queues(celery_setup: CeleryTestSetup) -> list:
    broker: RabbitMQManagementBroker = celery_setup.broker
    api = broker.get_management_url() + "/api/queues"
    response = requests.get(api, auth=HTTPBasicAuth("guest", "guest"))
    assert response.status_code == 200

    queues = response.json()
    assert isinstance(queues, list)

    return queues


@pytest.fixture
def exchanges(celery_setup: CeleryTestSetup) -> list:
    broker: RabbitMQManagementBroker = celery_setup.broker
    api = broker.get_management_url() + "/api/exchanges"
    response = requests.get(api, auth=HTTPBasicAuth("guest", "guest"))
    assert response.status_code == 200

    exchanges = response.json()
    assert isinstance(exchanges, list)

    return exchanges


def queue_configuration_test_helper(celery_setup, queues):
    res = [queue for queue in queues if queue["name"].startswith('celery_delayed')]
    assert len(res) == 28
    for queue in res:
        queue_level = int(queue["name"].split("_")[-1])

        queue_arguments = queue["arguments"]
        if queue_level == 0:
            assert queue_arguments["x-dead-letter-exchange"] == "celery_delayed_delivery"
        else:
            assert queue_arguments["x-dead-letter-exchange"] == f"celery_delayed_{queue_level - 1}"

        assert queue_arguments["x-message-ttl"] == pow(2, queue_level) * 1000

        conf = celery_setup.app.conf
        assert queue_arguments["x-queue-type"] == conf.broker_native_delayed_delivery_queue_type


def exchange_configuration_test_helper(exchanges):
    res = [exchange for exchange in exchanges if exchange["name"].startswith('celery_delayed')]
    assert len(res) == 29
    for exchange in res:
        assert exchange["type"] == "topic"


class test_broker_configuration_quorum:
    @pytest.fixture
    def default_worker_app(self, default_worker_app: Celery) -> Celery:
        app = default_worker_app
        app.conf.broker_transport_options = {"confirm_publish": True}
        app.conf.task_default_queue_type = "quorum"
        app.conf.broker_native_delayed_delivery_queue_type = 'quorum'
        app.conf.task_default_exchange_type = 'topic'
        app.conf.task_default_routing_key = 'celery'

        return app

    def test_native_delayed_delivery_queue_configuration(
        self,
        queues: list,
        celery_setup: CeleryTestSetup
    ):
        queue_configuration_test_helper(celery_setup, queues)

    def test_native_delayed_delivery_exchange_configuration(self, exchanges: list):
        exchange_configuration_test_helper(exchanges)


class test_broker_configuration_classic:
    @pytest.fixture
    def default_worker_app(self, default_worker_app: Celery) -> Celery:
        app = default_worker_app
        app.conf.broker_transport_options = {"confirm_publish": True}
        app.conf.task_default_queue_type = "quorum"
        app.conf.broker_native_delayed_delivery_queue_type = 'classic'
        app.conf.task_default_exchange_type = 'topic'
        app.conf.task_default_routing_key = 'celery'

        return app

    def test_native_delayed_delivery_queue_configuration(
        self,
        queues: list,
        celery_setup: CeleryTestSetup
    ):
        queue_configuration_test_helper(celery_setup, queues)

    def test_native_delayed_delivery_exchange_configuration(self, exchanges: list):
        exchange_configuration_test_helper(exchanges)


class test_native_delayed_delivery:
    @pytest.fixture
    def default_worker_app(self, default_worker_app: Celery) -> Celery:
        app = default_worker_app
        app.conf.broker_transport_options = {"confirm_publish": True}
        app.conf.task_default_queue_type = "quorum"
        app.conf.task_default_exchange_type = 'topic'
        app.conf.task_default_routing_key = 'celery'

        return app

    def test_countdown(self, celery_setup: CeleryTestSetup):
        s = noop.s().set(queue=celery_setup.worker.worker_queue)

        result = s.apply_async(countdown=5)

        result.get(timeout=10)

    def test_eta(self, celery_setup: CeleryTestSetup):
        s = noop.s().set(queue=celery_setup.worker.worker_queue)

        result = s.apply_async(eta=datetime.now(datetime_timezone.utc) + timedelta(0, 5))

        result.get(timeout=10)

    def test_eta_str(self, celery_setup: CeleryTestSetup):
        s = noop.s().set(queue=celery_setup.worker.worker_queue)

        result = s.apply_async(eta=(datetime.now(datetime_timezone.utc) + timedelta(0, 5)).isoformat())

        result.get(timeout=10)

    def test_eta_in_the_past(self, celery_setup: CeleryTestSetup):
        s = noop.s().set(queue=celery_setup.worker.worker_queue)

        result = s.apply_async(eta=(datetime.now(datetime_timezone.utc) - timedelta(0, 5)).isoformat())

        result.get(timeout=10)

    def test_long_delay(self, celery_setup: CeleryTestSetup, queues: list):
        """Test task with a delay longer than 24 hours."""
        s = noop.s().set(queue=celery_setup.worker.worker_queue)
        future_time = datetime.now(datetime_timezone.utc) + timedelta(hours=25)
        result = s.apply_async(eta=future_time)

        assert result.status == "PENDING", (
            f"Task should be PENDING but was {result.status}"
        )
        assert result.ready() is False, (
            "Task with future ETA should not be ready"
        )

    def test_multiple_tasks_same_eta(self, celery_setup: CeleryTestSetup):
        """Test multiple tasks scheduled for the same time."""
        s = noop.s().set(queue=celery_setup.worker.worker_queue)
        future_time = datetime.now(datetime_timezone.utc) + timedelta(seconds=5)

        results = [
            s.apply_async(eta=future_time)
            for _ in range(5)
        ]

        for result in results:
            result.get(timeout=10)
            assert result.status == "SUCCESS"

    def test_multiple_tasks_different_delays(self, celery_setup: CeleryTestSetup):
        """Test multiple tasks with different delay times."""
        s = noop.s().set(queue=celery_setup.worker.worker_queue)
        now = datetime.now(datetime_timezone.utc)

        results = [
            s.apply_async(eta=now + timedelta(seconds=delay))
            for delay in (2, 4, 6)
        ]

        completion_times = []
        for result in results:
            result.get(timeout=10)
            completion_times.append(datetime.now(datetime_timezone.utc))

        for i in range(1, len(completion_times)):
            assert completion_times[i] > completion_times[i-1], (
                f"Task {i} completed at {completion_times[i]} which is not after "
                f"task {i-1} completed at {completion_times[i-1]}"
            )

    def test_revoke_delayed_task(self, celery_setup: CeleryTestSetup):
        """Test revoking a delayed task before it executes."""
        s = noop.s().set(queue=celery_setup.worker.worker_queue)
        result = s.apply_async(countdown=10)

        assert result.status == "PENDING"
        result.revoke()

        time.sleep(12)
        assert result.status == "REVOKED"

    def test_chain_with_delays(self, celery_setup: CeleryTestSetup):
        """Test chain of tasks with delays between them."""
        c = chain(
            add.s(1, 2).set(countdown=2),
            add.s(3).set(countdown=2),
            add.s(4).set(countdown=2)
        ).set(queue=celery_setup.worker.worker_queue)

        result = c()
        assert result.get(timeout=15) == 10

    def test_zero_delay(self, celery_setup: CeleryTestSetup):
        """Test task with zero delay/countdown."""
        s = noop.s().set(queue=celery_setup.worker.worker_queue)

        result = s.apply_async(countdown=0)
        result.get(timeout=10)
        assert result.status == "SUCCESS"

    def test_negative_countdown(self, celery_setup: CeleryTestSetup):
        """Test task with negative countdown (should execute immediately)."""
        s = noop.s().set(queue=celery_setup.worker.worker_queue)

        result = s.apply_async(countdown=-5)
        result.get(timeout=10)
        assert result.status == "SUCCESS"

    def test_very_short_delay(self, celery_setup: CeleryTestSetup):
        """Test task with very short delay (1 second)."""
        s = noop.s().set(queue=celery_setup.worker.worker_queue)

        result = s.apply_async(countdown=1)
        result.get(timeout=10)
        assert result.status == "SUCCESS"

    def test_concurrent_delayed_tasks(self, celery_setup: CeleryTestSetup):
        """Test many concurrent delayed tasks."""
        s = noop.s().set(queue=celery_setup.worker.worker_queue)
        future_time = datetime.now(datetime_timezone.utc) + timedelta(seconds=2)

        results = [
            s.apply_async(eta=future_time)
            for _ in range(100)
        ]

        for result in results:
            result.get(timeout=10)
            assert result.status == "SUCCESS"
