from datetime import timedelta
from datetime import timezone as datetime_timezone

import pytest
import requests
from future.backports.datetime import datetime
from pytest_celery import CeleryTestSetup
from requests.auth import HTTPBasicAuth

from celery import Celery
from t.smoke.tasks import noop
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

    def test_native_delayed_delivery_exchange_configuration(self, exchanges: list, celery_setup: CeleryTestSetup):
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
        celery_setup: CeleryTestSetup,
        default_worker_app: Celery
    ):
        queue_configuration_test_helper(celery_setup, queues)

    def test_native_delayed_delivery_exchange_configuration(self, exchanges: list, celery_setup: CeleryTestSetup):
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
