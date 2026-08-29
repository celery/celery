import requests
from pytest_celery import RESULT_TIMEOUT, CeleryTestSetup
from requests.auth import HTTPBasicAuth

from celery.canvas import group
from t.integration.tasks import add, identity
from t.smoke.tests.quorum_queues.conftest import RabbitMQManagementBroker


class test_broker_configuration:
    def test_queue_type(self, celery_setup: CeleryTestSetup):
        broker: RabbitMQManagementBroker = celery_setup.broker
        api = broker.get_management_url() + "/api/queues"
        response = requests.get(api, auth=HTTPBasicAuth("guest", "guest"))
        assert response.status_code == 200
        res = response.json()
        assert isinstance(res, list)
        worker_queue = next((queue for queue in res if queue["name"] == celery_setup.worker.worker_queue), None)
        assert worker_queue is not None, f'"{celery_setup.worker.worker_queue}" queue not found'
        queue_type = worker_queue.get("type")
        assert queue_type == "quorum", f'"{celery_setup.worker.worker_queue}" queue is not a quorum queue'


class test_quorum_queues:
    def test_signature(self, celery_setup: CeleryTestSetup):
        sig = identity.si("test_signature").set(queue=celery_setup.worker.worker_queue)
        assert sig.delay().get(timeout=RESULT_TIMEOUT) == "test_signature"

    def test_group(self, celery_setup: CeleryTestSetup):
        sig = group(
            group(add.si(1, 1), add.si(2, 2)),
            group([add.si(1, 1), add.si(2, 2)]),
            group(s for s in [add.si(1, 1), add.si(2, 2)]),
        )
        res = sig.apply_async(queue=celery_setup.worker.worker_queue)
        assert res.get(timeout=RESULT_TIMEOUT) == [2, 4, 2, 4, 2, 4]
