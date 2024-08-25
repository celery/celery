from datetime import timedelta
from datetime import timezone as datetime_timezone

import pytest
from future.backports.datetime import datetime
from pytest_celery import CeleryTestSetup

from celery import Celery
from t.smoke.tasks import noop


class test_native_delayed_delivery:
    @pytest.fixture
    def default_worker_app(self, default_worker_app: Celery) -> Celery:
        app = default_worker_app
        app.conf.broker_transport_options = {"confirm_publish": True}
        app.conf.task_default_queue_type = "quorum"
        app.conf.broker_native_delayed_delivery = True
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
