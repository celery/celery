"""Smoke tests for default queue type fallback behavior.

Tests that Celery respects task_default_exchange_type and
task_default_queue_type when using implicit routing.
"""
from __future__ import annotations

import pytest
import requests
from pytest_celery import RESULT_TIMEOUT, CeleryTestSetup
from requests.auth import HTTPBasicAuth

from celery import Celery
from t.smoke.tasks import noop
from t.smoke.tests.quorum_queues.conftest import RabbitMQManagementBroker


class test_default_queue_type_fallback:
    """Verify Celery honors default exchange/queue type settings."""

    @pytest.fixture
    def default_worker_app(self, default_worker_app: Celery) -> Celery:
        app = default_worker_app
        app.conf.task_default_exchange_type = "topic"
        app.conf.task_default_routing_key = "celery"
        return app

    @pytest.mark.xfail(
        reason=(
            "Celery does not respect task_default_exchange_type/queue_type "
            "when using implicit routing to the 'celery' queue. It creates "
            "a classic queue and direct exchange instead."
        ),
        strict=False,
    )
    def test_fallback_to_correct_exchange_type(self, celery_setup: CeleryTestSetup):
        """Default exchange type should be topic, not direct."""
        queue = celery_setup.worker.worker_queue
        result = noop.s().set(queue=queue).delay()
        assert result.get(timeout=RESULT_TIMEOUT) is None

        broker: RabbitMQManagementBroker = celery_setup.broker
        api = broker.get_management_url() + "/api"
        auth = HTTPBasicAuth("guest", "guest")

        response = requests.get(f"{api}/exchanges/%2F/celery/", auth=auth)
        response.raise_for_status()
        exchange_info = response.json()

        assert exchange_info["type"] != "direct", (
            "Expected Celery to honor task_default_exchange_type, "
            f"but got: {exchange_info['type']}"
        )
