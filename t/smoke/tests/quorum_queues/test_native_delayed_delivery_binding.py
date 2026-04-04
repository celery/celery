"""Smoke tests for native delayed delivery queue binding.

Verifies queue bindings are created correctly for native delayed delivery.

See https://github.com/celery/celery/issues/9960
"""
from __future__ import annotations

import requests
from pytest_celery import CeleryTestSetup
from requests.auth import HTTPBasicAuth

from t.smoke.tests.quorum_queues.conftest import RabbitMQManagementBroker


class test_native_delayed_delivery_binding:
    """Verify delayed delivery bindings are created on worker startup."""

    def test_worker_creates_delayed_delivery_bindings(self, celery_setup: CeleryTestSetup):
        """Consumed queue gets delayed delivery bindings from the worker."""
        broker: RabbitMQManagementBroker = celery_setup.broker
        api = broker.get_management_url() + "/api"
        auth = HTTPBasicAuth("guest", "guest")

        # Check celery_delayed_delivery exchange has bindings
        response = requests.get(
            f"{api}/exchanges/%2F/celery_delayed_delivery/bindings/source",
            auth=auth,
        )
        response.raise_for_status()
        bindings = response.json()

        queue = celery_setup.worker.worker_queue
        queue_bindings = [
            b for b in bindings
            if queue in b.get("routing_key", "")
        ]
        assert len(queue_bindings) >= 1, (
            f"Expected delayed delivery binding for {queue!r}, "
            f"got bindings: {bindings!r}"
        )
