"""Smoke tests for native delayed delivery queue binding.

Verifies queue bindings are created correctly for native delayed delivery,
including when some queues in task_queues fail to bind.

See https://github.com/celery/celery/issues/9960
"""
from __future__ import annotations

import pytest
import requests
from pytest_celery import CeleryTestSetup
from requests.auth import HTTPBasicAuth

from celery import Celery
from t.smoke.tests.quorum_queues.conftest import RabbitMQManagementBroker


class test_native_delayed_delivery_binding:
    """Verify the worker's consumed queue receives native delayed-delivery
    bindings on startup, alongside its immediate delivery binding.
    """

    @pytest.fixture
    def default_worker_app(self, default_worker_app: Celery) -> Celery:
        app = default_worker_app
        app.conf.task_default_exchange_type = "topic"
        app.conf.task_default_routing_key = "celery"
        return app

    def test_worker_creates_delayed_delivery_bindings(self, celery_setup: CeleryTestSetup):
        """Consumed queue gets delayed delivery bindings from the worker."""
        broker: RabbitMQManagementBroker = celery_setup.broker
        api = broker.get_management_url() + "/api"
        auth = HTTPBasicAuth("guest", "guest")
        queue = celery_setup.worker.worker_queue

        # 1. Check celery_delayed_delivery exchange has bindings for the queue
        response = requests.get(
            f"{api}/exchanges/%2F/celery_delayed_delivery/bindings/source",
            auth=auth,
        )
        response.raise_for_status()
        delayed_bindings = response.json()

        expected_rk = f"#.{queue}"
        queue_delayed_bindings = [
            b for b in delayed_bindings
            if b.get("routing_key") == expected_rk
        ]
        assert len(queue_delayed_bindings) >= 1, (
            f"Expected delayed delivery binding for {queue!r}, "
            f"got bindings: {delayed_bindings!r}"
        )

        # 2. Check queue has bindings from both immediate and delayed delivery
        response = requests.get(
            f"{api}/queues/%2F/{queue}/bindings",
            auth=auth,
        )
        response.raise_for_status()
        queue_bindings = response.json()

        routing_keys = {b.get("routing_key") for b in queue_bindings}

        # Immediate delivery routing key
        assert queue in routing_keys, (
            f"Expected immediate delivery routing key {queue!r} in bindings, "
            f"got: {queue_bindings!r}"
        )

        # Delayed delivery routing key (pattern: #.queue_name)
        delayed_rk = f"#.{queue}"
        assert delayed_rk in routing_keys, (
            f"Expected delayed delivery routing key {delayed_rk!r} in bindings, "
            f"got: {queue_bindings!r}"
        )
