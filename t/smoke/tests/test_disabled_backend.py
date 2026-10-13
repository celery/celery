"""Smoke test for DisabledBackend chain failure handling.

Regression test for issue where _fail_chain() called get_state() for plain chain
elements, causing NotImplementedError with DisabledBackend.
"""

from __future__ import annotations

import uuid
from time import monotonic, sleep

import pytest
from pytest_celery import RESULT_TIMEOUT, CeleryTestSetup

from celery.canvas import chain
from t.integration.conftest import get_redis_connection
from t.integration.tasks import fail, identity, redis_echo


@pytest.fixture
def celery_backend_cluster() -> None:
    """No backend cluster needed for DisabledBackend."""
    return None


@pytest.fixture
def celery_setup_config(celery_setup_config: dict) -> dict:
    """Disable result backend for testing DisabledBackend behavior.

    Configures the client-side app to use DisabledBackend.
    """
    celery_setup_config["result_backend"] = "disabled://"
    return celery_setup_config


@pytest.fixture
def default_worker_app(default_worker_app) -> object:
    """Configure worker to use DisabledBackend.

    Direct attribute assignment lands in conf.changes, which is what
    pytest-celery serializes into the worker container config; the
    celery_setup_config dict alone only configures the client side.
    """
    default_worker_app.conf.result_backend = "disabled://"
    return default_worker_app


class test_disabled_backend:
    def test_chain_failure_with_disabled_backend(self, celery_setup: CeleryTestSetup):
        """Regression test: plain chain failure with DisabledBackend can execute link_error callback.

        Verifies that when a plain chain fails with DisabledBackend, the link_error
        callback is still called, proving _fail_chain() doesn't raise NotImplementedError.
        """
        redis_key = str(uuid.uuid4())
        queue = celery_setup.worker.worker_queue

        # Create a chain where the first (non-final) task fails
        sig = chain(
            fail.si().set(queue=queue),
            identity.si("should_not_run").set(queue=queue),
        )

        # Add a link_error callback that writes to Redis
        errback_msg = "failure_handled"
        sig.link_error(redis_echo.si(errback_msg, redis_key).set(queue=queue))

        # Apply the chain (res.get() is not used because DisabledBackend raises NotImplementedError)
        sig.apply_async()

        # Wait for the worker to process and the callback to execute
        redis_connection = get_redis_connection()
        deadline = monotonic() + RESULT_TIMEOUT
        actual = []

        try:
            while monotonic() < deadline:
                actual = redis_connection.lrange(redis_key, 0, -1)
                if actual:
                    break
                sleep(0.1)

            assert actual == [b"failure_handled"]
        finally:
            redis_connection.delete(redis_key)
