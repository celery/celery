import time
from unittest.mock import Mock

import pytest
from billiard.einfo import ExceptionInfo
from celery import Celery
from celery.exceptions import WorkerLostError
from celery.worker.request import Request


@pytest.fixture
def failing_app():
    # Point at a closed port so connection attempts fail immediately.
    app = Celery(
        broker='memory://',
        backend='redis://localhost:1/0'  # Port 1 is almost always closed
    )
    app.conf.result_backend_always_retry = True
    app.conf.result_backend_max_retries = float('inf')
    app.conf.result_backend_base_sleep_between_retries_ms = 10
    app.conf.result_backend_max_sleep_between_retries_ms = 10
    return app


def test_on_failure_returns_fast_with_unreachable_backend(failing_app):
    """
    Integration test: a real backend connection failure should not block
    on_failure() indefinitely. With the bounded retry, it should fail fast.
    """
    @failing_app.task(bind=True, acks_late=True, reject_on_worker_lost=True)
    def dummy_task(self):
        pass

    try:
        raise WorkerLostError("Worker exited prematurely")
    except WorkerLostError:
        einfo = ExceptionInfo(internal=True)

    # Create a mock message object that mimics Kombu's Message
    message = Mock()
    message.headers = {'id': 'test-id', 'task': dummy_task.name}
    message.body = b'[]'
    message.content_type = 'application/json'
    message.content_encoding = 'utf-8'
    message.payload = ([], {}, {})
    message.delivery_info = {}
    message.properties = {}

    # Construct a minimal Request
    req = Request(
        message=message,
        app=failing_app,
        task=dummy_task,
        on_ack=Mock(),
        on_reject=Mock(),
    )

    start_time = time.monotonic()
    req.on_failure(einfo, send_failed_event=False)
    elapsed = time.monotonic() - start_time

    # On main this hangs indefinitely (in _success_was_stored);
    # here it should fail fast and requeue.
    assert elapsed < 2.0, (
        f"on_failure took {elapsed:.2f}s. It should fail fast "
        f"instead of retrying indefinitely with max_retries=inf."
    )
