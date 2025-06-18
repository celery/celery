# t/integration/test_rabbitmq_default_queue_type_fallback.py

import time

import pytest
from kombu import Connection

from celery import Celery


@pytest.fixture()
def app():
    return Celery(
        "test_app",
        broker="amqp://guest:guest@rabbit:5672//",
        backend="redis://redis:6379/0",
        include=["t.integration.test_rabbitmq_default_queue_type_fallback"],
    )


@pytest.fixture()
def ping(app):
    @app.task(name="ping")
    def ping():
        return "pong"
    return ping


@pytest.mark.amqp
@pytest.mark.timeout(30)
@pytest.mark.xfail(
    reason=(
        "Celery does not respect task_default_exchange_type/queue_type "
        "when using implicit routing to the 'celery' queue. It creates "
        "a classic queue and direct exchange instead."
    ),
    strict=True,
)
def test_fallback_to_classic_queue_and_direct_exchange(app, ping):
    from celery.contrib.testing.worker import start_worker

    # Start worker and submit task
    with start_worker(app, queues=["celery"], loglevel="info", perform_ping_check=False):
        result = ping.delay()
        assert result.get(timeout=10) == "pong"

        exchange_type = None
        start_time = time.time()
        timeout = 10  # Maximum wait time in seconds

        while time.time() - start_time < timeout:
            with Connection(app.conf.broker_url) as conn:
                channel = conn.channel()
                try:
                    response = channel.exchange_declare("celery", passive=True)
                    exchange_type = response['type']
                    break  # Exit loop if successful
                except Exception:
                    time.sleep(0.5)  # Wait briefly before retrying

        if exchange_type is None:
            exchange_type = "error: Exchange declaration timed out"
        assert exchange_type != "direct", (
            "Expected Celery to honor task_default_exchange_type, "
            f"but got: {exchange_type}"
        )
