import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import pytest
from kombu import Exchange, Queue

from celery import Celery, chord
from celery.contrib.testing.worker import start_worker
from celery.result import allow_join_result

logger = logging.getLogger(__name__)


@pytest.fixture(scope="function")
def app():
    """
    Celery app configured to:
    - Use quorum queues with topic exchanges
    - Route chord_unlock to a dedicated quorum queue
    """
    app = Celery(
        "test_app",
        broker="pyamqp://guest:guest@rabbit:5672//",
        backend="redis://redis/0",
    )

    app.conf.task_default_exchange_type = "topic"
    app.conf.task_default_exchange = "default_exchange"
    app.conf.task_default_queue = "default_queue"
    app.conf.task_default_routing_key = "default"

    app.conf.task_queues = [
        Queue(
            "header_queue",
            Exchange("header_exchange", type="topic"),
            routing_key="header_rk",
            queue_arguments={"x-queue-type": "quorum"},
        ),
        Queue(
            "chord_callback_queue",
            Exchange("chord_callback_exchange", type="topic"),
            routing_key="chord_callback_queue",
            queue_arguments={"x-queue-type": "quorum"},
        ),
    ]

    app.conf.task_routes = {
        "celery.chord_unlock": {
            "queue": "chord_callback_queue",
            "exchange": "chord_callback_exchange",
            "routing_key": "chord_callback_queue",
            "exchange_type": "topic",
        },
    }

    return app


@pytest.fixture
def add(app):
    @app.task(bind=True, max_retries=3, default_retry_delay=1)
    def add(self, x, y):
        time.sleep(0.05)
        return x + y
    return add


@pytest.fixture
def summarize(app):
    @app.task(bind=True, max_retries=3, default_retry_delay=1)
    def summarize(self, results):
        return sum(results)
    return summarize


def wait_for_chord_unlock(chord_result, timeout=10, interval=0.2):
    """
    Waits for chord_unlock to be enqueued by polling the `parent` of the chord result.
    This confirms that the header group finished and the callback is ready to run.
    """
    start = time.monotonic()
    while time.monotonic() - start < timeout:
        if chord_result.parent and chord_result.parent.ready():
            return True
        time.sleep(interval)
    return False


@pytest.mark.amqp
@pytest.mark.timeout(90)
@pytest.mark.xfail(reason="chord_unlock routed to quorum/topic queue intermittently fails under load")
def test_chord_unlock_stress_routing_to_quorum_queue(app, add, summarize):
    """
    Reproduces Celery Discussion #9742 (intermittently):
    When chord_unlock is routed to a quorum queue via topic exchange, it may not be consumed
    even if declared and bound, leading to stuck results.

    This stress test submits many chords rapidly, each routed explicitly via a topic exchange,
    and waits to see how many complete.
    """
    chord_count = 50
    header_fanout = 3
    failures = []

    pending_results = []

    with allow_join_result():
        # Submit chords BEFORE worker is running
        for i in range(chord_count):
            header = [
                add.s(i, j).set(
                    queue="header_queue",
                    exchange="header_exchange",
                    routing_key="header_rk",
                )
                for j in range(header_fanout)
            ]

            callback = summarize.s().set(
                queue="chord_callback_queue",
                exchange="chord_callback_exchange",
                routing_key="chord_callback_queue",
            )

            result = chord(header)(callback)
            pending_results.append((i, result))

        # Wait for chord_unlock tasks to be dispatched before starting the worker
        for i, result in pending_results:
            if not wait_for_chord_unlock(result):
                logger.warning(f"[!] Chord {i}: unlock was not dispatched within timeout")

        # Start worker that consumes both header and callback queues
        with start_worker(
            app, queues=["header_queue", "chord_callback_queue"], loglevel="info", perform_ping_check=False
        ):
            # Poll all chord results
            with ThreadPoolExecutor(max_workers=10) as executor:
                futures = {
                    executor.submit(result.get, timeout=20): (i, result)
                    for i, result in pending_results
                }

                for future in as_completed(futures):
                    i, result = futures[future]
                    try:
                        res = future.result()
                        logger.info(f"[✓] Chord {i} completed: {res}")
                    except Exception as exc:
                        logger.error(f"[✗] Chord {i} failed or stuck: {exc}")
                        failures.append((i, exc))

    # Assertion: all chords should have completed
    assert not failures, f"{len(failures)} of {chord_count} chords failed or got stuck"
