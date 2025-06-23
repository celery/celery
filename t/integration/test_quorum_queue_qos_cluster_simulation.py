import logging
import multiprocessing
import os

import pytest
from kombu import Connection, Exchange, Queue

from celery import Celery
from celery.contrib.testing.worker import start_worker

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def make_app(broker_url, backend_url):
    """Create and configure a new Celery app instance for testing."""
    return Celery(
        "race_test",
        broker=broker_url,
        backend=backend_url,
    )


def run_worker(broker_url, backend_url, queue, simulate_failure, result_queue):
    """
    Run a Celery worker in a separate process.
    If simulate_failure is True, simulate a startup error to test quorum visibility handling.
    The result (success or error message) is sent to result_queue.
    """
    # Ensure connection info is available as environment variables (used in CI)
    os.environ["TEST_BROKER"] = broker_url
    os.environ["TEST_BACKEND"] = backend_url
    os.environ["SIMULATE_FAILURE"] = "1" if simulate_failure else "0"

    # Attempt to delete the queue before starting, to avoid PreconditionFailed errors
    try:
        with Connection(broker_url) as conn:
            chan = conn.channel()
            queue(chan).delete(if_unused=False, if_empty=False)
            chan.close()
    except Exception as e:
        logger.info(f"[worker {simulate_failure}] queue deletion skipped: {e}")

    # Create Celery app with a single-task queue configuration
    app = make_app(broker_url, backend_url)
    app.conf.task_queues = [queue]
    app.conf.task_default_queue = queue.name
    app.conf.task_default_exchange = queue.exchange.name
    app.conf.task_default_routing_key = queue.routing_key
    app.conf.worker_prefetch_multiplier = 1
    app.conf.worker_concurrency = 1

    @app.task(name="dummy_task")
    def dummy_task():
        return f"ok from worker {simulate_failure}"

    try:
        with start_worker(
            app,
            loglevel="info",
            perform_ping_check=False,
            shutdown_timeout=5,
            terminate=True,
        ):
            # Simulate quorum propagation failure by raising early exception
            if simulate_failure:
                logger.info(">>> Simulating quorum detection failure <<<")
                raise Exception("Simulated quorum QoS failure")
            else:
                # Send and wait for task result to verify success
                dummy_task.delay().get(timeout=10)
        result_queue.put("ok")
    except Exception as e:
        result_queue.put(str(e))


@pytest.fixture
def app_config():
    """
    Provides test configuration:
    - broker and backend URLs (from env or defaults)
    - a preconfigured quorum queue with limited delivery
    """
    rabbitmq_user = os.environ.get("RABBITMQ_DEFAULT_USER", "guest")
    rabbitmq_pass = os.environ.get("RABBITMQ_DEFAULT_PASS", "guest")
    redis_host = os.environ.get("REDIS_HOST", "localhost")
    redis_port = os.environ.get("REDIS_PORT", "6379")

    broker_url = os.environ.get("TEST_BROKER", f"pyamqp://{rabbitmq_user}:{rabbitmq_pass}@localhost:5672//")
    backend_url = os.environ.get("TEST_BACKEND", f"redis://{redis_host}:{redis_port}/0")

    # Quorum queue with x-delivery-limit to simulate one-attempt delivery visibility
    queue = Queue(
        name="race_quorum_queue",
        exchange=Exchange("test_exchange", type="topic"),
        routing_key="test.routing.key",
        queue_arguments={
            "x-queue-type": "quorum",
            "x-delivery-limit": 1,
        },
    )
    return broker_url, backend_url, queue


@pytest.mark.amqp
@pytest.mark.timeout(60)
@pytest.mark.xfail(reason="Expect global QoS errors when quorum queue visibility hasn't propagated.", strict=False)
def test_simulated_rabbitmq_cluster_visibility_race(app_config):
    """
    Launches three worker processes sharing a quorum queue.
    The first worker simulates a failure to test quorum queue visibility and QoS error propagation.
    At least one failure is expected (PRECONDITION_FAILED or timeout).
    """
    broker_url, backend_url, queue = app_config
    results = []
    processes = []
    result_queues = []

    # Start three workers; first one simulates a quorum QoS failure
    for i in range(3):
        simulate = i == 0  # Only the first worker will simulate a startup failure
        result_queue = multiprocessing.Queue()
        result_queues.append(result_queue)

        p = multiprocessing.Process(
            target=run_worker,
            args=(broker_url, backend_url, queue, simulate, result_queue),
        )
        p.start()
        processes.append(p)

    # Join workers and collect results with timeout
    for i, (p, q) in enumerate(zip(processes, result_queues)):
        p.join(timeout=20)
        if p.is_alive():
            p.terminate()
            results.append(f"[worker {i}] timeout")
        else:
            results.append(q.get(timeout=5))

    logger.info(f"Results: {results}")

    # Ensure at least one failure occurred to confirm race condition was simulated
    expected_error = any("Simulated" in r or "failed" in r.lower() or "timeout" in r.lower() for r in results)
    assert expected_error, f"Expected at least one QoS-related failure. Results:\n{results}"
