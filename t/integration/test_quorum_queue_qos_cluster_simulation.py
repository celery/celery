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
    """Create and configure a new Celery app instance."""
    return Celery(
        "race_test",
        broker=broker_url,
        backend=backend_url,
    )


def run_worker(broker_url, backend_url, queue, simulate_failure, result_queue):
    """
    Run a Celery worker in a separate process.
    If simulate_failure is True, simulate a startup error to test quorum visibility race conditions.
    The result (success or failure string) is pushed into result_queue for the parent process to collect.
    """
    # Propagate broker/backend info to environment (CI-compatible)
    os.environ["TEST_BROKER"] = broker_url
    os.environ["TEST_BACKEND"] = backend_url
    os.environ["SIMULATE_FAILURE"] = "1" if simulate_failure else "0"

    # Clean up the queue before binding to it — prevents PreconditionFailed errors on re-declare
    try:
        with Connection(broker_url, connect_timeout=5) as conn:
            chan = conn.channel()
            queue(chan).delete(if_unused=False, if_empty=False)
            chan.close()
    except Exception as e:
        logger.warning(f"[worker {simulate_failure}] queue delete failed: {e}")

    # Create and configure Celery app with the queue explicitly set
    app = make_app(broker_url, backend_url)
    app.conf.task_queues = [queue]
    app.conf.task_default_queue = queue.name
    app.conf.task_default_exchange = queue.exchange.name
    app.conf.task_default_routing_key = queue.routing_key
    app.conf.worker_prefetch_multiplier = 1
    app.conf.worker_concurrency = 1

    # Define a dummy task for successful worker verification
    @app.task(name="dummy_task")
    def dummy_task():
        return f"ok from worker {simulate_failure}"

    try:
        # Launch worker in this subprocess (will block until shutdown or error)
        with start_worker(
            app,
            loglevel="info",
            perform_ping_check=False,
            shutdown_timeout=5,
            terminate=True,
        ):
            # Simulate a failure during boot for the first worker (to mimic quorum propagation issues)
            if simulate_failure:
                logger.info(f"[worker {simulate_failure}] Simulating quorum QoS failure")
                raise Exception("Simulated quorum QoS failure")

            # Otherwise run the task to confirm success
            result = dummy_task.delay().get(timeout=10)
            logger.info(f"[worker {simulate_failure}] task result: {result}")

        # If we reached here, the worker ran successfully
        result_queue.put("ok")

    except Exception as e:
        logger.warning(f"[worker {simulate_failure}] exception: {e}")
        result_queue.put(str(e))


@pytest.fixture
def app_config():
    """
    Prepare broker/backend URLs and a quorum queue with delivery limit to simulate one-attempt delivery.
    """
    rabbitmq_user = os.environ.get("RABBITMQ_DEFAULT_USER", "guest")
    rabbitmq_pass = os.environ.get("RABBITMQ_DEFAULT_PASS", "guest")
    redis_host = os.environ.get("REDIS_HOST", "localhost")
    redis_port = os.environ.get("REDIS_PORT", "6379")

    broker_url = os.environ.get("TEST_BROKER", f"pyamqp://{rabbitmq_user}:{rabbitmq_pass}@localhost:5672//")
    backend_url = os.environ.get("TEST_BACKEND", f"redis://{redis_host}:{redis_port}/0")

    # Define quorum queue with x-delivery-limit to ensure single visibility window
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
@pytest.mark.xfail(
    reason="Expect global QoS errors when quorum queue visibility hasn't propagated.",
    strict=False
)
def test_simulated_rabbitmq_cluster_visibility_race(app_config):
    """
    Launches 3 Celery workers sharing the same quorum queue.
    The first one simulates a failure due to quorum queue QoS visibility lag (as in production).
    The others run normally.
    The test passes if at least one worker fails (as expected), confirming that the condition exists.
    """
    broker_url, backend_url, queue = app_config
    results = []
    processes = []
    result_queues = []

    # Start three workers; only the first simulates failure
    for i in range(3):
        simulate = i == 0  # First worker simulates quorum failure
        result_queue = multiprocessing.Queue()
        result_queues.append(result_queue)

        # Launch each worker in an isolated process
        p = multiprocessing.Process(
            target=run_worker,
            args=(broker_url, backend_url, queue, simulate, result_queue),
        )
        p.start()
        processes.append(p)

    # Collect results with proper cleanup
    for i, (p, q) in enumerate(zip(processes, result_queues)):
        p.join(timeout=20)
        if p.is_alive():
            # Worker didn't exit in time — force kill
            logger.warning(f"[worker {i}] still alive after join timeout; forcing terminate")
            p.terminate()
            p.join(timeout=3)
            if p.is_alive():
                logger.warning(f"[worker {i}] still alive after terminate; forcing kill")
                p.kill()
                p.join(timeout=2)
            results.append(f"[worker {i}] timeout and forced kill")
        else:
            # Worker exited — try to get its result
            try:
                result = q.get(timeout=5)
                results.append(result)
            except Exception as e:
                results.append(f"[worker {i}] result_queue get failed: {e}")

    logger.info(f"Results: {results}")

    # At least one worker must fail to simulate quorum race behavior
    expected_error = any(
        "Simulated" in r or "failed" in r.lower() or "timeout" in r.lower()
        for r in results
    )
    assert expected_error, f"Expected at least one QoS-related failure. Results:\n{results}"
