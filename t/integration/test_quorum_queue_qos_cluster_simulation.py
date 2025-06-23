import logging
import multiprocessing
import os
import time
from queue import Empty as QueueEmpty

import pytest
from kombu import Exchange, Queue
from kombu.common import maybe_declare

from celery import Celery
from celery.worker.consumer import tasks as consumer_tasks

logger = logging.getLogger(__name__)


def delete_queue_amqp(queue_name, broker_url):
    """
    Delete the target queue from RabbitMQ to ensure a clean test run.
    Ignores errors if the queue does not yet exist.
    """
    from kombu import Connection
    with Connection(broker_url) as conn:
        try:
            queue = Queue(name=queue_name)
            queue(conn.channel()).delete(if_unused=False, if_empty=False)
        except Exception as exc:
            logger.warning(f"Queue deletion failed: {exc}")


@pytest.fixture
def app_config():
    """
    Returns broker, backend, and quorum queue definition for test setup.
    Uses environment-provided broker/backend to ensure CI compatibility.
    """
    rabbitmq_user = os.environ.get("RABBITMQ_DEFAULT_USER", "guest")
    rabbitmq_pass = os.environ.get("RABBITMQ_DEFAULT_PASS", "guest")
    redis_host = os.environ.get("REDIS_HOST", "localhost")
    redis_port = os.environ.get("REDIS_PORT", "6379")

    broker_url = os.environ.get("TEST_BROKER", f"pyamqp://{rabbitmq_user}:{rabbitmq_pass}@localhost:5672//")
    backend_url = os.environ.get("TEST_BACKEND", f"redis://{redis_host}:{redis_port}/0")
    queue = Queue(
        name="race_quorum_queue",
        exchange=Exchange("test_exchange", type="topic"),
        routing_key="test.routing.key",
        queue_arguments={"x-queue-type": "quorum"},
    )
    return broker_url, backend_url, queue


def worker_process(worker_id, simulate_fail_flag, result_queue, broker_url, backend_url, queue_name):
    """
    Worker process function used in multiprocessing to isolate Celery app context.
    It declares a quorum queue, patches quorum detection to fail once, starts a worker,
    submits a task, and returns the result (or error) through a multiprocessing.Queue.
    """
    app = Celery(f"worker_{worker_id}", broker=broker_url, backend=backend_url)

    queue = Queue(
        name=queue_name,
        exchange=Exchange("test_exchange", type="topic"),
        routing_key="test.routing.key",
        queue_arguments={"x-queue-type": "quorum"},
    )
    app.conf.task_queues = [queue]
    app.conf.worker_detect_quorum_queues = True
    app.conf.task_default_queue = queue_name
    app.conf.task_default_exchange = "test_exchange"
    app.conf.task_default_exchange_type = "topic"
    app.conf.task_default_routing_key = "test.routing.key"

    @app.task
    def dummy_task():
        return f"ok from worker {worker_id}"

    original_detect = consumer_tasks.detect_quorum_queues

    def flaky_detect_quorum_queues(app_arg, driver_type):
        with simulate_fail_flag.get_lock():
            if simulate_fail_flag.value:
                simulate_fail_flag.value = False
                logger.info(">>> Simulating quorum detection failure <<<")
                return False, queue_name
        return original_detect(app_arg, driver_type)

    consumer_tasks.detect_quorum_queues = flaky_detect_quorum_queues

    from celery.contrib.testing.worker import start_worker

    try:
        with start_worker(
            app,
            queues=[queue_name],
            loglevel="info",
            perform_ping_check=False,
            shutdown_timeout=60,
            hostname=f"worker_{worker_id}@testhost",
            terminate=True,
        ):
            logger.info(f"[worker {worker_id}] Worker started")

            with app.broker_connection() as conn:
                with conn.channel() as channel:
                    maybe_declare(queue.exchange, channel)
                    maybe_declare(queue, channel)

            result = dummy_task.apply_async(queue=queue_name)
            output = result.get(timeout=30)
            result_queue.put(output)
    except Exception as exc:
        logger.exception(f"[worker {worker_id}] Exception: {exc}")
        try:
            result_queue.put(f"[worker {worker_id}] failed: {exc}")
        except Exception:
            pass
    finally:
        time.sleep(0.2)
        result_queue.close()


@pytest.mark.amqp
@pytest.mark.timeout(180)
@pytest.mark.xfail(
    reason="Expect global QoS errors when quorum queue visibility hasn't propagated.",
    strict=False,
)
def test_simulated_rabbitmq_cluster_visibility_race(app_config):
    """
    Integration test to simulate a RabbitMQ quorum queue visibility race condition
    across cluster nodes.

    Celery workers use quorum detection logic to avoid premature global QoS setup
    before queues are fully propagated. This test verifies that such races fail
    gracefully and are detectable in logs or results.

    Simulates:
    - Multiple workers racing to attach to a quorum queue
    - One worker hitting visibility failure due to propagation lag
    - Captures at least one failure to validate race condition

    Expected:
    - At least one worker fails to initialize due to global QoS setup on
      an undeclared quorum queue
    - Failure is observable via logged error or result payload
    """
    broker_url, backend_url, queue = app_config

    delete_queue_amqp(queue.name, broker_url)

    simulate_fail_flag = multiprocessing.Value('b', True)
    result_queue = multiprocessing.Queue()
    processes = []

    for worker_id in range(3):
        p = multiprocessing.Process(
            target=worker_process,
            args=(worker_id, simulate_fail_flag, result_queue, broker_url, backend_url, queue.name),
        )
        processes.append(p)
        p.start()

    for p in processes:
        p.join(timeout=60)
        if p.is_alive():
            logger.warning(f"Process {p.pid} did not exit in time. Terminating.")
            p.terminate()
            p.join(timeout=10)

    results = []
    try:
        while True:
            results.append(result_queue.get_nowait())
    except QueueEmpty:
        pass
    finally:
        result_queue.close()

    logger.info(f"Worker results: {results}")

    # Assert: At least one worker should fail (simulated race condition)
    expected_error = any(
        isinstance(r, str) and "failed" in r.lower()
        for r in results
    )
    assert expected_error, f"Expected at least one QoS-related failure. Results: {results}"
