import io
import logging
import os
import sys
import threading
import time

import pytest
from kombu import Exchange, Queue
from kombu.common import maybe_declare as original_maybe_declare

from celery import Celery
from celery.contrib.testing.worker import start_worker
from celery.worker.consumer import tasks as consumer_tasks

logger = logging.getLogger(__name__)


def delete_queue_amqp(queue_name, broker_url):
    """Force delete a queue from RabbitMQ, ignoring if it doesn't exist yet."""
    from kombu import Connection
    with Connection(broker_url) as conn:
        try:
            channel = conn.channel()
            queue = Queue(name=queue_name)
            queue(channel).delete(if_unused=False, if_empty=False)
        except Exception as exc:
            logger.warning(f"Queue deletion failed (may not exist yet): {exc}")


@pytest.fixture
def app():
    """Create Celery app using CI-provided TEST_BROKER and TEST_BACKEND env vars."""
    broker_url = os.environ["TEST_BROKER"]
    backend_url = os.environ["TEST_BACKEND"]
    return Celery("test_app", broker=broker_url, backend=backend_url)


@pytest.mark.amqp
@pytest.mark.xfail(
    reason="Expect global QoS errors when quorum queue visibility hasn't propagated across RabbitMQ cluster.",
    strict=True,
)
def test_simulated_rabbitmq_cluster_visibility_race(app, monkeypatch, caplog):
    """
    Simulate a race condition in RabbitMQ cluster quorum queue visibility.
    This test ensures that global QoS on quorum queues fails gracefully
    when the queue is not yet visible to all cluster nodes.
    """
    quorum_queue = Queue(
        name="race_quorum_queue",
        exchange=Exchange("test_exchange", type="topic"),
        routing_key="test.routing.key",
        queue_arguments={"x-queue-type": "quorum"},
    )
    app.conf.task_queues = [quorum_queue]
    app.conf.worker_detect_quorum_queues = True

    @app.task
    def dummy_task():
        return "ok"

    # Ensure test starts with a fresh queue
    delete_queue_amqp(quorum_queue.name, app.conf.broker_url)

    # Monkeypatch quorum detection to fail on first call only
    detect_call_count = {"count": 0}
    original_detect = consumer_tasks.detect_quorum_queues

    def flaky_detect_quorum_queues(app_arg, driver_type):
        detect_call_count["count"] += 1
        if detect_call_count["count"] == 1:
            logger.info(">>> Simulating quorum detection failure for first worker <<<")
            return False, quorum_queue.name
        return original_detect(app_arg, driver_type)

    monkeypatch.setattr(consumer_tasks, "detect_quorum_queues", flaky_detect_quorum_queues)

    # Add artificial delay to maybe_declare to encourage race conditions
    def delayed_maybe_declare(entity, channel, **kwargs):
        time.sleep(0.2)
        return original_maybe_declare(entity, channel, **kwargs)

    monkeypatch.setattr("kombu.common.maybe_declare", delayed_maybe_declare)

    # Enable verbose logs for inspection
    for logger_name in ["kombu", "amqp"]:
        logging.getLogger(logger_name).setLevel(logging.DEBUG)

    # Collect stderr logs and worker-level exceptions
    errors = []
    stderr_capture = io.StringIO()
    original_stderr = sys.stderr
    sys.stderr = stderr_capture

    worker_count = 3
    threads = []
    done_flags = [threading.Event() for _ in range(worker_count)]
    task_results = [None] * worker_count

    def start_worker_instance(worker_id, done_event):
        try:
            with start_worker(
                app,
                queues=[quorum_queue.name],
                loglevel="info",
                perform_ping_check=False,
                shutdown_timeout=30,
                hostname=f"worker_{worker_id}@testhost",
                terminate=True,
            ):
                # Submit and wait for task result (bounded timeout)
                task_results[worker_id] = dummy_task.apply_async(queue=quorum_queue.name)
                result = task_results[worker_id].get(timeout=20)
                logger.info(f"Worker {worker_id} got task result: {result}")
        except Exception as exc:
            logger.exception(f"Worker {worker_id} failed")
            errors.append(exc)
        finally:
            done_event.set()

    try:
        # Launch worker threads
        for i in range(worker_count):
            t = threading.Thread(target=start_worker_instance, args=(i, done_flags[i]), daemon=True)
            threads.append(t)
            t.start()

        # Ensure all workers exit within timeout
        for event in done_flags:
            if not event.wait(timeout=60):
                logger.error("Worker thread did not finish within timeout")
                raise AssertionError("Worker thread did not finish within timeout")

    finally:
        sys.stderr = original_stderr

    # Gather captured logs
    stderr_output = stderr_capture.getvalue()
    log_output = "\n".join(caplog.messages)

    # Define AMQP/global QoS failure indicators
    qos_error_found = any(
        term in output
        for output in [stderr_output, log_output] + [str(e) for e in errors]
        for term in [
            "NOT_IMPLEMENTED", "540", "AMQPNotImplementedError", "PRECONDITION_FAILED",
            "UnexpectedFrame", "Connection to broker lost", "Basic.consume: (540)",
            "failed to exit within the allocated timeout",
        ]
    )

    # Assert we observed failure due to quorum queue visibility issues
    assert qos_error_found, (
        "\nExpected AMQP 540 error or connection error, but none found.\n"
        f"Captured exceptions:\n{errors}\n\n"
        f"Captured stderr:\n{stderr_output}\n\n"
        f"Captured logs:\n{log_output}\n"
    )
