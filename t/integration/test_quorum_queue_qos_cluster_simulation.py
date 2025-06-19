import io
import logging
import sys
import threading
import time

import pytest
from kombu import Exchange, Queue
from kombu.common import maybe_declare as original_maybe_declare

from celery import Celery
from celery.contrib.testing.worker import start_worker
from celery.worker.consumer import tasks as consumer_tasks


def delete_queue_amqp(queue_name, broker_url):
    """
    Delete a queue from RabbitMQ using Kombu.
    Avoids subprocesses or external tools like rabbitmqadmin.
    """
    from kombu import Connection

    with Connection(broker_url) as conn:
        try:
            channel = conn.channel()
            queue = Queue(name=queue_name)
            queue(channel).delete(if_unused=False, if_empty=False)
            print(f"Queue '{queue_name}' deleted.")
        except Exception as exc:
            print(f"Queue deletion failed (may not exist yet): {exc}")


@pytest.mark.amqp
@pytest.mark.xfail(
    reason=(
        "Cluster race visibility test: Expected intermittent AMQP 540 NOT_IMPLEMENTED errors "
        "may not trigger deterministically in some environments. Test passes only if such an error occurs."
    ),
    strict=True,
)
def test_simulated_rabbitmq_cluster_visibility_race(monkeypatch, caplog):
    """
    Simulate a RabbitMQ quorum queue cluster propagation race.

    Scenario:
    - First worker fails quorum detection (simulating cluster lag)
    - Other workers succeed
    - All workers start concurrently
    - Expect at least one RabbitMQ 540 NOT_IMPLEMENTED or connection-level AMQP error
      due to global QoS being incorrectly applied on a quorum queue
    """
    # -----------------------------------
    # Test App + Queue Setup
    # -----------------------------------
    app = Celery('test_app')
    app.conf.broker_url = 'pyamqp://guest:guest@rabbit:5672//'
    app.conf.worker_detect_quorum_queues = True

    quorum_queue = Queue(
        name='race_quorum_queue',
        exchange=Exchange('test_exchange', type='topic'),
        routing_key='test.routing.key',
        queue_arguments={"x-queue-type": "quorum"},
    )
    app.conf.task_queues = [quorum_queue]

    @app.task
    def dummy_task():
        return "ok"

    # Ensure the target queue doesn't exist before starting
    delete_queue_amqp(quorum_queue.name, app.conf.broker_url)

    # -----------------------------------
    # Fault Injection: Simulate quorum detection failure for first worker
    # -----------------------------------
    detect_call_count = {"count": 0}
    original_detect = consumer_tasks.detect_quorum_queues

    def flaky_detect_quorum_queues(app_arg, driver_type):
        detect_call_count["count"] += 1
        if detect_call_count["count"] == 1:
            print(">>> Simulating quorum detection failure for first worker <<<")
            return False, quorum_queue.name
        return original_detect(app_arg, driver_type)

    monkeypatch.setattr(consumer_tasks, "detect_quorum_queues", flaky_detect_quorum_queues)

    # -----------------------------------
    # Slow down queue declaration to amplify race
    # -----------------------------------
    def delayed_maybe_declare(entity, channel, **kwargs):
        time.sleep(0.2)
        return original_maybe_declare(entity, channel, **kwargs)

    monkeypatch.setattr("kombu.common.maybe_declare", delayed_maybe_declare)

    # Enable detailed AMQP and Kombu debug logging
    for logger_name in ["kombu", "amqp"]:
        logging.getLogger(logger_name).setLevel(logging.DEBUG)

    # -----------------------------------
    # Start multiple workers concurrently
    # -----------------------------------
    errors = []
    worker_count = 3
    stderr_capture = io.StringIO()
    log_handler = logging.StreamHandler(stderr_capture)
    log_handler.setLevel(logging.ERROR)
    test_logger = logging.getLogger("test_logger")
    test_logger.setLevel(logging.ERROR)
    test_logger.addHandler(log_handler)

    def start_worker_instance(worker_id):
        try:
            with start_worker(
                app,
                queues=[quorum_queue.name],
                loglevel='info',
                perform_ping_check=False,
                shutdown_timeout=30,
                hostname=f"worker_{worker_id}@testhost",
            ):
                dummy_task.apply_async(queue=quorum_queue.name)
        except Exception as exc:
            errors.append(exc)

    threads = []
    try:
        for worker_id in range(worker_count):
            thread = threading.Thread(target=start_worker_instance, args=(worker_id,))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join(timeout=30)
    finally:
        sys.stderr = original_stderr  # Always restore original stderr

    # -----------------------------------
    # Check for stuck threads
    # -----------------------------------
    for idx, thread in enumerate(threads):
        if thread.is_alive():
            print(f"WARNING: Worker thread {idx} did not shut down (likely stuck on AMQP error)")

    # -----------------------------------
    # Collect logs and stderr
    # -----------------------------------
    stderr_output = stderr_capture.getvalue()
    log_output = "\n".join(caplog.messages)

    if errors:
        print("\n=== Captured Worker Exceptions ===")
        for exc in errors:
            print(repr(exc))
    else:
        print("\n=== No exceptions captured from any worker ===")

    print("\n=== FULL stderr_capture content ===")
    print(stderr_output)

    print("\n=== Captured Log Output ===")
    print(log_output)

    with open("/tmp/celery_quorum_qos_test_stderr.log", "w") as f:
        f.write(stderr_output)

    # -----------------------------------
    # Assertion: Expect at least one AMQP QoS-related error
    # -----------------------------------
    qos_error_found = (
        any(
            "NOT_IMPLEMENTED" in str(e)
            or "540" in str(e)
            or "failed to exit within the allocated timeout" in str(e)
            for e in errors
        )
        or any(keyword in stderr_output for keyword in [
            "NOT_IMPLEMENTED", "540", "AMQPNotImplementedError", "PRECONDITION_FAILED",
            "UnexpectedFrame", "Connection to broker lost", "Basic.consume: (540)"
        ])
        or any(keyword in log_output for keyword in [
            "NOT_IMPLEMENTED", "540", "AMQPNotImplementedError", "PRECONDITION_FAILED",
            "UnexpectedFrame", "Connection to broker lost", "Basic.consume: (540)"
        ])
    )

    if not qos_error_found:
        print("\n=== Assertion Failure Debug Dump ===")
        print("\n=== stderr_output ===\n", stderr_output)
        print("\n=== log_output ===\n", log_output)
        print("\n=== captured exceptions ===\n", errors)

    assert qos_error_found, (
        "\nExpected at least one RabbitMQ 540 NOT_IMPLEMENTED or AMQP connection-level error "
        f"during race condition test, but none found.\n\n"
        f"Captured exceptions:\n{errors}\n\n"
        f"Captured stderr:\n{stderr_output}\n\n"
        f"Captured logs:\n{log_output}\n"
    )
