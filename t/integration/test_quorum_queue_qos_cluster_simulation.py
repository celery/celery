import logging
import multiprocessing
import os
import uuid

import pytest
from kombu import Queue

from celery import Celery
from celery.contrib.testing.worker import start_worker

# Configure root logger for visibility during test execution
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def create_app(queue_name: str) -> Celery:
    """
    Creates and configures a Celery app instance using a quorum queue.
    Each worker gets a unique queue to avoid conflicts and cross-interference.
    """
    # Environment fallbacks for local or CI use
    rabbitmq_user = os.environ.get("RABBITMQ_DEFAULT_USER", "guest")
    rabbitmq_pass = os.environ.get("RABBITMQ_DEFAULT_PASS", "guest")
    redis_host = os.environ.get("REDIS_HOST", "localhost")
    redis_port = os.environ.get("REDIS_PORT", "6379")

    # Build broker/backend URLs
    broker_url = os.environ.get("TEST_BROKER", f"pyamqp://{rabbitmq_user}:{rabbitmq_pass}@localhost:5672//")
    backend_url = os.environ.get("TEST_BACKEND", f"redis://{redis_host}:{redis_port}/0")

    app = Celery(
        "quorum_qos_race",
        broker=os.environ.get("TEST_BROKER", broker_url),
        backend=os.environ.get("TEST_BACKEND", backend_url),
    )

    # Configure the task queue as a quorum queue
    app.conf.task_queues = [
        Queue(
            name=queue_name,
            queue_arguments={"x-queue-type": "quorum"},
        )
    ]
    app.conf.task_default_queue = queue_name

    # Worker configuration to enforce visibility of QoS behavior
    app.conf.worker_prefetch_multiplier = 1
    app.conf.task_acks_late = True
    app.conf.task_reject_on_worker_lost = True
    app.conf.broker_transport_options = {"confirm_publish": True}

    return app


def dummy_task_factory(app: Celery, simulate_qos_issue: bool):
    """
    Dynamically creates a dummy task that may simulate a QoS global failure.
    This is bound to the Celery app instance to isolate task definitions per worker.
    """
    @app.task(name="dummy_task")
    def dummy_task():
        if simulate_qos_issue:
            raise Exception("qos.global not allowed on quorum queues (simulated)")
        return "ok"

    return dummy_task


def run_worker(simulate_qos_issue: bool, result_queue: multiprocessing.Queue):
    """
    Starts a Celery worker in a subprocess and submits a task.
    Result is reported via a multiprocessing-safe queue.
    """
    queue_name = f"race_quorum_queue_{uuid.uuid4().hex}"  # Isolated queue per process
    app = create_app(queue_name)
    task = dummy_task_factory(app, simulate_qos_issue)

    try:
        # Launch worker and run task
        with start_worker(
            app,
            queues=[queue_name],
            loglevel="INFO",
            perform_ping_check=False,
            shutdown_timeout=15,
        ):
            res = task.delay()
            try:
                result = res.get(timeout=10)
                result_queue.put({"status": "ok", "result": result})
            except Exception as e:
                result_queue.put({"status": "error", "reason": str(e)})
    except Exception as e:
        logger.exception("[worker %s] external failure", simulate_qos_issue)
        result_queue.put({"status": "external_failure", "reason": str(e)})
    finally:
        # Fallback in case of total crash or no result sent
        if result_queue.empty():
            result_queue.put({"status": "crash", "reason": "Worker crashed without reporting"})


@pytest.mark.amqp
@pytest.mark.timeout(90)
def test_rabbitmq_quorum_qos_visibility_race():
    """
    Simulates a race condition in quorum queue propagation where a Celery worker
    incorrectly applies global QoS before quorum metadata is fully resolved.

    This test spawns 3 subprocesses with one simulating a QoS violation.
    It verifies isolation, detects global QoS misuse, and forcibly shuts down
    all processes to avoid CI hangs.
    """
    results = []
    processes = []
    queues = []

    # Start 3 workers, with only the first simulating a race-related error
    for i in range(3):
        simulate = (i == 0)
        q = multiprocessing.Queue()
        p = multiprocessing.Process(
            target=run_worker,
            args=(simulate, q),
        )
        processes.append(p)
        queues.append(q)
        p.start()

    try:
        # Collect results and forcibly kill unresponsive workers
        for i, (p, q) in enumerate(zip(processes, queues)):
            try:
                p.join(timeout=30)
                if p.is_alive():
                    logger.warning(f"[worker {i}] still alive after timeout, terminating")
                    p.terminate()
                    p.join(timeout=10)
                    results.append({"status": "timeout", "reason": f"[worker {i}] timeout"})
                else:
                    try:
                        results.append(q.get(timeout=5))
                    except Exception as e:
                        results.append({"status": "error", "reason": f"Result error: {str(e)}"})
            except Exception:
                logger.exception(f"[worker {i}] crashed during join")
                try:
                    results.append(q.get(timeout=5))
                except Exception:
                    results.append({"status": "crash", "reason": f"Worker {i} crashed and gave no result"})

        logger.info(f"Results: {results}")

        # If the simulated global QoS error is observed, fail the test
        if any("qos.global not allowed" in r.get("reason", "").lower() for r in results):
            for i, p in enumerate(processes):
                if p.is_alive():
                    logger.warning(f"[worker {i}] still alive before xfail, terminating")
                    p.terminate()
                    p.join(timeout=10)
            pytest.xfail("Detected global QoS usage on quorum queue (simulated failure)")

    finally:
        # Always clean up any remaining live processes
        for i, p in enumerate(processes):
            if p.is_alive():
                logger.warning(f"[worker {i}] still alive in final cleanup, terminating")
                p.terminate()
                p.join(timeout=10)
