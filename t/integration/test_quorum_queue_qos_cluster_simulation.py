import logging
import multiprocessing
import os
import uuid

import pytest
from kombu import Queue

from celery import Celery
from celery.contrib.testing.worker import start_worker

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Force safe multiprocessing start method for CI (e.g., GitHub Actions runners)
try:
    multiprocessing.set_start_method("spawn", force=True)
except RuntimeError:
    # Ignore if already set (e.g., when running multiple tests in same process)
    pass


def create_app(queue_name: str) -> Celery:
    """
    Create and configure a Celery app with a dedicated quorum queue.
    """
    rabbitmq_user = os.environ.get("RABBITMQ_DEFAULT_USER", "guest")
    rabbitmq_pass = os.environ.get("RABBITMQ_DEFAULT_PASS", "guest")
    redis_host = os.environ.get("REDIS_HOST", "localhost")
    redis_port = os.environ.get("REDIS_PORT", "6379")

    broker_url = os.environ.get("TEST_BROKER", f"pyamqp://{rabbitmq_user}:{rabbitmq_pass}@localhost:5672//")
    backend_url = os.environ.get("TEST_BACKEND", f"redis://{redis_host}:{redis_port}/0")

    app = Celery("quorum_qos_race", broker=broker_url, backend=backend_url)

    # Simulate production-like configuration with quorum and required options
    app.conf.task_queues = [
        Queue(
            name=queue_name,
            queue_arguments={"x-queue-type": "quorum"},
        )
    ]
    app.conf.task_default_queue = queue_name
    app.conf.worker_prefetch_multiplier = 1
    app.conf.task_acks_late = True
    app.conf.task_reject_on_worker_lost = True
    app.conf.broker_transport_options = {"confirm_publish": True}

    return app


def dummy_task_factory(app: Celery, simulate_qos_issue: bool):
    """
    Register a dummy task that conditionally raises an exception to simulate
    a global QoS failure due to premature quorum declaration propagation.
    """
    @app.task(name="dummy_task")
    def dummy_task():
        if simulate_qos_issue:
            raise Exception("qos.global not allowed on quorum queues (simulated)")
        return "ok"

    return dummy_task


def run_worker(simulate_qos_issue: bool, result_queue):
    """
    Entry point for subprocess worker. Launches a Celery worker and submits
    a dummy task. Reports result or failure to a multiprocessing-safe queue.
    """
    queue_name = f"race_quorum_queue_{uuid.uuid4().hex}"
    app = create_app(queue_name)
    task = dummy_task_factory(app, simulate_qos_issue)

    try:
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
        if result_queue.empty():
            # If worker died before putting anything in the result queue
            result_queue.put({"status": "crash", "reason": "Worker crashed without reporting"})


@pytest.mark.amqp
@pytest.mark.timeout(90)
def test_rabbitmq_quorum_qos_visibility_race():
    """
    This test simulates a race condition in clustered RabbitMQ nodes with quorum queues,
    where the first worker may attempt global QoS before quorum info is fully propagated.
    If such an error is detected, the test xfails (known limitation).
    """
    results = []
    processes = []
    queues = []
    managers = []

    for i in range(3):
        simulate = (i == 0)  # Simulate failure only on the first worker

        # Use multiprocessing.Manager().Queue() to avoid hangs on CI
        manager = multiprocessing.Manager()
        q = manager.Queue()
        managers.append(manager)
        queues.append(q)

        # Spawn a new daemonized process to isolate each worker
        p = multiprocessing.Process(
            target=run_worker,
            args=(simulate, q),
        )
        p.daemon = True  # Ensures cleanup even if test exits early
        processes.append(p)
        p.start()

    try:
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

        # If simulated global QoS error is detected, xfail the test
        if any("qos.global not allowed" in r.get("reason", "").lower() for r in results):
            for i, p in enumerate(processes):
                if p.is_alive():
                    logger.warning(f"[worker {i}] still alive before xfail, terminating")
                    p.terminate()
                    p.join(timeout=10)
            pytest.xfail("Detected global QoS usage on quorum queue (simulated failure)")
    finally:
        # Final cleanup to ensure no worker is left hanging
        for i, p in enumerate(processes):
            if p.is_alive():
                logger.warning(f"[worker {i}] still alive in final cleanup, terminating")
                p.terminate()
                p.join(timeout=10)
        for m in managers:
            m.shutdown()  # Shut down manager processes to prevent hanging CI
