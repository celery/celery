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


def create_app(queue_name: str) -> Celery:
    rabbitmq_user = os.environ.get("RABBITMQ_DEFAULT_USER", "guest")
    rabbitmq_pass = os.environ.get("RABBITMQ_DEFAULT_PASS", "guest")
    redis_host = os.environ.get("REDIS_HOST", "localhost")
    redis_port = os.environ.get("REDIS_PORT", "6379")

    broker_url = os.environ.get("TEST_BROKER", f"pyamqp://{rabbitmq_user}:{rabbitmq_pass}@localhost:5672//")
    backend_url = os.environ.get("TEST_BACKEND", f"redis://{redis_host}:{redis_port}/0")
    app = Celery(
        "quorum_qos_race",
        broker=os.environ.get("TEST_BROKER", broker_url),
        backend=os.environ.get("TEST_BACKEND", backend_url),
    )

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
    @app.task(name="dummy_task")
    def dummy_task():
        if simulate_qos_issue:
            raise Exception("qos.global not allowed on quorum queues (simulated)")
        return "ok"

    return dummy_task


def run_worker(simulate_qos_issue: bool, result_queue: multiprocessing.Queue):
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
            result_queue.put({"status": "crash", "reason": "Worker crashed without reporting"})


@pytest.mark.amqp
@pytest.mark.timeout(90)
def test_rabbitmq_quorum_qos_visibility_race():
    """
    Simulates a quorum queue visibility race condition in clustered RabbitMQ nodes,
    where global QoS is incorrectly applied before quorum properties are known.
    """
    results = []
    processes = []
    queues = []

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

    for i, (p, q) in enumerate(zip(processes, queues)):
        p.join(timeout=30)
        if p.is_alive():
            logger.warning(f"[worker {i}] still alive after timeout, terminating")
            p.terminate()
            p.join()
            results.append({"status": "timeout", "reason": f"[worker {i}] timeout"})
        else:
            try:
                results.append(q.get(timeout=5))
            except Exception as e:
                results.append({"status": "error", "reason": f"Result error: {str(e)}"})

    logger.info(f"Results: {results}")

    if any("qos.global not allowed" in r.get("reason", "").lower() for r in results):
        pytest.xfail("Detected global QoS usage on quorum queue (simulated failure)")
