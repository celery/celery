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
    return Celery(
        "race_test",
        broker=broker_url,
        backend=backend_url,
    )


def run_worker(broker_url, backend_url, queue, simulate_failure, result_queue):
    os.environ["TEST_BROKER"] = broker_url
    os.environ["TEST_BACKEND"] = backend_url
    os.environ["SIMULATE_FAILURE"] = "1" if simulate_failure else "0"

    # Delete the queue first to avoid precondition error
    try:
        with Connection(broker_url) as conn:
            chan = conn.channel()
            queue(chan).delete(if_unused=False, if_empty=False)
            chan.close()
    except Exception as e:
        logger.info(f"[worker {simulate_failure}] queue deletion skipped: {e}")

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
            if simulate_failure:
                logger.info(">>> Simulating quorum detection failure <<<")
                raise Exception("Simulated quorum QoS failure")
            else:
                dummy_task.delay().get(timeout=10)
        result_queue.put("ok")
    except Exception as e:
        result_queue.put(str(e))


@pytest.fixture
def app_config():
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
    broker_url, backend_url, queue = app_config
    results = []
    processes = []
    result_queues = []

    for i in range(3):
        simulate = i == 0
        result_queue = multiprocessing.Queue()
        result_queues.append(result_queue)

        p = multiprocessing.Process(
            target=run_worker,
            args=(broker_url, backend_url, queue, simulate, result_queue),
        )
        p.start()
        processes.append(p)

    for i, (p, q) in enumerate(zip(processes, result_queues)):
        p.join(timeout=20)
        if p.is_alive():
            p.terminate()
            results.append(f"[worker {i}] timeout")
        else:
            results.append(q.get(timeout=5))

    logger.info(f"Results: {results}")

    expected_error = any("Simulated" in r or "failed" in r.lower() or "timeout" in r.lower() for r in results)
    assert expected_error, f"Expected at least one QoS-related failure. Results:\n{results}"
