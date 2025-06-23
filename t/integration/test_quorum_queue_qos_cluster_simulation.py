import logging
import multiprocessing
import os
import time
from queue import Empty

import pytest
from kombu import Exchange, Queue
from kombu.common import maybe_declare

from celery import Celery
from celery.worker.consumer import tasks as consumer_tasks

logger = logging.getLogger(__name__)


def delete_queue_amqp(queue_name, broker_url):
    from kombu import Connection
    with Connection(broker_url) as conn:
        try:
            queue = Queue(name=queue_name)
            queue(conn.channel()).delete(if_unused=False, if_empty=False)
        except Exception as exc:
            logger.warning(f"Queue deletion failed: {exc}")


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
        queue_arguments={"x-queue-type": "quorum"},
    )
    return broker_url, backend_url, queue


def worker_process(worker_id, simulate_fail_flag, result_queue, broker_url, backend_url, queue_name):
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
            with app.broker_connection() as conn:
                with conn.channel() as channel:
                    maybe_declare(queue.exchange, channel)
                    maybe_declare(queue, channel)

            result = dummy_task.apply_async(queue=queue_name)
            output = result.get(timeout=30)
            result_queue.put(output)
    except Exception as exc:
        logger.exception(f"[worker {worker_id}] Exception: {exc}")
        result_queue.put(f"[worker {worker_id}] failed: {exc}")
    finally:
        result_queue.close()


@pytest.mark.amqp
@pytest.mark.timeout(180)
@pytest.mark.xfail(
    reason="Expect global QoS errors when quorum queue visibility hasn't propagated.",
    strict=False,
)
def test_simulated_rabbitmq_cluster_visibility_race(app_config):
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
        p.start()
        processes.append(p)

    deadline = time.time() + 70
    for p in processes:
        remaining = deadline - time.time()
        p.join(timeout=remaining)
        if p.is_alive():
            logger.warning(f"[main] Worker {p.pid} hung. Forcing termination.")
            p.terminate()
            p.join(timeout=5)

    results = []
    while not result_queue.empty():
        try:
            results.append(result_queue.get_nowait())
        except Empty:
            break

    logger.info(f"Worker results: {results}")

    assert any("failed" in r.lower() for r in results if isinstance(r, str)), \
        f"Expected at least one QoS-related failure. Results: {results}"
