import logging
import multiprocessing
import os
<<<<<<< HEAD
import sys
=======
>>>>>>> 35e7531e5 (Fixes linting)

import pytest
from kombu import Connection, Exchange, Queue
from kombu.common import maybe_declare

from celery import Celery
from celery.contrib.testing.worker import start_worker
from celery.worker.consumer import tasks as consumer_tasks

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


@pytest.fixture
def app_config():
    rabbitmq_user = os.environ.get("RABBITMQ_DEFAULT_USER", "guest")
    rabbitmq_pass = os.environ.get("RABBITMQ_DEFAULT_PASS", "guest")
    redis_host = os.environ.get("REDIS_HOST", "localhost")
    redis_port = os.environ.get("REDIS_PORT", "6379")

    broker_url = os.environ.get("TEST_BROKER", f"pyamqp://{rabbitmq_user}:{rabbitmq_pass}@localhost:5672//")
    backend_url = os.environ.get("TEST_BACKEND", f"redis://{redis_host}:{redis_port}/0")

    queue = Queue(
        "race_quorum_queue",
        Exchange("test_exchange", type="topic"),
        routing_key="test.routing.key",
        queue_arguments={"x-queue-type": "quorum"},
    )

    return broker_url, backend_url, queue


def delete_queue_amqp(queue_name, broker_url):
    with Connection(broker_url) as conn:
        channel = conn.channel()
        try:
            channel.queue_delete(queue=queue_name)
            logger.info(f"Deleted queue: {queue_name}")
        except Exception as exc:
            logger.warning(f"Failed to delete queue {queue_name}: {exc}")


def run_worker(broker_url, backend_url, queue, simulate_failure, result_queue):
    app = Celery("race_worker", broker=broker_url, backend=backend_url)

    app.conf.update(
        task_queues=[queue],
        worker_detect_quorum_queues=True,
        task_default_queue=queue.name,
        task_default_exchange=queue.exchange.name,
        task_default_exchange_type=queue.exchange.type,
        task_default_routing_key=queue.routing_key,
    )

    @app.task
    def dummy_task():
        return "ok from worker"

    if simulate_failure:
        logger.info(">>> Simulating quorum detection failure <<<")

        def fake_detect_quorum_queues(app, driver):
            return False, queue.name

        consumer_tasks.detect_quorum_queues = fake_detect_quorum_queues

    try:
        with start_worker(
            app, queues=[queue.name], loglevel="info", perform_ping_check=False, shutdown_timeout=30, terminate=True
        ):
            with app.broker_connection() as conn:
                with conn.channel() as channel:
                    maybe_declare(queue.exchange, channel)
                    maybe_declare(queue, channel)

            res = dummy_task.apply_async(queue=queue.name)
            result = res.get(timeout=20)
            result_queue.put(result)
    except Exception as e:
        result_queue.put(str(e))


@pytest.mark.amqp
@pytest.mark.timeout(120)
@pytest.mark.xfail(reason="Expect global QoS errors when quorum queue visibility hasn't propagated.", strict=False)
def test_simulated_rabbitmq_cluster_visibility_race(app_config):
    broker_url, backend_url, queue = app_config

    delete_queue_amqp(queue.name, broker_url)

    results = []

    for i in range(3):
        result_queue = multiprocessing.Queue()
        p = multiprocessing.Process(
            target=run_worker,
            args=(broker_url, backend_url, queue, i == 0, result_queue),
        )
        p.start()
        p.join(timeout=40)

        if p.exitcode != 0:
            p.terminate()
            results.append(f"[worker {i}] terminated or failed")
        else:
            try:
                result = result_queue.get_nowait()
                results.append(result)
            except Exception as e:
                results.append(f"[worker {i}] no result: {e}")

    expected_error = any(
        "NOT_IMPLEMENTED" in str(r) or "failed" in str(r).lower() or "timeout" in str(r).lower() for r in results
    )
    assert expected_error, f"Expected at least one QoS-related failure. Results:\n{results}"
