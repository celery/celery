from __future__ import annotations

import uuid

import pytest
from kombu import Exchange, Queue
from pytest_celery import RESULT_TIMEOUT, CeleryBrokerCluster, RabbitMQTestBroker
from tenacity import retry, stop_after_attempt, wait_fixed

from celery import Celery, chord
from t.integration.conftest import get_redis_connection
from t.integration.tasks import add, redis_echo
from t.smoke.workers.dev import SmokeWorkerContainer

CHORD_HEADERS = {"my_header": "my_value"}
CHORD_HEADERS_QUEUE = Queue(
    "chord_headers_queue",
    exchange=Exchange("chord_headers_exchange", type="headers"),
    binding_arguments=CHORD_HEADERS,
)


@pytest.fixture
def celery_broker_cluster(celery_rabbitmq_broker: RabbitMQTestBroker) -> CeleryBrokerCluster:
    cluster = CeleryBrokerCluster(celery_rabbitmq_broker)
    yield cluster
    cluster.teardown()


@pytest.fixture
def celery_backend_cluster() -> None:
    return None


@pytest.fixture
def celery_setup_config(celery_setup_config: dict, tmp_path) -> dict:
    celery_setup_config.update(
        {
            "result_backend": f"file://{tmp_path}",
            "task_create_missing_queues": False,
            "task_default_queue": CHORD_HEADERS_QUEUE.name,
            "task_queues": [CHORD_HEADERS_QUEUE],
        }
    )
    return celery_setup_config


@pytest.fixture
def default_worker_container_cls() -> type[SmokeWorkerContainer]:
    class ChordHeadersWorkerContainer(SmokeWorkerContainer):
        @classmethod
        def worker_queue(cls) -> str:
            return CHORD_HEADERS_QUEUE.name

    return ChordHeadersWorkerContainer


@pytest.fixture
def default_worker_app(default_worker_app: Celery) -> Celery:
    app = default_worker_app
    app.conf.result_backend = "file:///tmp"
    app.conf.task_default_queue = CHORD_HEADERS_QUEUE.name
    return app


class test_chord_headers_exchange:
    @pytest.mark.usefixtures("celery_setup")
    def test_chord_body_runs_when_routed_through_a_headers_exchange(self):
        redis_key = str(uuid.uuid4())
        header = [
            add.s(1, 1).set(headers=CHORD_HEADERS),
            add.s(2, 2).set(headers=CHORD_HEADERS),
        ]
        body = redis_echo.si("chord_body_ran", redis_key).set(headers=CHORD_HEADERS)

        chord(header)(body)

        @retry(
            stop=stop_after_attempt(int(RESULT_TIMEOUT)),
            wait=wait_fixed(1),
            reraise=True,
        )
        def assert_body_ran():
            assert get_redis_connection().lrange(redis_key, 0, -1) == [b"chord_body_ran"]

        assert_body_ran()
