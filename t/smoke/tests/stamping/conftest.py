import pytest
from pytest_celery import CeleryTestSetup, CeleryTestWorker

from t.smoke.tests.stamping.workers.legacy import *  # noqa
from t.smoke.tests.stamping.workers.legacy import LegacyWorkerContainer
from t.smoke.workers.dev import SmokeWorkerContainer


@pytest.fixture
def default_rabbitmq_broker_image() -> str:
    # Celery 4 doesn't support RabbitMQ 4 due to:
    # https://github.com/celery/kombu/pull/2098
    return "rabbitmq:3"


@pytest.fixture
def default_worker_tasks(default_worker_tasks: set) -> set:
    from t.smoke.tests.stamping import tasks as stamping_tasks

    default_worker_tasks.add(stamping_tasks)
    return default_worker_tasks


@pytest.fixture
def default_worker_signals(default_worker_signals: set) -> set:
    from t.smoke.tests.stamping import signals

    default_worker_signals.add(signals)
    return default_worker_signals


@pytest.fixture
def dev_worker(celery_setup: CeleryTestSetup) -> CeleryTestWorker:
    worker: CeleryTestWorker
    for worker in celery_setup.worker_cluster:
        if worker.version == SmokeWorkerContainer.version():
            return worker
    return None


@pytest.fixture
def legacy_worker(celery_setup: CeleryTestSetup) -> CeleryTestWorker:
    worker: CeleryTestWorker
    for worker in celery_setup.worker_cluster:
        if worker.version == LegacyWorkerContainer.version():
            return worker
    return None
