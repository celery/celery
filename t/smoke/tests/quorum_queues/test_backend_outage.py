"""A short result backend outage on a quorum queue (#8588).

RabbitMQ drops a message after its delivery limit, 20 by default, so a
requeue loop loses tasks here instead of only spinning.
"""
from __future__ import annotations

import pytest
from pytest_celery import CeleryBackendCluster, CeleryTestSetup, RedisTestBackend

from celery import Celery
from t.smoke.tests.test_backend_outage import run_tasks_through_backend_outage


@pytest.fixture
def celery_backend_cluster(celery_redis_backend: RedisTestBackend) -> CeleryBackendCluster:
    cluster = CeleryBackendCluster(celery_redis_backend)
    yield cluster
    cluster.teardown()


@pytest.fixture
def default_worker_app(default_worker_app: Celery) -> Celery:
    app = default_worker_app
    app.conf.task_acks_late = True
    app.conf.task_track_started = True
    app.conf.worker_prefetch_multiplier = 1
    app.conf.worker_concurrency = 1
    return app


class test_backend_outage_on_quorum_queue:
    def test_tasks_survive_a_short_backend_outage(self, celery_setup: CeleryTestSetup):
        run_tasks_through_backend_outage(celery_setup)
