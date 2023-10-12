from typing import Tuple

import pytest
from pytest_celery import CeleryTestWorker, CeleryWorkerCluster

from t.smoke.workers.dev import *  # noqa
from t.smoke.workers.latest import *  # noqa
from t.smoke.workers.legacy import *  # noqa


@pytest.fixture
def default_worker_tasks() -> set:
    from t.integration import tasks as integration_tests_tasks
    from t.smoke import tasks as smoke_tests_tasks

    yield {
        integration_tests_tasks,
        smoke_tests_tasks,
    }


# TODO: Restructure the smoke tests folders so the conftest would define the
# cluster having such a complex cluster for sanity, nothing (e.g normal dev worker via celery_setup_worker)
# and special combos (e.g celery dev worker + celery4 worker)
# The tests define how they use the worker cluster, so the logic is in the tests.
# This way you can define a custom celery test setup for each tests package


@pytest.fixture(
    # Each param item is a list of workers to be used in the cluster
    params=[
        ["celery_setup_worker"],
        ["celery_setup_worker", "celery_latest_worker"],
        ["celery_setup_worker", "celery_legacy_worker"],
        ["celery_setup_worker", "celery_legacy_worker", "celery_latest_worker"],
    ]
)
def celery_worker_cluster(request: pytest.FixtureRequest) -> CeleryWorkerCluster:
    nodes: Tuple[CeleryTestWorker] = [request.getfixturevalue(worker) for worker in request.param]
    cluster = CeleryWorkerCluster(*nodes)
    yield cluster
    cluster.teardown()
