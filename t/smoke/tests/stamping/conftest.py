from typing import Tuple

import pytest
from pytest_celery import CeleryTestWorker, CeleryWorkerCluster


@pytest.fixture(
    # Each param item is a list of workers to be used in the cluster
    params=[
        # ["celery_setup_worker"],
        ["celery_setup_worker", "celery_legacy_worker", "celery_latest_worker"],
    ]
)
def celery_worker_cluster(request: pytest.FixtureRequest) -> CeleryWorkerCluster:
    nodes: Tuple[CeleryTestWorker] = [request.getfixturevalue(worker) for worker in request.param]
    cluster = CeleryWorkerCluster(*nodes)
    yield cluster
    cluster.teardown()
