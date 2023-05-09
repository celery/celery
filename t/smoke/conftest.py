from typing import Tuple

import pytest
from pytest_celery.api.components.worker.cluster import CeleryWorkerCluster
from pytest_celery.api.components.worker.node import CeleryTestWorker

from t.smoke.common import *  # noqa


@pytest.fixture(
    # Each param item is a list of workers to be used in the cluster
    params=[
        ["celery_setup_worker"],
        ["celery_setup_worker", "celery4_worker"],
    ]
)
def celery_worker_cluster(request: pytest.FixtureRequest) -> CeleryWorkerCluster:
    nodes: Tuple[CeleryTestWorker] = [request.getfixturevalue(worker) for worker in request.param]
    cluster = CeleryWorkerCluster(*nodes)
    yield cluster
    cluster.teardown()
