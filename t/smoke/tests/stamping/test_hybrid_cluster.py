from __future__ import annotations

import json

import pytest
from pytest_celery import RESULT_TIMEOUT, CeleryTestSetup, CeleryTestWorker, CeleryWorkerCluster

from celery.canvas import StampingVisitor, chain
from t.integration.tasks import StampOnReplace, identity, replace_with_stamped_task


def get_hybrid_clusters_matrix() -> list[list[str]]:
    """Returns a matrix of hybrid worker clusters

    Each item in the matrix is a list of workers to be used in the cluster
    and each cluster will be tested separately (with parallel support)
    """

    return [
        # Dev worker only
        ["celery_setup_worker"],
        # Legacy (Celery 4) worker only
        ["celery_legacy_worker"],
        # Both dev and legacy workers
        ["celery_setup_worker", "celery_legacy_worker"],
        # Dev worker and last official Celery release worker
        ["celery_setup_worker", "celery_latest_worker"],
        # Dev worker and legacy worker and last official Celery release worker
        ["celery_setup_worker", "celery_latest_worker", "celery_legacy_worker"],
    ]


@pytest.fixture(params=get_hybrid_clusters_matrix())
def celery_worker_cluster(request: pytest.FixtureRequest) -> CeleryWorkerCluster:
    nodes: tuple[CeleryTestWorker] = [
        request.getfixturevalue(worker) for worker in request.param
    ]
    cluster = CeleryWorkerCluster(*nodes)
    yield cluster
    cluster.teardown()


class test_stamping_hybrid_worker_cluster:
    def test_sanity(self, celery_setup: CeleryTestSetup):
        stamp = {"stamp": 42}

        class CustomStampingVisitor(StampingVisitor):
            def on_signature(self, sig, **headers) -> dict:
                return stamp.copy()

        worker: CeleryTestWorker
        for worker in celery_setup.worker_cluster:
            queue = worker.worker_queue
            stamped_task = identity.si(123)
            stamped_task.stamp(visitor=CustomStampingVisitor())
            assert stamped_task.apply_async(queue=queue).get(timeout=RESULT_TIMEOUT)
            assert worker.logs().count(json.dumps(stamp, indent=4, sort_keys=True))

    def test_sanity_worker_hop(self, celery_setup: CeleryTestSetup):
        if len(celery_setup.worker_cluster) < 2:
            pytest.skip("Not enough workers in cluster")

        stamp = {"stamp": 42}

        class CustomStampingVisitor(StampingVisitor):
            def on_signature(self, sig, **headers) -> dict:
                return stamp.copy()

        w1: CeleryTestWorker = celery_setup.worker_cluster[0]
        w2: CeleryTestWorker = celery_setup.worker_cluster[1]
        stamped_task = chain(
            identity.si(4).set(queue=w1.worker_queue),
            identity.si(2).set(queue=w2.worker_queue),
        )
        stamped_task.stamp(visitor=CustomStampingVisitor())
        stamped_task.apply_async().get(timeout=RESULT_TIMEOUT)

        stamp = json.dumps(stamp, indent=4)
        worker: CeleryTestWorker
        for worker in (w1, w2):
            assert worker.logs().count(stamp)

    def test_multiple_stamps_multiple_workers(self, celery_setup: CeleryTestSetup):
        if len(celery_setup.worker_cluster) < 2:
            pytest.skip("Not enough workers in cluster")

        stamp = {"stamp": 420}
        stamp1 = {**stamp, "stamp1": 4}
        stamp2 = {**stamp, "stamp2": 2}

        w1: CeleryTestWorker = celery_setup.worker_cluster[0]
        w2: CeleryTestWorker = celery_setup.worker_cluster[1]
        stamped_task = chain(
            identity.si(4).set(queue=w1.worker_queue).stamp(stamp1=stamp1["stamp1"]),
            identity.si(2).set(queue=w2.worker_queue).stamp(stamp2=stamp2["stamp2"]),
        )
        stamped_task.stamp(stamp=stamp["stamp"])
        stamped_task.apply_async().get(timeout=RESULT_TIMEOUT)

        stamp1 = json.dumps(stamp1, indent=4)
        stamp2 = json.dumps(stamp2, indent=4)

        assert w1.logs().count(stamp1)
        assert w1.logs().count(stamp2) == 0

        assert w2.logs().count(stamp1) == 0
        assert w2.logs().count(stamp2)

    def test_stamping_on_replace_with_legacy_worker_in_cluster(
        self,
        celery_setup: CeleryTestSetup,
        dev_worker: CeleryTestWorker,
        legacy_worker: CeleryTestWorker,
    ):
        if len(celery_setup.worker_cluster) < 2:
            pytest.skip("Not enough workers in cluster")

        if not dev_worker:
            pytest.skip("Dev worker not in cluster")

        if not legacy_worker:
            pytest.skip("Legacy worker not in cluster")

        stamp = {"stamp": "Only for dev worker tasks"}
        stamp1 = {**StampOnReplace.stamp, "stamp1": "1) Only for legacy worker tasks"}
        stamp2 = {**StampOnReplace.stamp, "stamp2": "2) Only for legacy worker tasks"}

        replaced_sig1 = (
            identity.si(4)
            .set(queue=legacy_worker.worker_queue)
            .stamp(stamp1=stamp1["stamp1"])
        )
        replaced_sig2 = (
            identity.si(2)
            .set(queue=legacy_worker.worker_queue)
            .stamp(stamp2=stamp2["stamp2"])
        )

        stamped_task = chain(
            replace_with_stamped_task.si(replace_with=replaced_sig1).set(
                queue=dev_worker.worker_queue
            ),
            replace_with_stamped_task.si(replace_with=replaced_sig2).set(
                queue=dev_worker.worker_queue
            ),
        )
        stamped_task.stamp(stamp=stamp["stamp"])
        stamped_task.apply_async().get(timeout=RESULT_TIMEOUT)

        stamp = json.dumps(stamp, indent=4)
        stamp1 = json.dumps(stamp1, indent=4)
        stamp2 = json.dumps(stamp2, indent=4)

        assert dev_worker.logs().count(stamp)
        assert dev_worker.logs().count(stamp1) == 0
        assert dev_worker.logs().count(stamp2) == 0

        assert legacy_worker.logs().count(stamp) == 0
        assert legacy_worker.logs().count(stamp1)
        assert legacy_worker.logs().count(stamp2)
