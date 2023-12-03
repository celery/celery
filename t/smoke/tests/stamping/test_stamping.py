from __future__ import annotations

import json

import pytest
from pytest_celery import (RESULT_TIMEOUT, CeleryBackendCluster, CeleryTestSetup, CeleryTestWorker,
                           CeleryWorkerCluster)

from celery.canvas import Signature, StampingVisitor, chain
from celery.result import AsyncResult
from t.integration.tasks import StampOnReplace, add, identity, replace_with_stamped_task
from t.smoke.tests.stamping.tasks import wait_for_revoke
from t.smoke.workers.dev import SmokeWorkerContainer
from t.smoke.workers.legacy import CeleryLegacyWorkerContainer


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
        if worker.version == CeleryLegacyWorkerContainer.version():
            return worker
    return None


class test_stamping:
    def test_callback(self, dev_worker: CeleryTestWorker):
        on_signature_stamp = {"on_signature_stamp": 4}
        no_visitor_stamp = {"no_visitor_stamp": "Stamp without visitor"}
        on_callback_stamp = {"on_callback_stamp": 2}
        link_stamp = {
            **on_signature_stamp,
            **no_visitor_stamp,
            **on_callback_stamp,
        }

        class CustomStampingVisitor(StampingVisitor):
            def on_signature(self, sig, **headers) -> dict:
                return on_signature_stamp.copy()

            def on_callback(self, callback, **header) -> dict:
                return on_callback_stamp.copy()

        stamped_task = identity.si(123).set(queue=dev_worker.worker_queue)
        stamped_task.link(
            add.s(0)
            .stamp(no_visitor_stamp=no_visitor_stamp["no_visitor_stamp"])
            .set(queue=dev_worker.worker_queue)
        )
        stamped_task.stamp(visitor=CustomStampingVisitor())
        stamped_task.delay().get(timeout=RESULT_TIMEOUT)
        assert dev_worker.logs().count(
            json.dumps(on_signature_stamp, indent=4, sort_keys=True)
        )
        assert dev_worker.logs().count(json.dumps(link_stamp, indent=4, sort_keys=True))


class test_stamping_hybrid_worker_cluster:
    @pytest.fixture(
        # Each param item is a list of workers to be used in the cluster
        # and each cluster will be tested separately (with parallel support)
        params=[
            ["celery_setup_worker"],
            ["celery_legacy_worker"],
            ["celery_setup_worker", "celery_legacy_worker"],
            ["celery_setup_worker", "celery_latest_worker", "celery_legacy_worker"],
        ]
    )
    def celery_worker_cluster(
        self,
        request: pytest.FixtureRequest,
    ) -> CeleryWorkerCluster:
        nodes: tuple[CeleryTestWorker] = [
            request.getfixturevalue(worker) for worker in request.param
        ]
        cluster = CeleryWorkerCluster(*nodes)
        yield cluster
        cluster.teardown()

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


class test_revoke_by_stamped_headers:
    @pytest.fixture
    def celery_worker_cluster(
        self,
        celery_worker: CeleryTestWorker,
        celery_latest_worker: CeleryTestWorker,
    ) -> CeleryWorkerCluster:
        cluster = CeleryWorkerCluster(celery_worker, celery_latest_worker)
        yield cluster
        cluster.teardown()

    @pytest.fixture
    def celery_backend_cluster(self) -> CeleryBackendCluster:
        # Disable backend
        return None

    @pytest.fixture
    def wait_for_revoke_timeout(self) -> int:
        return 4

    @pytest.fixture
    def canvas(
        self,
        dev_worker: CeleryTestWorker,
        wait_for_revoke_timeout: int,
    ) -> Signature:
        return chain(
            identity.s(wait_for_revoke_timeout),
            wait_for_revoke.s(waitfor_worker_queue=dev_worker.worker_queue).set(
                queue=dev_worker.worker_queue
            ),
        )

    def test_revoke_by_stamped_headers_after_publish(
        self,
        dev_worker: CeleryTestWorker,
        celery_latest_worker: CeleryTestWorker,
        wait_for_revoke_timeout: int,
        canvas: Signature,
    ):
        result: AsyncResult = canvas.apply_async(
            queue=celery_latest_worker.worker_queue
        )
        result.revoke_by_stamped_headers(StampOnReplace.stamp, terminate=True)
        dev_worker.assert_log_does_not_exist(
            "Done waiting",
            timeout=wait_for_revoke_timeout,
        )

    def test_revoke_by_stamped_headers_before_publish(
        self,
        dev_worker: CeleryTestWorker,
        celery_latest_worker: CeleryTestWorker,
        canvas: Signature,
    ):
        dev_worker.app.control.revoke_by_stamped_headers(
            StampOnReplace.stamp,
            terminate=True,
        )
        canvas.apply_async(queue=celery_latest_worker.worker_queue)
        dev_worker.assert_log_exists("Discarding revoked task")
        dev_worker.assert_log_exists(f"revoked by header: {StampOnReplace.stamp}")
