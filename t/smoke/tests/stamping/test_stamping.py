import json

import pytest
from pytest_celery import RESULT_TIMEOUT, CeleryTestSetup, CeleryTestWorker

from celery.canvas import StampingVisitor, chain
from t.integration.tasks import StampOnReplace, identity, replace_with_stamped_task
from t.smoke.workers.dev import SmokeWorkerContainer
from t.smoke.workers.legacy import CeleryLegacyWorkerContainer


class test_stamping:
    def test_sanity(self, celery_setup: CeleryTestSetup):
        stamp = {"stamp": 42}

        class CustomStampingVisitor(StampingVisitor):
            def on_signature(self, sig, **headers) -> dict:
                return stamp.copy()

        stamped_task = identity.si(123)
        stamped_task.stamp(visitor=CustomStampingVisitor())

        worker: CeleryTestWorker
        for worker in celery_setup.worker_cluster:
            queue = worker.worker_queue
            stamped_task.apply_async(queue=queue).get(timeout=RESULT_TIMEOUT)
            assert worker.logs().count(json.dumps(stamp, indent=4))

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
        for worker in celery_setup.worker_cluster:
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
    ):
        if len(celery_setup.worker_cluster) < 2:
            pytest.skip("Not enough workers in cluster")

        stamp = {"stamp": "Only for dev worker tasks"}
        stamp1 = {**StampOnReplace.stamp, "stamp1": "1) Only for legacy worker tasks"}
        stamp2 = {**StampOnReplace.stamp, "stamp2": "2) Only for legacy worker tasks"}

        worker: CeleryTestWorker
        for worker in celery_setup.worker_cluster:
            if worker.version == SmokeWorkerContainer.version():
                dev_worker = worker
            elif worker.version == CeleryLegacyWorkerContainer.version():
                legacy_worker = worker

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
