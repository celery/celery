import json

import pytest
from pytest_celery import RESULT_TIMEOUT, CeleryTestSetup, CeleryTestWorker

from celery.canvas import StampingVisitor, chain
from t.integration.tasks import identity


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
        if len(celery_setup.worker_cluster) < 3:
            pytest.skip("Not enough workers in cluster")

        stamp = {"stamp": 42}

        class CustomStampingVisitor(StampingVisitor):
            def on_signature(self, sig, **headers) -> dict:
                return stamp.copy()

        w1: CeleryTestWorker = celery_setup.worker_cluster[0]
        w2: CeleryTestWorker = celery_setup.worker_cluster[1]
        w3: CeleryTestWorker = celery_setup.worker_cluster[2]
        stamped_task = chain(
            identity.si(4).set(queue=w1.worker_queue),
            identity.si(2).set(queue=w2.worker_queue),
            identity.si(0).set(queue=w3.worker_queue),
        )
        stamped_task.stamp(visitor=CustomStampingVisitor())
        stamped_task.apply_async().get(timeout=RESULT_TIMEOUT)

        stamp = json.dumps(stamp, indent=4)
        worker: CeleryTestWorker
        for worker in celery_setup.worker_cluster:
            assert worker.logs().count(stamp)

    def test_multiple_stamps_multiple_workers(self, celery_setup: CeleryTestSetup):
        if len(celery_setup.worker_cluster) < 3:
            pytest.skip("Not enough workers in cluster")

        stamp = {"stamp": 420}
        stamp1 = {**stamp, "stamp1": 4}
        stamp2 = {**stamp, "stamp2": 2}
        stamp3 = {**stamp, "stamp3": 42}

        w1: CeleryTestWorker = celery_setup.worker_cluster[0]
        w2: CeleryTestWorker = celery_setup.worker_cluster[1]
        w3: CeleryTestWorker = celery_setup.worker_cluster[2]
        stamped_task = chain(
            identity.si(4).set(queue=w1.worker_queue).stamp(stamp1=stamp1["stamp1"]),
            identity.si(2).set(queue=w2.worker_queue).stamp(stamp2=stamp2["stamp2"]),
            identity.si(0).set(queue=w3.worker_queue).stamp(stamp3=stamp3["stamp3"]),
        )
        stamped_task.stamp(stamp=stamp["stamp"])
        stamped_task.apply_async().get(timeout=RESULT_TIMEOUT)

        stamp1 = json.dumps(stamp1, indent=4)
        stamp2 = json.dumps(stamp2, indent=4)
        stamp3 = json.dumps(stamp3, indent=4)

        assert w1.logs().count(stamp1)
        assert w1.logs().count(stamp2) == 0
        assert w1.logs().count(stamp3) == 0

        assert w2.logs().count(stamp1) == 0
        assert w2.logs().count(stamp2)
        assert w2.logs().count(stamp3) == 0

        assert w3.logs().count(stamp1) == 0
        assert w3.logs().count(stamp2) == 0
        assert w3.logs().count(stamp3)
