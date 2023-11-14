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
