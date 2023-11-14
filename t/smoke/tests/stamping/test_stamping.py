import json

from pytest_celery import RESULT_TIMEOUT, CeleryTestSetup, CeleryTestWorker

from celery.canvas import StampingVisitor
from t.integration.tasks import identity
from t.smoke.workers.legacy import CeleryLegacyWorkerContainer


class test_stamping:
    def test_sanity(self, celery_setup: CeleryTestSetup):
        on_signature = {"stamp": 42}

        class CustomStampingVisitor(StampingVisitor):
            def on_signature(self, sig, **headers) -> dict:
                return on_signature.copy()

        stamped_task = identity.si(123)
        stamped_task.stamp(visitor=CustomStampingVisitor())

        worker: CeleryTestWorker
        for worker in celery_setup.worker_cluster:
            stamped_task.apply_async(queue=worker.worker_queue).get(
                timeout=RESULT_TIMEOUT
            )
            if worker.worker_name == CeleryLegacyWorkerContainer.worker_name():
                assert worker.logs().count("No stamps found")
            else:
                assert worker.logs().count(json.dumps(on_signature, indent=4))
