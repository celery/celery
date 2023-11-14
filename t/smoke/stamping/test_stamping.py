from pytest_celery import RESULT_TIMEOUT, CeleryTestSetup, CeleryTestWorker

from celery.canvas import signature
from t.integration.tasks import identity


class test_stamping:
    def test_sanity(self, celery_setup: CeleryTestSetup):
        worker: CeleryTestWorker
        for worker in celery_setup.worker_cluster:
            queue = worker.worker_queue
            sig = signature(identity, args=("test_signature",), queue=queue)
            assert sig.delay().get(timeout=RESULT_TIMEOUT) == "test_signature"
