import pytest
from pytest_celery import RESULT_TIMEOUT, CeleryTestSetup, CeleryTestWorker, CeleryWorkerCluster

from celery import signature
from t.integration.tasks import add, identity
from t.smoke.tasks import replace_with_task


class test_replace:
    @pytest.fixture
    def celery_worker_cluster(
        self,
        celery_worker: CeleryTestWorker,
        celery_latest_worker: CeleryTestWorker,
    ) -> CeleryWorkerCluster:
        cluster = CeleryWorkerCluster(celery_worker, celery_latest_worker)
        yield cluster
        cluster.teardown()

    def test_sanity(self, celery_setup: CeleryTestSetup):
        queues = [w.worker_queue for w in celery_setup.worker_cluster]
        assert len(queues) == 2
        assert queues[0] != queues[1]
        replace_with = signature(identity, args=(40,), queue=queues[1])
        sig1 = replace_with_task.s(replace_with)
        sig2 = add.s(2).set(queue=queues[1])
        c = sig1 | sig2
        r = c.apply_async(queue=queues[0])
        assert r.get(timeout=RESULT_TIMEOUT) == 42
