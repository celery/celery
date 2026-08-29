from __future__ import annotations

import pytest
from pytest_celery import CeleryBackendCluster, CeleryTestWorker, CeleryWorkerCluster

from celery.canvas import Signature, chain
from celery.result import AsyncResult
from t.integration.tasks import StampOnReplace, identity
from t.smoke.tests.stamping.tasks import wait_for_revoke


@pytest.fixture
def celery_worker_cluster(
    celery_worker: CeleryTestWorker,
    celery_latest_worker: CeleryTestWorker,
) -> CeleryWorkerCluster:
    cluster = CeleryWorkerCluster(celery_worker, celery_latest_worker)
    yield cluster
    cluster.teardown()


@pytest.fixture
def celery_backend_cluster() -> CeleryBackendCluster:
    # Disable backend
    return None


@pytest.fixture
def wait_for_revoke_timeout() -> int:
    return 4


@pytest.fixture
def canvas(
    dev_worker: CeleryTestWorker,
    wait_for_revoke_timeout: int,
) -> Signature:
    return chain(
        identity.s(wait_for_revoke_timeout),
        wait_for_revoke.s(waitfor_worker_queue=dev_worker.worker_queue).set(
            queue=dev_worker.worker_queue
        ),
    )


class test_revoke_by_stamped_headers:
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
