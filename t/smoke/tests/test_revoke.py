"""Revoking a task while one of several workers runs it (#10913)."""
from __future__ import annotations

from time import monotonic, sleep

import pytest
from pytest_celery import RESULT_TIMEOUT, CeleryTestSetup, CeleryTestWorker, CeleryWorkerCluster

from celery import Celery, states
from celery.exceptions import TaskRevokedError
from celery.result import AsyncResult
from t.smoke.tasks import long_running_task


@pytest.fixture
def celery_worker_cluster(
    celery_worker: CeleryTestWorker,
    celery_alt_dev_worker: CeleryTestWorker,
) -> CeleryWorkerCluster:
    cluster = CeleryWorkerCluster(celery_worker, celery_alt_dev_worker)
    yield cluster
    cluster.teardown()


def wait_until_started(result: AsyncResult) -> None:
    deadline = monotonic() + RESULT_TIMEOUT
    while result.app.AsyncResult(result.id).state != states.STARTED:
        assert monotonic() < deadline, f"{result.id} never started"
        sleep(0.1)


def poll_until_ready(result: AsyncResult) -> list[str]:
    """Read the state the way a client does, and return each state seen."""
    seen = []
    deadline = monotonic() + RESULT_TIMEOUT
    while True:
        state = result.state
        if not seen or seen[-1] != state:
            seen.append(state)
        if state in states.READY_STATES:
            return seen
        assert monotonic() < deadline, f"{result.id} never finished, saw {seen}"
        sleep(0.1)


class test_revoke:
    @pytest.fixture
    def default_worker_app(self, default_worker_app: Celery) -> Celery:
        app = default_worker_app
        app.conf.task_track_started = True
        return app

    def test_running_task_is_not_marked_revoked(self, celery_setup: CeleryTestSetup):
        assert len(celery_setup.worker_cluster) > 1
        result = long_running_task.s(5).apply_async(queue=celery_setup.worker.worker_queue)
        wait_until_started(result)

        # Every worker gets the revoke, but only one runs the task. Without
        # terminate the task keeps running, so it must not be reported as
        # REVOKED, and the client that revoked it must get its result.
        result.revoke()
        assert poll_until_ready(result) == [states.STARTED, states.SUCCESS]
        assert result.get(timeout=RESULT_TIMEOUT) is True

    def test_terminated_task_is_marked_revoked(self, celery_setup: CeleryTestSetup):
        assert len(celery_setup.worker_cluster) > 1
        result = long_running_task.s(30).apply_async(queue=celery_setup.worker.worker_queue)
        wait_until_started(result)

        result.revoke(terminate=True)
        seen = poll_until_ready(result)
        assert seen[-1] == states.REVOKED
        assert states.SUCCESS not in seen
        with pytest.raises(TaskRevokedError):
            result.get(timeout=RESULT_TIMEOUT)

    def test_task_not_started_is_marked_revoked(self, celery_setup: CeleryTestSetup):
        assert len(celery_setup.worker_cluster) > 1
        result = long_running_task.s(1).apply_async(
            queue=celery_setup.worker.worker_queue, countdown=30,
        )

        result.revoke()
        seen = poll_until_ready(result)
        assert seen[-1] == states.REVOKED
        assert set(seen) <= {states.PENDING, states.REVOKED}
        with pytest.raises(TaskRevokedError):
            result.get(timeout=RESULT_TIMEOUT)
