from __future__ import annotations

import pytest
from pytest_celery import CeleryTestSetup

from celery import group
from t.smoke.tasks import long_running_task

RESULT_TIMEOUT = 30


class test_autoscale_keepalive:
    @pytest.fixture
    def default_worker_command(self, default_worker_container_cls):
        return default_worker_container_cls.command("--pool=prefork", "--autoscale=2,1")

    @pytest.fixture
    def default_worker_env(self, default_worker_env: dict) -> dict:
        default_worker_env["AUTOSCALE_KEEPALIVE"] = "5"
        return default_worker_env

    def test_short_gaps_do_not_replace_pool_processes(self, celery_setup: CeleryTestSetup):
        worker = celery_setup.worker
        queue = worker.worker_queue
        inspector = celery_setup.app.control.inspect(
            destination=[worker.hostname()], timeout=RESULT_TIMEOUT,
        )

        burst = group(long_running_task.si(2).set(queue=queue) for _ in range(2))
        assert burst.delay().get(timeout=RESULT_TIMEOUT) == [True, True]
        stats = inspector.stats()
        assert stats is not None
        processes = set(stats[worker.hostname()]["pool"]["processes"])
        assert len(processes) == 2

        # Receipts continue beyond the keepalive measured from the initial growth.
        # Each gap is short enough that the same processes should handle the next burst.
        for _ in range(3):
            assert burst.delay().get(timeout=RESULT_TIMEOUT) == [True, True]
            stats = inspector.stats()
            assert stats is not None
            assert set(stats[worker.hostname()]["pool"]["processes"]) == processes
