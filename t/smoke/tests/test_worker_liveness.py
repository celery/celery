"""The worker liveness file, checked from inside the worker's container.

The checks run the liveness probe from the workers guide.
"""
from __future__ import annotations

from time import monotonic, sleep

import pytest
from pytest_celery import CeleryTestSetup, CeleryWorkerContainer
from tenacity import retry, stop_after_delay, wait_fixed

from celery import Celery
from celery.result import AsyncResult
from t.smoke.tasks import long_running_task

LIVENESS_FILE = "/tmp/celery-liveness"
INTERVAL = 2
# The guide's probe allows two intervals. ``date`` and ``stat`` count whole
# seconds, which at an interval this short can add one more.
STALE_AFTER = 2 * INTERVAL + 1
PROBE = f"test $(( $(date +%s) - $(stat -c %Y {LIVENESS_FILE}) )) -lt {STALE_AFTER}"
# A frozen worker or broker touched the file at most an interval ago. The
# extra seconds are for docker exec.
STALE_WITHIN = STALE_AFTER + INTERVAL + 5
# Long enough that a worker that can't read its probe stays stale for most of
# a task, far past the few failed checks assert_probe_keeps_passing allows.
TASK_SECONDS = 20
# Check while the first task runs. Stopping early leaves time to finish a run
# of failed checks and to read the logs before it ends.
BUSY_SECONDS = TASK_SECONDS - 3 * INTERVAL


@pytest.fixture
def default_worker_app(default_worker_app: Celery) -> Celery:
    app = default_worker_app
    app.conf.worker_liveness_file = LIVENESS_FILE
    app.conf.worker_liveness_interval = INTERVAL
    app.conf.worker_prefetch_multiplier = 1
    return app


@pytest.fixture
def celery_backend_cluster() -> None:
    return None


def probe(celery_setup: CeleryTestSetup) -> tuple[bool, bytes]:
    exit_code, output = celery_setup.worker.container.exec_run(["/bin/sh", "-c", PROBE])
    return exit_code == 0, output


def wait_for_probe(celery_setup: CeleryTestSetup, passes: bool, seconds: float = 60) -> None:
    @retry(stop=stop_after_delay(seconds), wait=wait_fixed(0.5), reraise=True)
    def check():
        passed, output = probe(celery_setup)
        assert passed is passes, f"probe {'failed' if passes else 'passed'}: {output!r}"

    check()


def assert_probe_keeps_passing(celery_setup: CeleryTestSetup, seconds: float) -> None:
    # A late probe or a slow ``docker exec`` can fail a few checks in a row.
    # A worker that can't read its probe fails them until its task ends.
    failing_since = None
    deadline = monotonic() + seconds
    # Don't stop in the middle of a run of failed checks.
    while monotonic() < deadline or failing_since is not None:
        passed, output = probe(celery_setup)
        if passed:
            failing_since = None
        else:
            if failing_since is None:
                failing_since = monotonic()
            assert monotonic() - failing_since < 2 * INTERVAL, f"probe kept failing: {output!r}"
        sleep(0.5)


def keep_the_worker_busy(celery_setup: CeleryTestSetup) -> list[AsyncResult]:
    # The worker has one slot, so the second task waits for the first.
    queue = celery_setup.worker.worker_queue
    results = [long_running_task.s(TASK_SECONDS).apply_async(queue=queue) for _ in range(2)]
    celery_setup.worker.assert_log_exists("Starting long running task")
    return results


class test_worker_liveness:

    @pytest.fixture
    def default_worker_app(self, default_worker_app: Celery) -> Celery:
        # With late acks the running task fills the prefetch window, so the
        # task channel takes no messages until it ends.
        default_worker_app.conf.task_acks_late = True
        return default_worker_app

    @pytest.fixture
    def default_worker_command(self, default_worker_container_cls: type[CeleryWorkerContainer]) -> list[str]:
        return default_worker_container_cls.command("--pool=prefork", "--concurrency=1")

    def test_probe_passes_while_the_prefetch_window_is_full(self, celery_setup: CeleryTestSetup):
        wait_for_probe(celery_setup, passes=True)
        _, second = keep_the_worker_busy(celery_setup)
        assert_probe_keeps_passing(celery_setup, seconds=BUSY_SECONDS)
        assert f"[{second.id}] received" not in celery_setup.worker.logs()

    def test_probe_fails_while_the_broker_is_frozen(self, celery_setup: CeleryTestSetup):
        wait_for_probe(celery_setup, passes=True)
        celery_setup.broker.container.kill(signal="SIGSTOP")
        try:
            wait_for_probe(celery_setup, passes=False, seconds=STALE_WITHIN)
        finally:
            celery_setup.broker.container.kill(signal="SIGCONT")
        wait_for_probe(celery_setup, passes=True)

    def test_probe_fails_while_the_worker_is_frozen(self, celery_setup: CeleryTestSetup):
        wait_for_probe(celery_setup, passes=True)
        celery_setup.worker.container.kill(signal="SIGSTOP")
        try:
            wait_for_probe(celery_setup, passes=False, seconds=STALE_WITHIN)
        finally:
            celery_setup.worker.container.kill(signal="SIGCONT")
        wait_for_probe(celery_setup, passes=True)


class test_worker_liveness_gevent:

    @pytest.fixture
    def default_worker_command(self, default_worker_container_cls: type[CeleryWorkerContainer]) -> list[str]:
        return default_worker_container_cls.command("--pool=gevent", "--concurrency=1")

    def test_probe_passes_while_a_task_waits_for_the_pool(self, celery_setup: CeleryTestSetup):
        # With early acks the second task is delivered while the first runs,
        # and handing it to the full pool blocks the event loop until the
        # first task ends.
        wait_for_probe(celery_setup, passes=True)
        first, second = keep_the_worker_busy(celery_setup)
        assert_probe_keeps_passing(celery_setup, seconds=BUSY_SECONDS)
        logs = celery_setup.worker.logs()
        assert f"[{second.id}] received" in logs
        assert f"[{first.id}] succeeded" not in logs
