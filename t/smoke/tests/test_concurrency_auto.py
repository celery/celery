"""Smoke tests for ``--concurrency=auto`` (cgroup-aware concurrency).

These tests run real workers in Docker containers with real CFS quotas
(``cpu_quota``/``cpu_period`` on the container), so the cgroup files read by
``celery.utils.sysinfo.cpu_budget()`` are written by the kernel —
nothing is mocked. They complement the unit tests in
``t/unit/utils/test_sysinfo.py``, which exercise the parsing logic against
synthetic files.
"""

from __future__ import annotations

import os
from time import sleep

import pytest
from pytest_celery import CeleryTestSetup, CeleryTestWorker, CeleryWorkerContainer, defaults
from pytest_docker_tools import container, fxtr

from t.smoke.workers.dev import SmokeWorkerContainer

RESULT_TIMEOUT = 30
CPU_PERIOD = 100_000


@pytest.fixture
def worker_cpu_quota() -> int:
    """CFS quota (µs per period) applied to the worker container.

    Defaults to 2 CPUs; override per test class. ``-1`` means no quota
    (``cpu.max = max``).
    """
    return 2 * CPU_PERIOD


@pytest.fixture
def worker_cpuset_cpus() -> str | None:
    """CPU affinity applied to the worker container (``--cpuset-cpus``).

    None means unpinned; override per test class.
    """
    return None


# Same container as t/smoke/workers/dev.py, plus a CFS quota. Module-level
# definition shadows the default worker container for tests in this file only.
default_worker_container = container(
    image="{celery_dev_worker_image.id}",
    ports=fxtr("default_worker_ports"),
    environment=fxtr("default_worker_env"),
    network="{default_pytest_celery_network.name}",
    volumes={
        "{default_worker_volume.name}": defaults.DEFAULT_WORKER_VOLUME,
        os.path.abspath(os.getcwd()): {
            "bind": "/celery",
            "mode": "rw",
        },
    },
    wrapper_class=SmokeWorkerContainer,
    timeout=defaults.DEFAULT_WORKER_CONTAINER_TIMEOUT,
    command=fxtr("default_worker_command"),
    cpu_quota=fxtr("worker_cpu_quota"),
    cpu_period=CPU_PERIOD,
    cpuset_cpus=fxtr("worker_cpuset_cpus"),
)


@pytest.fixture
def default_worker_container_cls() -> type[CeleryWorkerContainer]:
    return SmokeWorkerContainer


@pytest.fixture(scope="session")
def default_worker_container_session_cls() -> type[CeleryWorkerContainer]:
    return SmokeWorkerContainer


def container_cpu_count(worker: CeleryTestWorker) -> int:
    """CPUs in the affinity mask inside the worker container (host CPUs
    unless ``--cpuset-cpus`` is set; unaffected by the CFS quota)."""
    exit_code, output = worker.container.exec_run(
        'python -c "import os; print(len(os.sched_getaffinity(0)))"'
    )
    assert exit_code == 0, output
    return int(output.decode().strip())


def get_pool_stats(setup: CeleryTestSetup, worker: CeleryTestWorker, attempts: int = 10) -> dict:
    """Fetch the worker's pool stats over the control channel, with retries
    while the worker finishes booting."""
    last = None
    for _ in range(attempts):
        last = setup.app.control.inspect(
            destination=[worker.hostname()], timeout=3
        ).stats()
        if last and worker.hostname() in last:
            return last[worker.hostname()]["pool"]
        sleep(1)
    raise AssertionError(f"No stats reply from {worker.hostname()}: {last!r}")


class test_auto_prefork_with_quota:
    """quota=2 CPUs + prefork: pool must size to the quota, not the host."""

    @pytest.fixture
    def default_worker_command(self, default_worker_container_cls: type[CeleryWorkerContainer]) -> list[str]:
        return default_worker_container_cls.command("--concurrency=auto", "--pool=prefork")

    def test_pool_size_matches_cgroup_quota(self, celery_setup: CeleryTestSetup):
        worker = celery_setup.worker
        expected = min(container_cpu_count(worker), 2)
        worker.assert_log_exists(
            f"worker_concurrency='auto' resolved to {expected} (pool=prefork, cgroup cpu quota=2.00",
            timeout=RESULT_TIMEOUT,
        )
        pool = get_pool_stats(celery_setup, worker)
        assert pool["max-concurrency"] == expected
        assert len(pool["processes"]) == expected


class test_auto_prefork_fractional_quota:
    """quota=0.5 CPU + prefork: ceil() to 1 process, never 0."""

    @pytest.fixture
    def worker_cpu_quota(self) -> int:
        return CPU_PERIOD // 2

    @pytest.fixture
    def default_worker_command(self, default_worker_container_cls: type[CeleryWorkerContainer]) -> list[str]:
        return default_worker_container_cls.command("--concurrency=auto", "--pool=prefork")

    def test_fractional_quota_rounds_up_to_one(self, celery_setup: CeleryTestSetup):
        worker = celery_setup.worker
        worker.assert_log_exists(
            "worker_concurrency='auto' resolved to 1 (pool=prefork, cgroup cpu quota=0.50",
            timeout=RESULT_TIMEOUT,
        )
        pool = get_pool_stats(celery_setup, worker)
        assert pool["max-concurrency"] == 1
        assert len(pool["processes"]) == 1


class test_auto_prefork_no_quota:
    """No CFS quota (``cpu.max = max``): auto falls back to host CPU count."""

    @pytest.fixture
    def worker_cpu_quota(self) -> int:
        return -1  # Docker: no quota

    @pytest.fixture
    def default_worker_command(self, default_worker_container_cls: type[CeleryWorkerContainer]) -> list[str]:
        return default_worker_container_cls.command("--concurrency=auto", "--pool=prefork")

    def test_no_quota_falls_back_to_host_cpu_count(self, celery_setup: CeleryTestSetup):
        worker = celery_setup.worker
        expected = container_cpu_count(worker)
        worker.assert_log_exists(
            f"worker_concurrency='auto' resolved to {expected} (pool=prefork, no cgroup cpu quota found",
            timeout=RESULT_TIMEOUT,
        )
        pool = get_pool_stats(celery_setup, worker)
        assert pool["max-concurrency"] == expected


class test_auto_prefork_cpuset_no_quota:
    """No CFS quota but pinned to one core (``--cpuset-cpus=0``): auto must
    size from the affinity mask, not the host CPU count."""

    @pytest.fixture
    def worker_cpu_quota(self) -> int:
        return -1

    @pytest.fixture
    def worker_cpuset_cpus(self) -> str:
        return "0"

    @pytest.fixture
    def default_worker_command(self, default_worker_container_cls: type[CeleryWorkerContainer]) -> list[str]:
        return default_worker_container_cls.command("--concurrency=auto", "--pool=prefork")

    def test_cpuset_caps_pool_size(self, celery_setup: CeleryTestSetup):
        worker = celery_setup.worker
        worker.assert_log_exists(
            "worker_concurrency='auto' resolved to 1 (pool=prefork, no cgroup cpu quota found, available cpus=1)",
            timeout=RESULT_TIMEOUT,
        )
        pool = get_pool_stats(celery_setup, worker)
        assert pool["max-concurrency"] == 1
        assert len(pool["processes"]) == 1


class test_auto_threads_noop:
    """quota=2 CPUs + threads pool: auto is a documented no-op for
    non-CPU-bound pools and must NOT cap to the quota."""

    @pytest.fixture
    def default_worker_command(self, default_worker_container_cls: type[CeleryWorkerContainer]) -> list[str]:
        return default_worker_container_cls.command("--concurrency=auto", "--pool=threads")

    def test_threads_pool_ignores_quota(self, celery_setup: CeleryTestSetup):
        worker = celery_setup.worker
        worker.assert_log_exists(
            "worker_concurrency='auto' only sizes the prefork pool",
            timeout=RESULT_TIMEOUT,
        )
        expected = container_cpu_count(worker)
        pool = get_pool_stats(celery_setup, worker)
        assert pool["max-concurrency"] == expected


class test_auto_solo_noop:
    """quota=2 CPUs + solo pool: solo runs one task at a time, so auto is a
    documented no-op and must NOT consult the quota."""

    @pytest.fixture
    def default_worker_command(self, default_worker_container_cls: type[CeleryWorkerContainer]) -> list[str]:
        return default_worker_container_cls.command("--concurrency=auto", "--pool=solo")

    def test_solo_pool_is_noop(self, celery_setup: CeleryTestSetup):
        worker = celery_setup.worker
        worker.assert_log_exists(
            "worker_concurrency='auto' only sizes the prefork pool",
            timeout=RESULT_TIMEOUT,
        )
        worker.assert_log_does_not_exist("worker_concurrency='auto' resolved to")
        pool = get_pool_stats(celery_setup, worker)
        assert pool["max-concurrency"] == 1


class test_default_concurrency_unchanged:
    """Regression guard: without ``auto``, a quota'd worker still sizes the
    pool from the host CPU count exactly as before this feature."""

    @pytest.fixture
    def worker_cpu_quota(self) -> int:
        return CPU_PERIOD  # 1 CPU — distinguishable from any multi-CPU host

    def test_default_ignores_cgroup_quota(self, celery_setup: CeleryTestSetup):
        worker = celery_setup.worker
        host_cpus = container_cpu_count(worker)
        if host_cpus < 2:
            pytest.skip("Need >1 host CPU to distinguish quota from host count")
        worker.assert_log_does_not_exist("worker_concurrency='auto'")
        pool = get_pool_stats(celery_setup, worker)
        assert pool["max-concurrency"] == host_cpus


class test_auto_explicit_int_quota_interaction:
    """An explicit integer always wins over the quota — auto only applies
    when requested."""

    @pytest.fixture
    def default_worker_command(self, default_worker_container_cls: type[CeleryWorkerContainer]) -> list[str]:
        return default_worker_container_cls.command("--concurrency=3", "--pool=prefork")

    def test_explicit_int_overrides_quota(self, celery_setup: CeleryTestSetup):
        worker = celery_setup.worker
        worker.assert_log_does_not_exist("worker_concurrency='auto'")
        pool = get_pool_stats(celery_setup, worker)
        assert pool["max-concurrency"] == 3
        assert len(pool["processes"]) == 3
