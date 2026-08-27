"""System information utilities."""
from __future__ import annotations

import os
from math import ceil

from kombu.utils.objects import cached_property

__all__ = (
    'load_average', 'df',
    'AUTO_CONCURRENCY', 'is_auto_concurrency', 'effective_cpu_count',
)


#: Sentinel string accepted by ``worker_concurrency`` / ``--concurrency``
#: to request cgroup-aware auto-sizing on Linux.
AUTO_CONCURRENCY = 'auto'


def is_auto_concurrency(value) -> bool:
    """Return True if ``value`` is the ``auto`` concurrency sentinel.

    Accepts case-insensitive ``"auto"`` with surrounding whitespace stripped.
    """
    return (
        isinstance(value, str)
        and value.strip().lower() == AUTO_CONCURRENCY
    )


def _detect_cgroup_cpu_count(fallback: int) -> int:
    """Return CPU count derived from the cgroup CFS quota.

    Reads ``/sys/fs/cgroup/cpu.max`` (cgroup v2) and falls back to
    ``/sys/fs/cgroup/cpu/cpu.cfs_quota_us`` + ``cpu.cfs_period_us``
    (cgroup v1). Returns ``ceil(quota / period)`` clamped to
    ``[1, fallback]``. Returns ``fallback`` when no CPU quota is set,
    when the quota is disabled, or when running on a non-Linux platform.

    Mirrors joblib/loky's ``_cpu_count_cgroup``; see
    https://github.com/joblib/loky/blob/master/loky/backend/context.py
    """
    quota_us = period_us = None

    cpu_max = '/sys/fs/cgroup/cpu.max'
    if os.path.exists(cpu_max):
        try:
            with open(cpu_max) as fh:
                parts = fh.read().strip().split()
            if len(parts) == 2:
                quota_us, period_us = parts
        except OSError:
            pass

    if quota_us is None:
        v1_quota = '/sys/fs/cgroup/cpu/cpu.cfs_quota_us'
        v1_period = '/sys/fs/cgroup/cpu/cpu.cfs_period_us'
        if os.path.exists(v1_quota) and os.path.exists(v1_period):
            try:
                with open(v1_quota) as fh:
                    quota_us = fh.read().strip()
                with open(v1_period) as fh:
                    period_us = fh.read().strip()
            except OSError:
                return fallback
        else:
            return fallback

    # cgroup v2 reports "max" when no quota is set.
    if quota_us == 'max':
        return fallback

    try:
        quota = int(quota_us)
        period = int(period_us)
    except (TypeError, ValueError):
        return fallback

    # cgroup v1 reports -1 to disable the quota.
    if quota <= 0 or period <= 0:
        return fallback

    return min(fallback, max(1, ceil(quota / period)))


def effective_cpu_count(use_cgroup_quota: bool = True) -> int:
    """Return effective CPU count for sizing a worker pool.

    When ``use_cgroup_quota`` is True (the default, intended for CPU-bound
    pools such as prefork), the returned value honors any cgroup CFS
    bandwidth quota on Linux so workers do not oversubscribe and incur
    CFS throttling inside container CPU limits.

    When ``use_cgroup_quota`` is False (intended for cooperative or thread
    pools, where concurrency is bound by memory / file descriptors /
    downstream pool sizes rather than CPU), returns ``os.cpu_count()``
    unchanged.

    Falls back to ``os.cpu_count()`` (or 2 if undetermined) on non-Linux
    platforms, when no cgroup CPU controller is mounted, or when the
    cgroup files are unreadable.
    """
    fallback = os.cpu_count() or 2
    if not use_cgroup_quota:
        return fallback
    return _detect_cgroup_cpu_count(fallback)


if hasattr(os, 'getloadavg'):

    def _load_average() -> tuple[float, ...]:
        return tuple(ceil(l * 1e2) / 1e2 for l in os.getloadavg())

else:  # pragma: no cover
    # Windows doesn't have getloadavg
    def _load_average() -> tuple[float, ...]:
        return 0.0, 0.0, 0.0,


def load_average() -> tuple[float, ...]:
    """Return system load average as a triple."""
    return _load_average()


class df:
    """Disk information."""

    def __init__(self, path: str | bytes | os.PathLike) -> None:
        self.path = path

    @property
    def total_blocks(self) -> float:
        return self.stat.f_blocks * self.stat.f_frsize / 1024

    @property
    def available(self) -> float:
        return self.stat.f_bavail * self.stat.f_frsize / 1024

    @property
    def capacity(self) -> int:
        avail = self.stat.f_bavail
        used = self.stat.f_blocks - self.stat.f_bfree
        return int(ceil(used * 100.0 / (used + avail) + 0.5))

    @cached_property
    def stat(self) -> os.statvfs_result:
        return os.statvfs(os.path.abspath(self.path))
