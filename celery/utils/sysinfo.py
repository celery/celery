"""System information utilities."""
from __future__ import annotations

import os
import posixpath
from math import ceil
from typing import NamedTuple

from kombu.utils.objects import cached_property

__all__ = (
    'load_average', 'df',
    'AUTO_CONCURRENCY', 'is_auto_concurrency', 'available_cpu_count',
    'cgroup_cpu_quota', 'CpuBudget', 'cpu_budget',
)


#: Sentinel string accepted by ``worker_concurrency`` / ``--concurrency``
#: to request cgroup-aware auto-sizing on Linux.
AUTO_CONCURRENCY = 'auto'

CGROUP_ROOT = '/sys/fs/cgroup'
PROC_SELF_CGROUP = '/proc/self/cgroup'


def is_auto_concurrency(value) -> bool:
    """Return True if ``value`` is the ``auto`` concurrency sentinel.

    Accepts case-insensitive ``"auto"`` with surrounding whitespace stripped.
    """
    return (
        isinstance(value, str)
        and value.strip().lower() == AUTO_CONCURRENCY
    )


def available_cpu_count() -> int:
    """Return the number of CPUs this process may run on.

    Honors the scheduler affinity mask (``taskset``, cpusets,
    ``docker run --cpuset-cpus``) via :func:`os.process_cpu_count` on
    Python 3.13+ or :func:`os.sched_getaffinity` on older Linux. Falls
    back to :func:`os.cpu_count` (or 2 if undetermined) where neither
    is available.
    """
    count = None
    if hasattr(os, 'process_cpu_count'):
        count = os.process_cpu_count()
    elif hasattr(os, 'sched_getaffinity'):
        try:
            count = len(os.sched_getaffinity(0))
        except OSError:
            pass
    return count or os.cpu_count() or 2


def _read_text(path: str) -> str | None:
    try:
        with open(path) as fh:
            return fh.read()
    except OSError:
        return None


def _parse_quota(quota_us: str, period_us: str) -> float | None:
    """Return ``quota / period`` in CPUs, or None when no quota applies.

    cgroup v2 reports ``max`` and cgroup v1 reports ``-1`` for an
    unlimited quota; both resolve to None.
    """
    if quota_us == 'max':
        return None
    try:
        quota = int(quota_us)
        period = int(period_us)
    except ValueError:
        return None
    if quota <= 0 or period <= 0:
        return None
    return quota / period


def _cgroup_paths() -> tuple[str | None, str | None]:
    """Return this process's (v2 path, v1 cpu-controller path).

    Parsed from ``/proc/self/cgroup``. Either entry is None when the
    corresponding hierarchy is absent. Paths are relative to the cgroup
    mount and always start with ``/``.
    """
    content = _read_text(PROC_SELF_CGROUP)
    if content is None:
        return None, None
    v2_path = v1_path = None
    for line in content.splitlines():
        parts = line.split(':', 2)
        if len(parts) != 3:
            continue
        hierarchy, controllers, path = parts
        if hierarchy == '0' and controllers == '':
            v2_path = path
        elif 'cpu' in controllers.split(','):
            v1_path = path
    return v2_path, v1_path


def _ancestors(path: str) -> list[str]:
    """Return ``path`` and every ancestor up to and including the root.

    ``'/a/b'`` -> ``['/a/b', '/a', '/']``. A path that does not start
    with ``/`` (a v1 hierarchy seen from inside a private cgroup
    namespace can yield ``/../..`` shapes) is normalised first.
    """
    norm = posixpath.normpath('/' + path.lstrip('/'))
    out = [norm]
    while norm != '/':
        norm = posixpath.dirname(norm)
        out.append(norm)
    return out


def _read_v2_quota(cgroup_dir: str) -> tuple[str, str] | None:
    content = _read_text(posixpath.join(cgroup_dir, 'cpu.max'))
    parts = content.split() if content else []
    return (parts[0], parts[1]) if len(parts) == 2 else None


def _read_v1_quota(cgroup_dir: str) -> tuple[str, str] | None:
    quota = _read_text(posixpath.join(cgroup_dir, 'cpu.cfs_quota_us'))
    period = _read_text(posixpath.join(cgroup_dir, 'cpu.cfs_period_us'))
    if quota is None or period is None:
        return None
    return quota.strip(), period.strip()


def _min_quota(base: str, cgroup_path: str, read) -> float | None:
    """Smallest quota found at ``cgroup_path`` and each of its ancestors.

    ``read(dir)`` returns ``(quota_us, period_us)`` strings or None.
    """
    quotas = []
    for rel in _ancestors(cgroup_path):
        raw = read(posixpath.join(base, rel.lstrip('/')))
        cpus = _parse_quota(*raw) if raw else None
        if cpus is not None:
            quotas.append(cpus)
    return min(quotas) if quotas else None


def cgroup_cpu_quota() -> float | None:
    """Return the effective CFS CPU quota (in CPUs) for this process.

    Resolves the process's own cgroup from ``/proc/self/cgroup``, then
    walks up to the hierarchy root and returns the **minimum** quota
    found along the way, so a limit set on a pod-level or systemd
    slice-level ancestor is honored. Tries cgroup v2 (``cpu.max``) first
    and cgroup v1 (``cpu.cfs_quota_us`` / ``cpu.cfs_period_us``) second.

    When ``/proc/self/cgroup`` is unavailable (older kernels, restricted
    ``/proc``), only the mount root is inspected, which is the process's
    own cgroup inside a private cgroup namespace.

    Returns None when no quota is set anywhere in the chain, when no
    cgroup CPU controller is mounted, or on non-Linux platforms.
    """
    v2_path, v1_path = _cgroup_paths()
    if v2_path is None and v1_path is None:
        v2_path = v1_path = '/'
    if v2_path is not None:
        cpus = _min_quota(CGROUP_ROOT, v2_path, _read_v2_quota)
        if cpus is not None:
            return cpus
    if v1_path is not None:
        cpus = _min_quota(posixpath.join(CGROUP_ROOT, 'cpu'), v1_path, _read_v1_quota)
        if cpus is not None:
            return cpus
    return None


class CpuBudget(NamedTuple):
    """Result of :func:`cpu_budget`."""

    #: Concurrency to use.
    count: int
    #: Affinity-aware CPU count, see :func:`available_cpu_count`.
    available: int
    #: cgroup CFS quota in CPUs, or None when no quota applies or the
    #: quota was not consulted.
    quota: float | None


def cpu_budget(use_cgroup_quota: bool = True) -> CpuBudget:
    """Return the CPU budget for sizing a worker pool.

    Starts from :func:`available_cpu_count`. When ``use_cgroup_quota`` is
    True (CPU-bound pools), caps it to ``ceil(quota)`` of any cgroup CFS
    quota, clamped to ``[1, available]``. ``ceil`` follows joblib/loky so a
    fractional quota is fully used; see :setting:`worker_concurrency`.

    ``quota`` is None when it was not consulted or no quota applies.
    """
    available = available_cpu_count()
    if not use_cgroup_quota:
        return CpuBudget(available, available, None)
    quota = cgroup_cpu_quota()
    if quota is None:
        return CpuBudget(available, available, None)
    return CpuBudget(min(available, max(1, ceil(quota))), available, quota)


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
