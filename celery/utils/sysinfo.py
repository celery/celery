"""System information utilities."""
from __future__ import annotations

import os
from math import ceil

from kombu.utils.objects import cached_property

__all__ = ('load_average', 'df')


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
