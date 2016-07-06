# -*- coding: utf-8 -*-
"""System information utilities."""
import os

from collections import namedtuple

from math import ceil
from typing import Tuple

from kombu.utils import cached_property

__all__ = ['load_average', 'load_average_t', 'df']

load_average_t = namedtuple('load_average_t', (
    'min_1', 'min_5', 'min_15',
))


def _avg(f: float) -> float:
    return ceil(f * 1e2) / 1e2


if hasattr(os, 'getloadavg'):

    def load_average() -> load_average_t:
        min_1, min_5, min_15 = os.getloadavg()
        return load_average_t(_avg(min_1), _avg(min_5), _avg(min_15))

else:  # pragma: no cover
    # Windows doesn't have getloadavg
    def load_average() -> load_average_t:  # noqa
        return load_average_t(0.0, 0.0, 0.0)


class df:

    def __init__(self, path: str) -> None:
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
