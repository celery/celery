import importlib
import os

import pytest

from celery.utils.sysinfo import df, load_average

try:
    posix = importlib.import_module('posix')
except Exception:
    posix = None


@pytest.mark.skipif(
    not hasattr(os, 'getloadavg'),
    reason='Function os.getloadavg is not defined'
)
def test_load_average(patching):
    getloadavg = patching('os.getloadavg')
    getloadavg.return_value = 0.54736328125, 0.6357421875, 0.69921875
    l = load_average()
    assert l
    assert l == (0.55, 0.64, 0.7)


@pytest.mark.skipif(
    not hasattr(posix, 'statvfs_result'),
    reason='Function posix.statvfs_result is not defined'
)
def test_df():
    x = df('/')
    assert x.total_blocks
    assert x.available
    assert x.capacity
    assert x.stat
