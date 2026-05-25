import importlib
import os
from unittest.mock import mock_open, patch

import pytest

from celery.utils.sysinfo import (
    AUTO_CONCURRENCY,
    df,
    effective_cpu_count,
    is_auto_concurrency,
    load_average,
)

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


class test_is_auto_concurrency:

    @pytest.mark.parametrize('value', [
        'auto', 'AUTO', 'Auto', ' auto ', '\tauto\n',
    ])
    def test_recognized(self, value):
        assert is_auto_concurrency(value) is True

    @pytest.mark.parametrize('value', [
        None, '', '0', '4', 4, 'autopilot', 'auto4', object(),
    ])
    def test_not_recognized(self, value):
        assert is_auto_concurrency(value) is False

    def test_sentinel_value(self):
        # The exported constant matches what is_auto_concurrency accepts.
        assert is_auto_concurrency(AUTO_CONCURRENCY)


class test_effective_cpu_count:

    def test_use_cgroup_quota_false_returns_cpu_count(self):
        with patch('celery.utils.sysinfo.os.cpu_count', return_value=8):
            assert effective_cpu_count(use_cgroup_quota=False) == 8

    def test_cgroup_v2_max_returns_fallback(self):
        # cgroup v2 reports "max" when no quota is set.
        with patch('celery.utils.sysinfo.os.cpu_count', return_value=8), \
                patch(
                    'celery.utils.sysinfo.os.path.exists',
                    side_effect=lambda p: p == '/sys/fs/cgroup/cpu.max',
                ), \
                patch(
                    'celery.utils.sysinfo.open',
                    mock_open(read_data='max 100000'),
                    create=True,
                ):
            assert effective_cpu_count(use_cgroup_quota=True) == 8

    def test_cgroup_v2_quota_returns_ceil(self):
        # 1.5 CPU quota → ceil(150000 / 100000) == 2.
        with patch('celery.utils.sysinfo.os.cpu_count', return_value=8), \
                patch(
                    'celery.utils.sysinfo.os.path.exists',
                    side_effect=lambda p: p == '/sys/fs/cgroup/cpu.max',
                ), \
                patch(
                    'celery.utils.sysinfo.open',
                    mock_open(read_data='150000 100000'),
                    create=True,
                ):
            assert effective_cpu_count(use_cgroup_quota=True) == 2

    def test_cgroup_v2_quota_clamped_to_host(self):
        # A quota larger than host CPU count must not over-report.
        with patch('celery.utils.sysinfo.os.cpu_count', return_value=4), \
                patch(
                    'celery.utils.sysinfo.os.path.exists',
                    side_effect=lambda p: p == '/sys/fs/cgroup/cpu.max',
                ), \
                patch(
                    'celery.utils.sysinfo.open',
                    mock_open(read_data='8000000 100000'),
                    create=True,
                ):
            assert effective_cpu_count(use_cgroup_quota=True) == 4

    def test_cgroup_v2_sub_one_quota_floors_to_one(self):
        # ceil(0.3) == 1, never 0.
        with patch('celery.utils.sysinfo.os.cpu_count', return_value=8), \
                patch(
                    'celery.utils.sysinfo.os.path.exists',
                    side_effect=lambda p: p == '/sys/fs/cgroup/cpu.max',
                ), \
                patch(
                    'celery.utils.sysinfo.open',
                    mock_open(read_data='30000 100000'),
                    create=True,
                ):
            assert effective_cpu_count(use_cgroup_quota=True) == 1

    def test_cgroup_v1_quota(self):
        v1_quota = '/sys/fs/cgroup/cpu/cpu.cfs_quota_us'
        v1_period = '/sys/fs/cgroup/cpu/cpu.cfs_period_us'

        def fake_open(path, *args, **kwargs):
            if path == v1_quota:
                return mock_open(read_data='200000')()
            if path == v1_period:
                return mock_open(read_data='100000')()
            raise FileNotFoundError(path)

        with patch('celery.utils.sysinfo.os.cpu_count', return_value=8), \
                patch(
                    'celery.utils.sysinfo.os.path.exists',
                    side_effect=lambda p: p in (v1_quota, v1_period),
                ), \
                patch(
                    'celery.utils.sysinfo.open',
                    side_effect=fake_open,
                    create=True,
                ):
            assert effective_cpu_count(use_cgroup_quota=True) == 2

    def test_cgroup_v1_disabled_quota_returns_fallback(self):
        # cgroup v1 uses -1 to mean "no quota".
        v1_quota = '/sys/fs/cgroup/cpu/cpu.cfs_quota_us'
        v1_period = '/sys/fs/cgroup/cpu/cpu.cfs_period_us'

        def fake_open(path, *args, **kwargs):
            if path == v1_quota:
                return mock_open(read_data='-1')()
            if path == v1_period:
                return mock_open(read_data='100000')()
            raise FileNotFoundError(path)

        with patch('celery.utils.sysinfo.os.cpu_count', return_value=8), \
                patch(
                    'celery.utils.sysinfo.os.path.exists',
                    side_effect=lambda p: p in (v1_quota, v1_period),
                ), \
                patch(
                    'celery.utils.sysinfo.open',
                    side_effect=fake_open,
                    create=True,
                ):
            assert effective_cpu_count(use_cgroup_quota=True) == 8

    def test_no_cgroup_returns_fallback(self):
        # No cgroup files present (non-Linux or no CPU controller).
        with patch('celery.utils.sysinfo.os.cpu_count', return_value=8), \
                patch(
                    'celery.utils.sysinfo.os.path.exists',
                    return_value=False,
                ):
            assert effective_cpu_count(use_cgroup_quota=True) == 8

    def test_malformed_cgroup_v2_falls_back(self):
        # Garbage in the file should not crash the worker.
        with patch('celery.utils.sysinfo.os.cpu_count', return_value=8), \
                patch(
                    'celery.utils.sysinfo.os.path.exists',
                    side_effect=lambda p: p == '/sys/fs/cgroup/cpu.max',
                ), \
                patch(
                    'celery.utils.sysinfo.open',
                    mock_open(read_data='not a number'),
                    create=True,
                ):
            assert effective_cpu_count(use_cgroup_quota=True) == 8

    def test_unreadable_cgroup_v2_falls_back(self):
        # Permission denied on the cgroup file should not crash.
        with patch('celery.utils.sysinfo.os.cpu_count', return_value=8), \
                patch(
                    'celery.utils.sysinfo.os.path.exists',
                    side_effect=lambda p: p == '/sys/fs/cgroup/cpu.max',
                ), \
                patch(
                    'celery.utils.sysinfo.open',
                    side_effect=PermissionError('denied'),
                    create=True,
                ):
            assert effective_cpu_count(use_cgroup_quota=True) == 8

    def test_cpu_count_none_uses_default_two(self):
        # When os.cpu_count() returns None (rare), fall back to 2.
        with patch('celery.utils.sysinfo.os.cpu_count', return_value=None), \
                patch(
                    'celery.utils.sysinfo.os.path.exists',
                    return_value=False,
                ):
            assert effective_cpu_count(use_cgroup_quota=False) == 2
            assert effective_cpu_count(use_cgroup_quota=True) == 2
