import importlib
import os
from unittest.mock import patch

import pytest

from celery.utils.sysinfo import (AUTO_CONCURRENCY, PROC_SELF_CGROUP, CpuBudget, _ancestors, available_cpu_count,
                                  cgroup_cpu_quota, cpu_budget, df, is_auto_concurrency, load_average)

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


def _fake_fs(files):
    """Patch ``_read_text`` to serve ``files`` ({path: content})."""
    return patch(
        'celery.utils.sysinfo._read_text',
        side_effect=lambda path: files.get(path),
    )


def _available(n):
    return patch('celery.utils.sysinfo.available_cpu_count', return_value=n)


V2_MAX = '/sys/fs/cgroup/cpu.max'
V1_QUOTA = '/sys/fs/cgroup/cpu/cpu.cfs_quota_us'
V1_PERIOD = '/sys/fs/cgroup/cpu/cpu.cfs_period_us'
PROC_V2 = '0::/kubepods/burstable/pod1/ctr1\n'
PROC_V1 = (
    '12:cpu,cpuacct:/docker/abc\n'
    '11:memory:/docker/abc\n'
    '1:name=systemd:/docker/abc\n'
)


class test_available_cpu_count:

    def test_prefers_process_cpu_count(self):
        with patch('celery.utils.sysinfo.os') as os_mock:
            os_mock.process_cpu_count.return_value = 3
            os_mock.sched_getaffinity.return_value = {0, 1}
            os_mock.cpu_count.return_value = 8
            assert available_cpu_count() == 3
            os_mock.sched_getaffinity.assert_not_called()

    def test_process_cpu_count_none_uses_cpu_count(self):
        with patch('celery.utils.sysinfo.os', spec=['process_cpu_count', 'cpu_count']) as os_mock:
            os_mock.process_cpu_count.return_value = None
            os_mock.cpu_count.return_value = 8
            assert available_cpu_count() == 8

    def test_falls_back_to_sched_getaffinity(self):
        with patch('celery.utils.sysinfo.os', spec=['sched_getaffinity', 'cpu_count']) as os_mock:
            os_mock.sched_getaffinity.return_value = {0, 1}
            os_mock.cpu_count.return_value = 8
            assert available_cpu_count() == 2

    def test_affinity_oserror_falls_back_to_cpu_count(self):
        with patch('celery.utils.sysinfo.os', spec=['sched_getaffinity', 'cpu_count']) as os_mock:
            os_mock.sched_getaffinity.side_effect = OSError('nope')
            os_mock.cpu_count.return_value = 8
            assert available_cpu_count() == 8

    def test_no_affinity_api_uses_cpu_count(self):
        with patch('celery.utils.sysinfo.os', spec=['cpu_count']) as os_mock:
            os_mock.cpu_count.return_value = 8
            assert available_cpu_count() == 8

    def test_cpu_count_none_uses_default_two(self):
        with patch('celery.utils.sysinfo.os', spec=['cpu_count']) as os_mock:
            os_mock.cpu_count.return_value = None
            assert available_cpu_count() == 2


@pytest.mark.parametrize('path, expected', [
    ('/', ['/']),
    ('/a/b', ['/a/b', '/a', '/']),
    ('a/b', ['/a/b', '/a', '/']),
    # v1 seen from inside a private cgroup namespace.
    ('/../..', ['/']),
])
def test_ancestors(path, expected):
    assert _ancestors(path) == expected


class test_cgroup_cpu_quota:

    def test_no_files_returns_none(self):
        with _fake_fs({}):
            assert cgroup_cpu_quota() is None

    def test_v2_root_when_proc_unavailable(self):
        # No /proc/self/cgroup: only the mount root is inspected, which is
        # the process's own cgroup inside a private cgroup namespace.
        with _fake_fs({V2_MAX: '200000 100000\n'}):
            assert cgroup_cpu_quota() == 2.0

    def test_v2_max_returns_none(self):
        with _fake_fs({V2_MAX: 'max 100000\n'}):
            assert cgroup_cpu_quota() is None

    def test_v2_own_cgroup_from_proc(self):
        files = {
            PROC_SELF_CGROUP: PROC_V2,
            '/sys/fs/cgroup/kubepods/burstable/pod1/ctr1/cpu.max': '150000 100000\n',
        }
        with _fake_fs(files):
            assert cgroup_cpu_quota() == 1.5

    def test_v2_ancestor_limit_is_honored(self):
        # cgroupns=host: the leaf has no quota, the pod-level ancestor does.
        files = {
            PROC_SELF_CGROUP: PROC_V2,
            '/sys/fs/cgroup/kubepods/burstable/pod1/ctr1/cpu.max': 'max 100000\n',
            '/sys/fs/cgroup/kubepods/burstable/pod1/cpu.max': '300000 100000\n',
        }
        with _fake_fs(files):
            assert cgroup_cpu_quota() == 3.0

    def test_v2_minimum_along_chain_wins(self):
        files = {
            PROC_SELF_CGROUP: PROC_V2,
            '/sys/fs/cgroup/kubepods/burstable/pod1/ctr1/cpu.max': '400000 100000\n',
            '/sys/fs/cgroup/kubepods/burstable/pod1/cpu.max': '200000 100000\n',
            '/sys/fs/cgroup/kubepods/cpu.max': '800000 100000\n',
        }
        with _fake_fs(files):
            assert cgroup_cpu_quota() == 2.0

    def test_v2_root_cgroup_line(self):
        # Private cgroup namespace: /proc/self/cgroup reports "0::/".
        files = {
            PROC_SELF_CGROUP: '0::/\n',
            V2_MAX: '200000 100000\n',
        }
        with _fake_fs(files):
            assert cgroup_cpu_quota() == 2.0

    def test_v1_quota_from_proc(self):
        files = {
            PROC_SELF_CGROUP: PROC_V1,
            '/sys/fs/cgroup/cpu/docker/abc/cpu.cfs_quota_us': '200000\n',
            '/sys/fs/cgroup/cpu/docker/abc/cpu.cfs_period_us': '100000\n',
        }
        with _fake_fs(files):
            assert cgroup_cpu_quota() == 2.0

    def test_v1_root_when_proc_unavailable(self):
        with _fake_fs({V1_QUOTA: '200000\n', V1_PERIOD: '100000\n'}):
            assert cgroup_cpu_quota() == 2.0

    def test_v1_disabled_quota_returns_none(self):
        # cgroup v1 uses -1 to mean "no quota".
        with _fake_fs({V1_QUOTA: '-1\n', V1_PERIOD: '100000\n'}):
            assert cgroup_cpu_quota() is None

    def test_v1_ancestor_limit_is_honored(self):
        files = {
            PROC_SELF_CGROUP: PROC_V1,
            '/sys/fs/cgroup/cpu/docker/abc/cpu.cfs_quota_us': '-1\n',
            '/sys/fs/cgroup/cpu/docker/abc/cpu.cfs_period_us': '100000\n',
            '/sys/fs/cgroup/cpu/docker/cpu.cfs_quota_us': '50000\n',
            '/sys/fs/cgroup/cpu/docker/cpu.cfs_period_us': '100000\n',
        }
        with _fake_fs(files):
            assert cgroup_cpu_quota() == 0.5

    def test_malformed_v2_falls_through_to_v1(self):
        # A hybrid host can expose both hierarchies. Garbage in cpu.max must
        # not mask a valid v1 quota.
        files = {
            V2_MAX: 'not a number\n',
            V1_QUOTA: '200000\n',
            V1_PERIOD: '100000\n',
        }
        with _fake_fs(files):
            assert cgroup_cpu_quota() == 2.0

    def test_malformed_v2_only_returns_none(self):
        with _fake_fs({V2_MAX: 'not a number\n'}):
            assert cgroup_cpu_quota() is None

    def test_malformed_v1_returns_none(self):
        with _fake_fs({V1_QUOTA: 'x\n', V1_PERIOD: '100000\n'}):
            assert cgroup_cpu_quota() is None

    def test_unreadable_files_return_none(self):
        # _read_text swallows OSError (permission denied etc.) as None.
        with patch('celery.utils.sysinfo.open', side_effect=PermissionError('denied'), create=True):
            assert cgroup_cpu_quota() is None

    def test_proc_line_without_cpu_controller_is_ignored(self):
        files = {
            PROC_SELF_CGROUP: '11:memory:/docker/abc\n',
            V1_QUOTA: '200000\n',
            V1_PERIOD: '100000\n',
        }
        with _fake_fs(files):
            # No cpu controller path and no v2 path: falls back to root.
            assert cgroup_cpu_quota() == 2.0


class test_cpu_budget:

    def test_use_cgroup_quota_false_skips_quota(self):
        with _available(8), patch('celery.utils.sysinfo.cgroup_cpu_quota') as q:
            assert cpu_budget(use_cgroup_quota=False) == CpuBudget(8, 8, None)
            q.assert_not_called()

    def test_no_quota_returns_available(self):
        with _available(8), patch('celery.utils.sysinfo.cgroup_cpu_quota', return_value=None):
            assert cpu_budget() == CpuBudget(8, 8, None)

    def test_quota_returns_ceil(self):
        # 1.5 CPU quota -> ceil == 2.
        with _available(8), patch('celery.utils.sysinfo.cgroup_cpu_quota', return_value=1.5):
            assert cpu_budget() == CpuBudget(2, 8, 1.5)

    def test_quota_clamped_to_available(self):
        # A quota larger than the affinity set must not over-report.
        with _available(4), patch('celery.utils.sysinfo.cgroup_cpu_quota', return_value=80.0):
            assert cpu_budget() == CpuBudget(4, 4, 80.0)

    def test_sub_one_quota_rounds_up_to_one(self):
        # ceil(0.3) == 1, never 0.
        with _available(8), patch('celery.utils.sysinfo.cgroup_cpu_quota', return_value=0.3):
            assert cpu_budget() == CpuBudget(1, 8, 0.3)
