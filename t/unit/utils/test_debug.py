from __future__ import absolute_import, unicode_literals

import pytest
from case import Mock

from celery.utils import debug


def test_on_blocking(patching):
    getframeinfo = patching('inspect.getframeinfo')
    frame = Mock(name='frame')
    with pytest.raises(RuntimeError):
        debug._on_blocking(1, frame)
        getframeinfo.assert_called_with(frame)


def test_blockdetection(patching):
    signals = patching('celery.utils.debug.signals')
    with debug.blockdetection(10):
        signals.arm_alarm.assert_called_with(10)
        signals.__setitem__.assert_called_with('ALRM', debug._on_blocking)
    signals.__setitem__.assert_called_with('ALRM', signals['ALRM'])
    signals.reset_alarm.assert_called_with()


def test_sample_mem(patching):
    mem_rss = patching('celery.utils.debug.mem_rss')
    prev, debug._mem_sample = debug._mem_sample, []
    try:
        debug.sample_mem()
        assert debug._mem_sample[0] is mem_rss()
    finally:
        debug._mem_sample = prev


def test_sample():
    x = list(range(100))
    assert list(debug.sample(x, 10)) == [
        0, 10, 20, 30, 40, 50, 60, 70, 80, 90,
    ]
    x = list(range(91))
    assert list(debug.sample(x, 10)) == [
        0, 9, 18, 27, 36, 45, 54, 63, 72, 81,
    ]


@pytest.mark.parametrize('f,precision,expected', [
    (10, 5, '10'),
    (10.45645234234, 5, '10.456'),
])
def test_hfloat(f, precision, expected):
    assert str(debug.hfloat(f, precision)) == expected


@pytest.mark.parametrize('byt,expected', [
    (2 ** 20, '1MB'),
    (4 * 2 ** 20, '4MB'),
    (2 ** 16, '64KB'),
    (2 ** 16, '64KB'),
    (2 ** 8, '256b'),
])
def test_humanbytes(byt, expected):
    assert debug.humanbytes(byt) == expected


def test_mem_rss(patching):
    humanbytes = patching('celery.utils.debug.humanbytes')
    ps = patching('celery.utils.debug.ps')
    ret = debug.mem_rss()
    ps.assert_called_with()
    ps().memory_info.assert_called_with()
    humanbytes.assert_called_with(ps().memory_info().rss)
    assert ret is humanbytes()
    ps.return_value = None
    assert debug.mem_rss() is None


def test_ps(patching):
    Process = patching('celery.utils.debug.Process')
    getpid = patching('os.getpid')
    prev, debug._process = debug._process, None
    try:
        debug.ps()
        Process.assert_called_with(getpid())
        assert debug._process is Process()
    finally:
        debug._process = prev
