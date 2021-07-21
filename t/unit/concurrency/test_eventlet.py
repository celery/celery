import sys
from unittest.mock import Mock, patch

import pytest

import t.skip
from celery.concurrency.eventlet import TaskPool, Timer, apply_target

eventlet_modules = (
    'eventlet',
    'eventlet.debug',
    'eventlet.greenthread',
    'eventlet.greenpool',
    'greenlet',
)

pytest.importorskip('eventlet')


@t.skip.if_pypy
class EventletCase:

    def setup(self):
        self.patching.modules(*eventlet_modules)

    def teardown(self):
        for mod in [mod for mod in sys.modules
                    if mod.startswith('eventlet')]:
            try:
                del(sys.modules[mod])
            except KeyError:
                pass


class test_aaa_eventlet_patch(EventletCase):

    def test_aaa_is_patched(self):
        with patch('eventlet.monkey_patch', create=True) as monkey_patch:
            from celery import maybe_patch_concurrency
            maybe_patch_concurrency(['x', '-P', 'eventlet'])
            monkey_patch.assert_called_with()

    @patch('eventlet.debug.hub_blocking_detection', create=True)
    @patch('eventlet.monkey_patch', create=True)
    def test_aaa_blockdetecet(
            self, monkey_patch, hub_blocking_detection, patching):
        patching.setenv('EVENTLET_NOBLOCK', '10.3')
        from celery import maybe_patch_concurrency
        maybe_patch_concurrency(['x', '-P', 'eventlet'])
        monkey_patch.assert_called_with()
        hub_blocking_detection.assert_called_with(10.3, 10.3)


class test_Timer(EventletCase):

    @pytest.fixture(autouse=True)
    def setup_patches(self, patching):
        self.spawn_after = patching('eventlet.greenthread.spawn_after')
        self.GreenletExit = patching('greenlet.GreenletExit')

    def test_sched(self):
        x = Timer()
        x.GreenletExit = KeyError
        entry = Mock()
        g = x._enter(1, 0, entry)
        assert x.queue

        x._entry_exit(g, entry)
        g.wait.side_effect = KeyError()
        x._entry_exit(g, entry)
        entry.cancel.assert_called_with()
        assert not x._queue

        x._queue.add(g)
        x.clear()
        x._queue.add(g)
        g.cancel.side_effect = KeyError()
        x.clear()

    def test_cancel(self):
        x = Timer()
        tref = Mock(name='tref')
        x.cancel(tref)
        tref.cancel.assert_called_with()
        x.GreenletExit = KeyError
        tref.cancel.side_effect = KeyError()
        x.cancel(tref)


class test_TaskPool(EventletCase):

    @pytest.fixture(autouse=True)
    def setup_patches(self, patching):
        self.GreenPool = patching('eventlet.greenpool.GreenPool')
        self.greenthread = patching('eventlet.greenthread')

    def test_pool(self):
        x = TaskPool()
        x.on_start()
        x.on_stop()
        x.on_apply(Mock())
        x._pool = None
        x.on_stop()
        assert x.getpid()

    @patch('celery.concurrency.eventlet.base')
    def test_apply_target(self, base):
        apply_target(Mock(), getpid=Mock())
        base.apply_target.assert_called()

    def test_grow(self):
        x = TaskPool(10)
        x._pool = Mock(name='_pool')
        x.grow(2)
        assert x.limit == 12
        x._pool.resize.assert_called_with(12)

    def test_shrink(self):
        x = TaskPool(10)
        x._pool = Mock(name='_pool')
        x.shrink(2)
        assert x.limit == 8
        x._pool.resize.assert_called_with(8)

    def test_get_info(self):
        x = TaskPool(10)
        x._pool = Mock(name='_pool')
        assert x._get_info() == {
            'max-concurrency': 10,
            'free-threads': x._pool.free(),
            'running-threads': x._pool.running(),
        }
