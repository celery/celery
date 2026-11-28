import sys
from unittest.mock import Mock, patch

import pytest
from billiard.einfo import ExceptionWithTraceback

from celery.exceptions import Terminated

try:
    import eventlet
    HAS_EVENTLET = True
except ImportError:
    HAS_EVENTLET = False

# CI (Python 3.11+) does not install eventlet
# (requirements/extras/eventlet.txt gates it to Python < 3.10),
# so tests that do not need eventlet must not be skipped with the
# eventlet-dependent ones, or they never run in CI at all.
requires_eventlet = pytest.mark.skipif(
    not HAS_EVENTLET, reason='eventlet is not installed')

from greenlet import GreenletExit  # noqa

import t.skip  # noqa
from celery.concurrency.eventlet import TaskPool, Timer, apply_target  # noqa

eventlet_modules = (
    'eventlet',
    'eventlet.debug',
    'eventlet.greenthread',
    'eventlet.greenpool',
    'greenlet',
)


@t.skip.if_pypy
class EventletCase:

    def setup_method(self):
        self.patching.modules(*eventlet_modules)

    def teardown_method(self):
        for mod in [mod for mod in sys.modules
                    if mod.startswith('eventlet')]:
            try:
                del (sys.modules[mod])
            except KeyError:
                pass


@requires_eventlet
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


@requires_eventlet
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


@requires_eventlet
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
        assert len(x._pool_map.keys()) == 1
        assert x.getpid()

    @patch('celery.concurrency.eventlet.base')
    def test_apply_target(self, base):
        apply_target(Mock(), getpid=Mock())
        base.apply_target.assert_called()

    def test_grow(self):
        x = TaskPool(10)
        x._pool = Mock(name='_pool')
        x._pool.sem.release = Mock()
        x.grow(2)
        assert x.limit == 12
        assert x._pool.size == 12
        assert x._pool.sem.release.call_count == 2

    def test_shrink(self):
        x = TaskPool(10)
        x._pool = Mock(name='_pool')
        x._pool.sem.counter = 8
        x._pool.size = 10
        x.shrink(2)
        assert x.limit == 8
        assert x._pool.size == 8
        assert x._pool.sem.counter == 6

    def test_num_processes_reports_capacity_for_autoscale(self):
        x = TaskPool(5)
        x._pool = Mock(name='_pool')
        x._pool.running.return_value = 1

        assert x.num_processes == 5

    def test_autoscaler_scales_from_capacity_not_running_greenlets(self):
        from celery.worker import autoscale

        x = TaskPool(3)
        x._pool = Mock(name='_pool')
        x.grow = Mock()
        scaler = autoscale.Autoscaler(x, 10, 3, worker=Mock())
        reserved = [Mock() for _ in range(5)]

        with patch('celery.worker.autoscale.state.reserved_requests', reserved):
            assert scaler._maybe_scale()

        x.grow.assert_called_once_with(2)

    def test_get_info(self):
        x = TaskPool(10)
        x._pool = Mock(name='_pool')
        assert x._get_info() == {
            'implementation': 'celery.concurrency.eventlet:TaskPool',
            'max-concurrency': 10,
            'free-threads': x._pool.free(),
            'running-threads': x._pool.running(),
        }

    def test_terminate_job(self):
        func = Mock()
        pool = TaskPool(10)
        pool.on_start()
        pool.on_apply(func)

        assert len(pool._pool_map.keys()) == 1
        pid = list(pool._pool_map.keys())[0]
        greenlet = pool._pool_map[pid]

        pool.terminate_job(pid)
        greenlet.link.assert_called_once()
        greenlet.kill.assert_called_once()

    def test_terminate_job_unknown_pid(self):
        pool = TaskPool(10)
        pool.on_start()

        # Unknown pid: terminate_job() is a no-op, not an error.
        pool.terminate_job(12345)
        assert len(pool._pool_map) == 0

    def test_cleanup_after_job_finish(self):
        testMap = {'1': None}
        TaskPool._cleanup_after_job_finish(None, testMap, '1')
        assert len(testMap) == 0


class test_TaskPoolKillableTarget:
    """Tests for TaskPool helpers that do not need eventlet installed.

    celery/concurrency/eventlet.py does not import eventlet at module
    level, so _make_killable_target() can be exercised without it.
    These tests must keep running when eventlet is not installed: CI
    (Python 3.11+) does not install eventlet, so a test skipped along
    with the eventlet-dependent ones never runs in CI and the #11086
    fix in this module gets no patch coverage (codecov/patch fails).
    """

    def test_make_killable_target(self):
        def valid_target():
            return "some result..."

        def terminating_target():
            raise GreenletExit()

        assert TaskPool._make_killable_target(valid_target)() == "some result..."
        # A killed greenlet must be reported as a terminated job, not as
        # a success: (False, None, None) made Request.on_success() ack a
        # late-ack task that never finished (Issue #11086).
        failed, retval, runtime = TaskPool._make_killable_target(terminating_target)()
        assert failed
        assert runtime is None
        exc = retval.exception
        if isinstance(exc, ExceptionWithTraceback):
            exc = exc.exc
        assert isinstance(exc, Terminated)


@requires_eventlet
class test_TaskPool_eventlet:

    def test_grow_wakes_spawn_waiter(self):
        pool = TaskPool(1)
        pool.on_start()
        first_started = eventlet.Event()
        release_first = eventlet.Event()
        second_started = eventlet.Event()
        waiter = None

        def first():
            first_started.send(True)
            release_first.wait()

        def second():
            second_started.send(True)

        try:
            pool._pool.spawn(first)
            with eventlet.Timeout(1):
                first_started.wait()

            waiter = eventlet.spawn(pool._pool.spawn, second)
            eventlet.sleep(0)
            assert not second_started.ready()

            pool.grow()

            with eventlet.Timeout(1):
                second_started.wait()
        finally:
            release_first.send(True)
            pool._pool.waitall()
            if waiter is not None:
                waiter.wait()
