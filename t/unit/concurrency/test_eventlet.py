import sys
from unittest.mock import Mock, patch

import pytest

pytest.importorskip('eventlet')

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


class test_Timer_real:

    @pytest.fixture
    def eventlet(self):
        # EventletCase leaves stubbed modules behind, start from a clean import.
        for mod in [mod for mod in sys.modules if mod.startswith('eventlet')]:
            del sys.modules[mod]
        return pytest.importorskip('eventlet')

    @pytest.fixture
    def timer(self, eventlet):
        timer = Timer()
        yield timer
        for g in list(timer.queue):
            g.entry.cancel()
            g.kill()
        eventlet.sleep(0)

    def assert_quiet(self, timer, eventlet, calls, expected):
        from eventlet.event import Event

        barrier = Event()
        timer.call_after(0.01, barrier.send)
        with eventlet.Timeout(2):
            barrier.wait()
        eventlet.sleep(0)
        assert calls == expected

    @pytest.mark.parametrize('cancel_via_timer', [False, True])
    def test_cancel_after_multiple_iterations(
            self, timer, eventlet, cancel_via_timer):
        from eventlet.queue import Queue

        ticks = Queue()
        calls = []

        def tick():
            calls.append(len(calls))
            ticks.put(len(calls))

        first = timer.call_repeatedly(0.001, tick)
        for expected in range(1, 4):
            assert ticks.get(timeout=2) == expected
        assert first.dead
        assert any(
            g.entry is first.entry and g is not first for g in timer.queue
        )
        if cancel_via_timer:
            timer.cancel(first)
        else:
            first.cancel()
        assert first.entry.canceled
        self.assert_quiet(timer, eventlet, calls, [0, 1, 2])
        assert not timer.queue

    def test_cancel_leaves_other_timer_running(self, timer, eventlet):
        from eventlet.queue import Queue

        ticks = Queue()
        canceled = []
        first = timer.call_repeatedly(0.001, canceled.append, ('canceled',))
        timer.call_repeatedly(0.001, ticks.put, ('other',))
        assert ticks.get(timeout=2) == 'other'
        first.cancel()
        expected = list(canceled)
        while not ticks.empty():
            ticks.get_nowait()
        assert ticks.get(timeout=2) == 'other'
        self.assert_quiet(timer, eventlet, canceled, expected)

    def test_clear_stops_repeated_scheduling(self, timer, eventlet):
        from eventlet.event import Event

        entered = Event()
        calls = []

        def tick():
            calls.append('tick')
            entered.send()
            eventlet.sleep(0.05)

        first = timer.call_repeatedly(0.001, tick)
        with eventlet.Timeout(2):
            entered.wait()
        timer.clear()
        assert first.entry.canceled
        assert not timer.queue
        self.assert_quiet(timer, eventlet, calls, ['tick'])

    def test_cancel_from_own_callback(self, timer, eventlet):
        from eventlet.queue import Queue

        ticks = Queue()
        calls = []

        def tick():
            calls.append(len(calls))
            ticks.put(len(calls))
            if len(calls) == 2:
                handle.cancel()

        handle = timer.call_repeatedly(0.001, tick)
        for expected in (1, 2):
            assert ticks.get(timeout=2) == expected
        assert handle.entry.canceled
        self.assert_quiet(timer, eventlet, calls, [0, 1])
        assert not timer.queue

    @pytest.mark.parametrize('cancel_via_timer', [False, True])
    def test_cancel_single_shot_before_start(
            self, timer, eventlet, cancel_via_timer):
        called = []
        handle = timer.call_after(60, called.append, ('unexpected',))
        if cancel_via_timer:
            timer.cancel(handle)
        else:
            handle.cancel()
        eventlet.sleep(0)
        assert handle.dead
        assert handle.entry.canceled
        self.assert_quiet(timer, eventlet, called, [])
        assert not timer.queue

    def test_entry_canceled_directly_does_not_run(self, timer, eventlet):
        called = []
        handle = timer.call_after(0.001, called.append, ('unexpected',))
        handle.entry.cancel()
        self.assert_quiet(timer, eventlet, called, [])


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

    def test_make_killable_target(self):
        def valid_target():
            return "some result..."

        def terminating_target():
            raise GreenletExit()

        assert TaskPool._make_killable_target(valid_target)() == "some result..."
        assert TaskPool._make_killable_target(terminating_target)() == (False, None, None)

    def test_cleanup_after_job_finish(self):
        testMap = {'1': None}
        TaskPool._cleanup_after_job_finish(None, testMap, '1')
        assert len(testMap) == 0


class test_TaskPool_eventlet:

    def test_grow_wakes_spawn_waiter(self):
        import eventlet

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
