from unittest.mock import Mock, patch

import pytest

from celery.concurrency.gevent import TaskPool, Timer, apply_timeout

gevent_modules = (
    'gevent',
    'gevent.greenlet',
    'gevent.monkey',
    'gevent.pool',
    'gevent.signal',
)


class test_gevent_patch:

    def test_is_patched(self):
        self.patching.modules(*gevent_modules)
        patch_all = self.patching('gevent.monkey.patch_all')
        import gevent
        gevent.version_info = (1, 0, 0)
        from celery import maybe_patch_concurrency
        maybe_patch_concurrency(['x', '-P', 'gevent'])
        patch_all.assert_called()


class test_Timer:

    def setup_method(self):
        self.patching.modules(*gevent_modules)
        self.greenlet = self.patching('gevent.greenlet')
        self.GreenletExit = self.patching('gevent.greenlet.GreenletExit')

    def test_sched(self):
        self.greenlet.Greenlet = object
        x = Timer()
        self.greenlet.Greenlet = Mock()
        x._Greenlet.spawn_later = Mock()
        x._GreenletExit = KeyError
        entry = Mock()
        g = x._enter(1, 0, entry)
        assert x.queue
        x._Greenlet.spawn_later.assert_called_once_with(0, x._apply_entry, entry)

        x._entry_exit(g)
        g.kill.assert_called_with()
        assert not x._queue

        x._queue.add(g)
        x.clear()
        g.entry.cancel.assert_called_once_with()
        x._queue.add(g)
        g.kill.side_effect = KeyError()
        x.clear()

        g = x._Greenlet()
        g.cancel()

    def test_cancel_and_apply_entry(self):
        import gevent
        gevent.Greenlet = type('Greenlet', (), {'kill': Mock()})
        x = Timer()
        entry = Mock(canceled=False)

        x._apply_entry(entry)
        entry.assert_called_once_with()

        g = x._Greenlet()
        g.entry = entry
        g.kill = Mock()
        g.cancel(block=False)

        entry.cancel.assert_called_once_with()
        g.kill.assert_called_once_with(block=False)

        entry.reset_mock()
        entry.canceled = True
        x._apply_entry(entry)
        entry.assert_not_called()


class test_Timer_real:
    @pytest.fixture
    def gevent(self):
        return pytest.importorskip('gevent')

    @pytest.fixture
    def timer(self, gevent):
        timer = Timer()
        yield timer
        for g in list(timer.queue):
            g.entry.cancel()
            g.kill(block=False)
        gevent.sleep(0)

    def assert_quiet(self, timer, gevent, calls, expected):
        from gevent.event import Event

        barrier = Event()
        observer = timer.call_after(0.01, barrier.set)
        assert barrier.wait(timeout=2)
        observer.join(timeout=2)
        gevent.sleep(0)
        assert calls == expected

    @pytest.mark.parametrize('cancel_via_timer', [False, True])
    def test_cancel_after_multiple_iterations(self, timer, gevent, cancel_via_timer):
        from gevent.queue import Queue

        ticks = Queue()
        calls = []

        def tick():
            calls.append(len(calls))
            ticks.put(len(calls))

        first = timer.call_repeatedly(0.001, tick)
        for expected in range(1, 4):
            assert ticks.get(timeout=2) == expected
        assert first.dead
        assert any(g.entry is first.entry and g is not first for g in timer.queue)
        if cancel_via_timer:
            timer.cancel(first)
        else:
            first.cancel()
        assert first.entry.canceled
        self.assert_quiet(timer, gevent, calls, [0, 1, 2])
        assert not timer.queue

    def test_cancel_leaves_other_timer_running(self, timer, gevent):
        from gevent.queue import Queue

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
        self.assert_quiet(timer, gevent, canceled, expected)

    def test_clear_stops_repeated_scheduling(self, timer, gevent):
        from gevent.event import Event

        entered = Event()
        calls = []

        def tick():
            calls.append('tick')
            entered.set()
            gevent.sleep(0.05)

        first = timer.call_repeatedly(0.001, tick)
        assert entered.wait(timeout=2)
        timer.clear()
        assert first.entry.canceled
        assert not timer.queue
        self.assert_quiet(timer, gevent, calls, ['tick'])

    def test_cancel_from_own_callback(self, timer, gevent):
        from gevent.queue import Queue

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
        self.assert_quiet(timer, gevent, calls, [0, 1])
        assert not timer.queue

    @pytest.mark.parametrize('method', ['call_after', 'call_at'])
    def test_single_shot_and_repeated_cancel(self, timer, gevent, method):
        from time import monotonic

        from gevent.event import Event

        called = Event()
        when = 0.001 if method == 'call_after' else monotonic() + 0.001
        handle = getattr(timer, method)(when, called.set)
        assert isinstance(handle, gevent.Greenlet)
        assert called.wait(timeout=2)
        handle.join(timeout=2)
        handle.cancel()
        timer.cancel(handle)
        handle.cancel(block=False)
        assert handle.dead
        assert handle.entry.canceled

    @pytest.mark.parametrize('cancel_via_timer', [False, True])
    def test_cancel_single_shot_before_start(self, timer, gevent, cancel_via_timer):
        called = []
        handle = timer.call_after(60, called.append, ('unexpected',))
        if cancel_via_timer:
            timer.cancel(handle)
        else:
            handle.cancel()
        assert handle.dead
        assert handle.entry.canceled
        self.assert_quiet(timer, gevent, called, [])
        assert not timer.queue


class test_TaskPool:

    def setup_method(self):
        self.patching.modules(*gevent_modules)
        self.spawn_raw = self.patching('gevent.spawn_raw')
        self.Pool = self.patching('gevent.pool.Pool')

    def test_pool(self):
        x = TaskPool(1)
        x.on_start()
        x.on_stop()
        x.on_apply(Mock())
        x._pool = None
        x.on_stop()

        x._pool = Mock()
        semaphore = x._pool._semaphore
        semaphore.counter = 1

        def release():
            semaphore.counter += 1

        semaphore.release = Mock(side_effect=release)
        x._pool.size = 1
        x.grow()
        assert x._pool.size == 2
        assert x.limit == 2
        semaphore.release.assert_called_once_with()
        x.shrink()
        assert x._pool.size == 1
        assert semaphore.counter == 1
        assert x.limit == 1

        x.limit = 3
        assert x.num_processes == 3

    def test_grow_updates_capacity_and_notifies_waiters(self):
        x = TaskPool(1)
        x._pool = Mock()
        x._pool._semaphore.release = Mock()
        x._pool.size = 1

        x.grow(4)

        assert x.limit == 5
        assert x._pool.size == 5
        assert x._pool._semaphore.release.call_count == 4

    def test_shrink_updates_capacity(self):
        x = TaskPool(5)
        x._pool = Mock()
        x._pool._semaphore.counter = 4
        x._pool.size = 5

        x.shrink(3)

        assert x.limit == 2
        assert x._pool.size == 2
        assert x._pool._semaphore.counter == 1

    def test_num_processes_reports_capacity_for_autoscale(self):
        x = TaskPool(5)
        x._pool = [object()]

        assert len(x._pool) == 1
        assert x.num_processes == 5

    def test_autoscaler_scales_from_capacity_not_running_greenlets(self):
        from celery.worker import autoscale

        x = TaskPool(3)
        x._pool = [object()]
        x.grow = Mock()
        scaler = autoscale.Autoscaler(x, 10, 3, worker=Mock())
        reserved = [Mock() for _ in range(5)]

        with patch('celery.worker.autoscale.state.reserved_requests', reserved):
            assert scaler._maybe_scale()

        x.grow.assert_called_once_with(2)

    def test_terminate_job(self):
        func = Mock()
        pool = TaskPool(10)
        pool.on_start()
        pool.on_apply(func)

        assert len(pool._pool_map.keys()) == 1
        pid = list(pool._pool_map.keys())[0]
        greenlet = pool._pool_map[pid]
        greenlet.link.assert_called_once()

        pool.terminate_job(pid)
        import gevent

        gevent.kill.assert_called_once()

    def test_make_killable_target(self):
        def valid_target():
            return "some result..."

        def terminating_target():
            from greenlet import GreenletExit
            raise GreenletExit

        assert TaskPool._make_killable_target(valid_target)() == "some result..."
        assert TaskPool._make_killable_target(terminating_target)() == (False, None, None)

    def test_cleanup_after_job_finish(self):
        testMap = {'1': None}
        TaskPool._cleanup_after_job_finish(None, testMap, '1')
        assert len(testMap) == 0


class test_apply_timeout:

    def test_apply_timeout(self):
        self.patching.modules(*gevent_modules)

        class Timeout(Exception):
            value = None

            def __init__(self, value):
                self.__class__.value = value

            def __enter__(self):
                return self

            def __exit__(self, *exc_info):
                pass
        timeout_callback = Mock(name='timeout_callback')
        apply_target = Mock(name='apply_target')
        getpid = Mock(name='getpid')
        apply_timeout(
            Mock(), timeout=10, callback=Mock(name='callback'),
            timeout_callback=timeout_callback, getpid=getpid,
            apply_target=apply_target, Timeout=Timeout,
        )
        assert Timeout.value == 10
        apply_target.assert_called()

        apply_target.side_effect = Timeout(10)
        apply_timeout(
            Mock(), timeout=10, callback=Mock(),
            timeout_callback=timeout_callback, getpid=getpid,
            apply_target=apply_target, Timeout=Timeout,
        )
        timeout_callback.assert_called_with(False, 10)
