import sys
from time import monotonic

from case import Mock, mock, patch

from celery.concurrency.base import BasePool
from celery.utils.objects import Bunch
from celery.worker import autoscale, state


class MockPool(BasePool):

    shrink_raises_exception = False
    shrink_raises_ValueError = False

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._pool = Bunch(_processes=self.limit)

    def grow(self, n=1):
        self._pool._processes += n

    def shrink(self, n=1):
        if self.shrink_raises_exception:
            raise KeyError('foo')
        if self.shrink_raises_ValueError:
            raise ValueError('foo')
        self._pool._processes -= n

    @property
    def num_processes(self):
        return self._pool._processes


class test_WorkerComponent:

    def test_register_with_event_loop(self):
        parent = Mock(name='parent')
        parent.autoscale = True
        parent.consumer.on_task_message = set()
        w = autoscale.WorkerComponent(parent)
        assert parent.autoscaler is None
        assert w.enabled

        hub = Mock(name='hub')
        w.create(parent)
        w.register_with_event_loop(parent, hub)
        assert (parent.autoscaler.maybe_scale in
                parent.consumer.on_task_message)
        hub.call_repeatedly.assert_called_with(
            parent.autoscaler.keepalive, parent.autoscaler.maybe_scale,
        )

        parent.hub = hub
        hub.on_init = []
        w.instantiate = Mock()
        w.register_with_event_loop(parent, Mock(name='loop'))
        assert parent.consumer.on_task_message

    def test_info_without_event_loop(self):
        parent = Mock(name='parent')
        parent.autoscale = True
        parent.max_concurrency = '10'
        parent.min_concurrency = '2'
        parent.use_eventloop = False
        w = autoscale.WorkerComponent(parent)
        w.create(parent)
        info = w.info(parent)
        assert 'autoscaler' in info
        assert parent.autoscaler_cls().info.called


class test_Autoscaler:

    def setup(self):
        self.pool = MockPool(3)

    def test_stop(self):

        class Scaler(autoscale.Autoscaler):
            alive = True
            joined = False

            def is_alive(self):
                return self.alive

            def join(self, timeout=None):
                self.joined = True

        worker = Mock(name='worker')
        x = Scaler(self.pool, 10, 3, worker=worker)
        x._is_stopped.set()
        x.stop()
        assert x.joined
        x.joined = False
        x.alive = False
        x.stop()
        assert not x.joined

    @mock.sleepdeprived(module=autoscale)
    def test_body(self):
        worker = Mock(name='worker')
        x = autoscale.Autoscaler(self.pool, 10, 3, worker=worker)
        x.body()
        assert x.pool.num_processes == 3
        _keep = [Mock(name=f'req{i}') for i in range(20)]
        [state.task_reserved(m) for m in _keep]
        x.body()
        x.body()
        assert x.pool.num_processes == 10
        state.reserved_requests.clear()
        x.body()
        assert x.pool.num_processes == 10
        x._last_scale_up = monotonic() - 10000
        x.body()
        assert x.pool.num_processes == 3

    def test_run(self):

        class Scaler(autoscale.Autoscaler):
            scale_called = False

            def body(self):
                self.scale_called = True
                self._is_shutdown.set()

        worker = Mock(name='worker')
        x = Scaler(self.pool, 10, 3, worker=worker)
        x.run()
        assert x._is_shutdown.isSet()
        assert x._is_stopped.isSet()
        assert x.scale_called

    def test_shrink_raises_exception(self):
        worker = Mock(name='worker')
        x = autoscale.Autoscaler(self.pool, 10, 3, worker=worker)
        x.scale_up(3)
        x.pool.shrink_raises_exception = True
        x._shrink(1)

    @patch('celery.worker.autoscale.debug')
    def test_shrink_raises_ValueError(self, debug):
        worker = Mock(name='worker')
        x = autoscale.Autoscaler(self.pool, 10, 3, worker=worker)
        x.scale_up(3)
        x._last_scale_up = monotonic() - 10000
        x.pool.shrink_raises_ValueError = True
        x.scale_down(1)
        assert debug.call_count

    def test_update(self):
        worker = Mock(name='worker')
        x = autoscale.Autoscaler(self.pool, 10, 3, worker=worker)
        x.worker.consumer.prefetch_multiplier = 1
        x.keepalive = -1
        assert x.processes == 3
        x.scale_up(5)
        x.update(7, None)
        assert x.processes == 7
        assert x.max_concurrency == 7
        x.scale_down(4)
        x.update(None, 6)
        assert x.processes == 6
        assert x.min_concurrency == 6

        x.update(max=300, min=10)
        x.update(max=300, min=2)
        x.update(max=None, min=None)

    def test_prefetch_count_on_updates(self):
        worker = Mock(name='worker')
        x = autoscale.Autoscaler(self.pool, 10, 3, worker=worker)
        x.worker.consumer.prefetch_multiplier = 1
        x.update(5, None)
        worker.consumer._update_prefetch_count.assert_called_with(-5)
        x.update(15, 7)
        worker.consumer._update_prefetch_count.assert_called_with(10)

    def test_prefetch_count_on_updates_prefetch_multiplier_gt_one(self):
        worker = Mock(name='worker')
        x = autoscale.Autoscaler(self.pool, 10, 3, worker=worker)
        x.worker.consumer.prefetch_multiplier = 4
        x.update(5, None)
        worker.consumer._update_prefetch_count.assert_called_with(-5)
        x.update(15, 7)
        worker.consumer._update_prefetch_count.assert_called_with(10)

    def test_info(self):
        worker = Mock(name='worker')
        x = autoscale.Autoscaler(self.pool, 10, 3, worker=worker)
        info = x.info()
        assert info['max'] == 10
        assert info['min'] == 3
        assert info['current'] == 3

    @patch('os._exit')
    def test_thread_crash(self, _exit):

        class _Autoscaler(autoscale.Autoscaler):

            def body(self):
                self._is_shutdown.set()
                raise OSError('foo')
        worker = Mock(name='worker')
        x = _Autoscaler(self.pool, 10, 3, worker=worker)

        stderr = Mock()
        p, sys.stderr = sys.stderr, stderr
        try:
            x.run()
        finally:
            sys.stderr = p
        _exit.assert_called_with(1)
        stderr.write.assert_called()

    @mock.sleepdeprived(module=autoscale)
    def test_no_negative_scale(self):
        total_num_processes = []
        worker = Mock(name='worker')
        x = autoscale.Autoscaler(self.pool, 10, 3, worker=worker)
        x.body()  # the body func scales up or down

        _keep = [Mock(name=f'req{i}') for i in range(35)]
        for req in _keep:
            state.task_reserved(req)
            x.body()
            total_num_processes.append(self.pool.num_processes)

        for req in _keep:
            state.task_ready(req)
            x.body()
            total_num_processes.append(self.pool.num_processes)

        assert all(x.min_concurrency <= i <= x.max_concurrency
                   for i in total_num_processes)
