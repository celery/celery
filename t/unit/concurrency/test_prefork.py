import errno
import os
import socket
import tempfile
from itertools import cycle
from unittest.mock import Mock, patch

import pytest
from billiard.pool import ApplyResult
from kombu.asynchronous import Hub

import t.skip
from celery.app.defaults import DEFAULTS
from celery.concurrency.asynpool import iterate_file_descriptors_safely
from celery.utils.collections import AttributeDict
from celery.utils.functional import noop
from celery.utils.objects import Bunch

try:
    from celery.concurrency import asynpool
    from celery.concurrency import prefork as mp
except ImportError:

    class _mp:
        RUN = 0x1

        class TaskPool:
            _pool = Mock()

            def __init__(self, *args, **kwargs):
                pass

            def start(self):
                pass

            def stop(self):
                pass

            def apply_async(self, *args, **kwargs):
                pass
    mp = _mp()
    asynpool = None


class MockResult:

    def __init__(self, value, pid):
        self.value = value
        self.pid = pid

    def worker_pids(self):
        return [self.pid]

    def get(self):
        return self.value


@patch('celery.platforms.set_mp_process_title')
class test_process_initializer:

    @staticmethod
    def Loader(*args, **kwargs):
        loader = Mock(*args, **kwargs)
        loader.conf = {}
        loader.override_backends = {}
        return loader

    @patch('celery.platforms.signals')
    def test_process_initializer(self, _signals, set_mp_process_title, restore_logging):
        from celery import signals
        from celery._state import _tls
        from celery.concurrency.prefork import WORKER_SIGIGNORE, WORKER_SIGRESET, process_initializer
        on_worker_process_init = Mock()
        signals.worker_process_init.connect(on_worker_process_init)

        with self.Celery(loader=self.Loader) as app:
            app.conf = AttributeDict(DEFAULTS)
            process_initializer(app, 'awesome.worker.com')
            _signals.ignore.assert_any_call(*WORKER_SIGIGNORE)
            _signals.reset.assert_any_call(*WORKER_SIGRESET)
            assert app.loader.init_worker.call_count
            on_worker_process_init.assert_called()
            assert _tls.current_app is app
            set_mp_process_title.assert_called_with(
                'celeryd', hostname='awesome.worker.com',
            )

            with patch('celery.app.trace.setup_worker_optimizations') as S:
                os.environ['FORKED_BY_MULTIPROCESSING'] = '1'
                try:
                    process_initializer(app, 'luke.worker.com')
                    S.assert_called_with(app, 'luke.worker.com')
                finally:
                    os.environ.pop('FORKED_BY_MULTIPROCESSING', None)

            os.environ['CELERY_LOG_FILE'] = 'worker%I.log'
            app.log.setup = Mock(name='log_setup')
            try:
                process_initializer(app, 'luke.worker.com')
            finally:
                os.environ.pop('CELERY_LOG_FILE', None)

    @patch('celery.platforms.set_pdeathsig')
    def test_pdeath_sig(self, _set_pdeathsig, set_mp_process_title, restore_logging):
        from celery import signals
        on_worker_process_init = Mock()
        signals.worker_process_init.connect(on_worker_process_init)
        from celery.concurrency.prefork import process_initializer

        with self.Celery(loader=self.Loader) as app:
            app.conf = AttributeDict(DEFAULTS)
            process_initializer(app, 'awesome.worker.com')
        _set_pdeathsig.assert_called_once_with('SIGKILL')


class test_process_destructor:

    @patch('celery.concurrency.prefork.signals')
    def test_process_destructor(self, signals):
        mp.process_destructor(13, -3)
        signals.worker_process_shutdown.send.assert_called_with(
            sender=None, pid=13, exitcode=-3,
        )


class MockPool:
    started = False
    closed = False
    joined = False
    terminated = False
    _state = None

    def __init__(self, *args, **kwargs):
        self.started = True
        self._timeout_handler = Mock()
        self._result_handler = Mock()
        self.maintain_pool = Mock()
        self._state = mp.RUN
        self._processes = kwargs.get('processes')
        self._proc_alive_timeout = kwargs.get('proc_alive_timeout')
        self._pool = [Bunch(pid=i, inqW_fd=1, outqR_fd=2)
                      for i in range(self._processes)]
        self._current_proc = cycle(range(self._processes))

    def close(self):
        self.closed = True
        self._state = 'CLOSE'

    def join(self):
        self.joined = True

    def terminate(self):
        self.terminated = True

    def terminate_job(self, *args, **kwargs):
        pass

    def restart(self, *args, **kwargs):
        pass

    def handle_result_event(self, *args, **kwargs):
        pass

    def flush(self):
        pass

    def grow(self, n=1):
        self._processes += n

    def shrink(self, n=1):
        self._processes -= n

    def apply_async(self, *args, **kwargs):
        pass

    def register_with_event_loop(self, loop):
        pass


class ExeMockPool(MockPool):

    def apply_async(self, target, args=(), kwargs={}, callback=noop):
        from threading import Timer
        res = target(*args, **kwargs)
        Timer(0.1, callback, (res,)).start()
        return MockResult(res, next(self._current_proc))


class TaskPool(mp.TaskPool):
    Pool = BlockingPool = MockPool


class ExeMockTaskPool(mp.TaskPool):
    Pool = BlockingPool = ExeMockPool


@t.skip.if_win32
class test_AsynPool:

    def setup_method(self):
        pytest.importorskip('multiprocessing')

    def test_gen_not_started(self):

        def gen():
            yield 1
            assert not asynpool.gen_not_started(g)
            yield 2
        g = gen()
        assert asynpool.gen_not_started(g)
        next(g)
        assert not asynpool.gen_not_started(g)
        list(g)
        assert not asynpool.gen_not_started(g)

        def gen2():
            yield 1
            raise RuntimeError('generator error')
        g = gen2()
        assert asynpool.gen_not_started(g)
        next(g)
        assert not asynpool.gen_not_started(g)
        with pytest.raises(RuntimeError):
            next(g)
        assert not asynpool.gen_not_started(g)

    @patch('select.select', create=True)
    def test_select(self, __select):
        ebadf = socket.error()
        ebadf.errno = errno.EBADF
        with patch('select.poll', create=True) as poller:
            poll = poller.return_value = Mock(name='poll.poll')
            poll.return_value = {3}, set(), 0
            assert asynpool._select({3}, poll=poll) == ({3}, set(), 0)

            poll.return_value = {3}, set(), 0
            assert asynpool._select({3}, None, {3}, poll=poll) == (
                {3}, set(), 0,
            )

            eintr = socket.error()
            eintr.errno = errno.EINTR
            poll.side_effect = eintr

            readers = {3}
            assert asynpool._select(readers, poll=poll) == (set(), set(), 1)
            assert 3 in readers

        with patch('select.poll', create=True) as poller:
            poll = poller.return_value = Mock(name='poll.poll')
            poll.side_effect = ebadf
            with patch('select.select') as selcheck:
                selcheck.side_effect = ebadf
                readers = {3}
                assert asynpool._select(readers, poll=poll) == (
                    set(), set(), 1,
                )
                assert 3 not in readers

        with patch('select.poll', create=True) as poller:
            poll = poller.return_value = Mock(name='poll.poll')
            poll.side_effect = MemoryError()
            with pytest.raises(MemoryError):
                asynpool._select({1}, poll=poll)

        with patch('select.poll', create=True) as poller:
            poll = poller.return_value = Mock(name='poll.poll')
            with patch('select.select') as selcheck:

                def se(*args):
                    selcheck.side_effect = MemoryError()
                    raise ebadf
                poll.side_effect = se
                with pytest.raises(MemoryError):
                    asynpool._select({3}, poll=poll)

        with patch('select.poll', create=True) as poller:
            poll = poller.return_value = Mock(name='poll.poll')
            with patch('select.select') as selcheck:

                def se2(*args):
                    selcheck.side_effect = socket.error()
                    selcheck.side_effect.errno = 1321
                    raise ebadf
                poll.side_effect = se2
                with pytest.raises(socket.error):
                    asynpool._select({3}, poll=poll)

        with patch('select.poll', create=True) as poller:
            poll = poller.return_value = Mock(name='poll.poll')

            poll.side_effect = socket.error()
            poll.side_effect.errno = 34134
            with pytest.raises(socket.error):
                asynpool._select({3}, poll=poll)

    def test_select_unpatched(self):
        with tempfile.TemporaryFile('w') as f:
            _, writeable, _ = asynpool._select(writers={f, }, err={f, })
            assert f.fileno() in writeable

        with tempfile.TemporaryFile('r') as f:
            readable, _, _ = asynpool._select(readers={f, }, err={f, })
            assert f.fileno() in readable

    def test_promise(self):
        fun = Mock()
        x = asynpool.promise(fun, (1,), {'foo': 1})
        x()
        assert x.ready
        fun.assert_called_with(1, foo=1)

    def test_Worker(self):
        w = asynpool.Worker(Mock(), Mock())
        w.on_loop_start(1234)
        w.outq.put.assert_called_with((asynpool.WORKER_UP, (1234,)))

    def test_iterate_file_descriptors_safely_source_data_list(self):
        # Given: a list of integers that could be file descriptors
        fd_iter = [1, 2, 3, 4, 5]

        # Given: a mock hub method that does nothing to call
        def _fake_hub(*args, **kwargs):
            raise OSError

        # When Calling the helper to iterate_file_descriptors_safely
        iterate_file_descriptors_safely(
            fd_iter, fd_iter, _fake_hub,
            "arg1", "arg2", kw1="kw1", kw2="kw2",
        )

        # Then: all items were removed from the managed data source
        assert fd_iter == [], "Expected all items removed from managed list"

    def test_iterate_file_descriptors_safely_source_data_set(self):
        # Given: a list of integers that could be file descriptors
        fd_iter = {1, 2, 3, 4, 5}

        # Given: a mock hub method that does nothing to call
        def _fake_hub(*args, **kwargs):
            raise OSError

        # When Calling the helper to iterate_file_descriptors_safely
        iterate_file_descriptors_safely(
            fd_iter, fd_iter, _fake_hub,
            "arg1", "arg2", kw1="kw1", kw2="kw2",
        )

        # Then: all items were removed from the managed data source
        assert fd_iter == set(), "Expected all items removed from managed set"

    def test_iterate_file_descriptors_safely_source_data_dict(self):
        # Given: a list of integers that could be file descriptors
        fd_iter = {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}

        # Given: a mock hub method that does nothing to call
        def _fake_hub(*args, **kwargs):
            raise OSError

        # When Calling the helper to iterate_file_descriptors_safely
        iterate_file_descriptors_safely(
            fd_iter, fd_iter, _fake_hub,
            "arg1", "arg2", kw1="kw1", kw2="kw2",
        )

        # Then: all items were removed from the managed data source
        assert fd_iter == {}, "Expected all items removed from managed dict"

    def _get_hub(self):
        hub = Hub()
        hub.readers = {}
        hub.writers = {}
        hub.timer = Mock(name='hub.timer')
        hub.timer._queue = [Mock()]
        hub.fire_timers = Mock(name='hub.fire_timers')
        hub.fire_timers.return_value = 1.7
        hub.poller = Mock(name='hub.poller')
        hub.close = Mock(name='hub.close()')
        return hub

    @t.skip.if_pypy
    def test_schedule_writes_hub_remove_writer_ready_fd_not_in_all_inqueues(self):
        pool = asynpool.AsynPool(threads=False)
        hub = self._get_hub()

        writer = Mock(name='writer')
        reader = Mock(name='reader')

        # add 2 fake fds with the same id
        hub.add_reader(6, reader, 6)
        hub.add_writer(6, writer, 6)
        pool._all_inqueues.clear()
        pool._create_write_handlers(hub)

        # check schedule_writes write fds remove not remove the reader one from the hub.
        hub.consolidate_callback(ready_fds=[6])
        assert 6 in hub.readers
        assert 6 not in hub.writers

    @t.skip.if_pypy
    def test_schedule_writes_hub_remove_writers_from_active_writers_when_get_index_error(self):
        pool = asynpool.AsynPool(threads=False)
        hub = self._get_hub()

        writer = Mock(name='writer')
        reader = Mock(name='reader')

        # add 3 fake fds with the same id to reader and writer
        hub.add_reader(6, reader, 6)
        hub.add_reader(8, reader, 8)
        hub.add_reader(9, reader, 9)
        hub.add_writer(6, writer, 6)
        hub.add_writer(8, writer, 8)
        hub.add_writer(9, writer, 9)

        # add fake fd to pool _all_inqueues to make sure we try to read from outbound_buffer
        # set active_writes to 6 to make sure we remove all write fds except 6
        pool._active_writes = {6}
        pool._all_inqueues = {2, 6, 8, 9}

        pool._create_write_handlers(hub)

        # clear outbound_buffer to get IndexError when trying to pop any message
        # in this case all active_writers fds will be removed from the hub
        pool.outbound_buffer.clear()

        hub.consolidate_callback(ready_fds=[2])
        if {6, 8, 9} <= hub.readers.keys() and not {8, 9} <= hub.writers.keys():
            assert True
        else:
            assert False

        assert 6 in hub.writers

    @t.skip.if_pypy
    def test_schedule_writes_hub_remove_fd_only_from_writers_when_write_job_is_done(self):
        pool = asynpool.AsynPool(threads=False)
        hub = self._get_hub()

        writer = Mock(name='writer')
        reader = Mock(name='reader')

        # add one writer and one reader with the same fd
        hub.add_writer(2, writer, 2)
        hub.add_reader(2, reader, 2)
        assert 2 in hub.writers

        # For test purposes to reach _write_job in schedule writes
        pool._all_inqueues = {2}
        worker = Mock("worker")
        # this lambda need to return a number higher than 4
        # to pass the while loop in _write_job function and to reach the hub.remove_writer
        worker.send_job_offset = lambda header, HW: 5

        pool._fileno_to_inq[2] = worker
        pool._create_write_handlers(hub)

        result = ApplyResult({}, lambda x: True)
        result._payload = [None, None, -1]
        pool.outbound_buffer.appendleft(result)

        hub.consolidate_callback(ready_fds=[2])
        assert 2 not in hub.writers
        assert 2 in hub.readers

    @t.skip.if_pypy
    def test_register_with_event_loop__no_on_tick_dupes(self):
        """Ensure AsynPool's register_with_event_loop only registers
        on_poll_start in the event loop the first time it's called. This
        prevents a leak when the Consumer is restarted.
        """
        pool = asynpool.AsynPool(threads=False)
        hub = Mock(name='hub')
        pool.register_with_event_loop(hub)
        pool.register_with_event_loop(hub)
        hub.on_tick.add.assert_called_once()

    @t.skip.if_pypy
    @patch('billiard.pool.Pool._create_worker_process')
    def test_before_create_process_signal(self, create_process):
        from celery import signals
        on_worker_before_create_process = Mock()
        signals.worker_before_create_process.connect(on_worker_before_create_process)
        pool = asynpool.AsynPool(processes=1, threads=False)
        create_process.assert_called_once_with(0)
        on_worker_before_create_process.assert_any_call(
            signal=signals.worker_before_create_process,
            sender=pool,
        )


@t.skip.if_win32
class test_ResultHandler:

    def setup_method(self):
        pytest.importorskip('multiprocessing')

    def test_process_result(self):
        x = asynpool.ResultHandler(
            Mock(), Mock(), {}, Mock(),
            Mock(), Mock(), Mock(), Mock(),
            fileno_to_outq={},
            on_process_alive=Mock(),
            on_job_ready=Mock(),
        )
        assert x
        hub = Mock(name='hub')
        recv = x._recv_message = Mock(name='recv_message')
        recv.return_value = iter([])
        x.on_state_change = Mock()
        x.register_with_event_loop(hub)
        proc = x.fileno_to_outq[3] = Mock()
        reader = proc.outq._reader
        reader.poll.return_value = False
        x.handle_event(6)  # KeyError
        x.handle_event(3)
        x._recv_message.assert_called_with(
            hub.add_reader, 3, x.on_state_change,
        )


class test_TaskPool:

    def test_start(self):
        pool = TaskPool(10)
        pool.start()
        assert pool._pool.started
        assert pool._pool._state == asynpool.RUN

        _pool = pool._pool
        pool.stop()
        assert _pool.closed
        assert _pool.joined
        pool.stop()

        pool.start()
        _pool = pool._pool
        pool.terminate()
        pool.terminate()
        assert _pool.terminated

    def test_restart(self):
        pool = TaskPool(10)
        pool._pool = Mock(name='pool')
        pool.restart()
        pool._pool.restart.assert_called_with()
        pool._pool.apply_async.assert_called_with(mp.noop)

    def test_did_start_ok(self):
        pool = TaskPool(10)
        pool._pool = Mock(name='pool')
        assert pool.did_start_ok() is pool._pool.did_start_ok()

    def test_register_with_event_loop(self):
        pool = TaskPool(10)
        pool._pool = Mock(name='pool')
        loop = Mock(name='loop')
        pool.register_with_event_loop(loop)
        pool._pool.register_with_event_loop.assert_called_with(loop)

    def test_on_close(self):
        pool = TaskPool(10)
        pool._pool = Mock(name='pool')
        pool._pool._state = mp.RUN
        pool.on_close()
        pool._pool.close.assert_called_with()

    def test_on_close__pool_not_running(self):
        pool = TaskPool(10)
        pool._pool = Mock(name='pool')
        pool._pool._state = mp.CLOSE
        pool.on_close()
        pool._pool.close.assert_not_called()

    def test_apply_async(self):
        pool = TaskPool(10)
        pool.start()
        pool.apply_async(lambda x: x, (2,), {})

    def test_grow_shrink(self):
        pool = TaskPool(10)
        pool.start()
        assert pool._pool._processes == 10
        pool.grow()
        assert pool._pool._processes == 11
        pool.shrink(2)
        assert pool._pool._processes == 9

    def test_info(self):
        pool = TaskPool(10)
        procs = [Bunch(pid=i) for i in range(pool.limit)]

        class _Pool:
            _pool = procs
            _maxtasksperchild = None
            timeout = 10
            soft_timeout = 5

            def human_write_stats(self, *args, **kwargs):
                return {}
        pool._pool = _Pool()
        info = pool.info
        assert info['max-concurrency'] == pool.limit
        assert info['max-tasks-per-child'] == 'N/A'
        assert info['timeouts'] == (5, 10)

    def test_num_processes(self):
        pool = TaskPool(7)
        pool.start()
        assert pool.num_processes == 7

    @patch('billiard.forking_enable')
    def test_on_start_proc_alive_timeout_default(self, __forking_enable):
        app = Mock(conf=AttributeDict(DEFAULTS))
        pool = TaskPool(4, app=app)
        pool.on_start()
        assert pool._pool._proc_alive_timeout == 4.0

    @patch('billiard.forking_enable')
    def test_on_start_proc_alive_timeout_custom(self, __forking_enable):
        app = Mock(conf=AttributeDict(DEFAULTS))
        app.conf.worker_proc_alive_timeout = 8.0
        pool = TaskPool(4, app=app)
        pool.on_start()
        assert pool._pool._proc_alive_timeout == 8.0
