import os
from unittest.mock import Mock, patch

from celery.concurrency import spawn


class MockPool:
    """Mock pool that prevents actual process creation."""
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
        self._state = 1  # RUN state
        self._processes = kwargs.get('processes', 1)
        self._proc_alive_timeout = kwargs.get('proc_alive_timeout')
        self._pool = [Mock(pid=i) for i in range(self._processes)]

    def close(self):
        self.closed = True
        self._state = 'CLOSE'

    def join(self):
        self.joined = True

    def terminate(self):
        self.terminated = True

    def did_start_ok(self):
        return True

    def apply_async(self, *args, **kwargs):
        pass

    def terminate_job(self, *args, **kwargs):
        pass

    def restart(self, *args, **kwargs):
        pass

    def register_with_event_loop(self, loop):
        pass

    def flush(self):
        pass

    def grow(self, n=1):
        self._processes += n

    def shrink(self, n=1):
        self._processes -= n


class TestTaskPool(spawn.TaskPool):
    """TaskPool that uses MockPool instead of real billiard pools."""
    Pool = MockPool
    BlockingPool = MockPool


class test_spawn_TaskPool:
    @patch('billiard.set_start_method')
    @patch('billiard.forking_enable')
    def test_on_start_sets_spawn(self, mock_forking_enable, set_method):
        pool = TestTaskPool(1)
        with patch.dict(os.environ, {}, clear=True):
            pool.on_start()
            set_method.assert_called_with('spawn', force=True)
            assert os.environ['FORKED_BY_MULTIPROCESSING'] == '1'
            # Verify the pool was created
            assert pool._pool is not None
            assert pool._pool.started
