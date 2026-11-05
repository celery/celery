from unittest.mock import MagicMock, Mock, patch

import pytest

from celery import states
from celery.app.trace import build_tracer
from celery.backends.base import BaseBackend, store_retry_timeout
from celery.exceptions import BackendStoreError, Reject

redis = pytest.importorskip('redis')
from redis.exceptions import AuthenticationError, ConnectionError, ResponseError, TimeoutError  # noqa: E402

from celery.backends.redis import RedisBackend  # noqa: E402

TIMEOUT = 20.0


class FakeClock:
    """Stands in for time.monotonic(); the backend's sleeps advance it."""

    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds


@pytest.fixture
def clock():
    clock = FakeClock()
    with patch('celery.backends.base.time.monotonic', clock):
        yield clock


class test_started_store_with_redis_backend:

    @pytest.fixture(autouse=True)
    def setup_backend(self, clock):
        self.clock = clock
        self.app.conf.result_backend_store_retry_timeout = TIMEOUT
        self.backend = RedisBackend(app=self.app)
        self.backend.client = MagicMock(name='client')
        self.backend.client.get.return_value = None
        self.backend._sleep = clock.sleep
        self.ran = Mock(name='body')
        ran = self.ran

        @self.app.task(shared=False, track_started=True, acks_late=True)
        def add(x, y):
            ran()
            return x + y

        add.backend = self.backend
        self.add = add

    def trace(self):
        tracer = build_tracer(self.add.name, self.add, eager=False, app=self.app)
        return tracer('id-1', (2, 2), {}, None)

    def stored_states(self):
        # Every store publishes the value it writes.
        published = self.backend.client.pipeline.return_value.__enter__.return_value.publish
        return [self.backend.decode(c.args[1])['status'] for c in published.call_args_list]

    def test_short_outage_runs_the_task_once(self):
        self.backend.client.get.side_effect = (
            [ConnectionError('backend gone')] * 5 + [None] * 10
        )

        self.trace()

        self.ran.assert_called_once()
        assert self.stored_states() == [states.STARTED, states.SUCCESS]
        assert self.clock.now < TIMEOUT

    def test_outage_while_storing_success_keeps_the_result(self):
        # The STARTED store works, then the backend goes away for a while.
        self.backend.client.get.side_effect = (
            [None] + [ConnectionError('backend gone')] * 5 + [None] * 10
        )

        self.trace()

        self.ran.assert_called_once()
        assert self.stored_states() == [states.STARTED, states.SUCCESS]

    @pytest.mark.parametrize('error', [ConnectionError, TimeoutError])
    def test_long_outage_requeues(self, error):
        self.backend.client.get.side_effect = error('backend gone')

        with patch.object(self.backend, 'mark_as_failure') as mark_as_failure:
            with pytest.raises(Reject) as excinfo:
                self.trace()

        assert excinfo.value.requeue is True
        assert isinstance(excinfo.value.reason, error)
        assert self.clock.now >= TIMEOUT
        mark_as_failure.assert_not_called()
        self.ran.assert_not_called()

    def test_does_not_retry_other_errors(self):
        self.backend.client.get.side_effect = ResponseError('bad reply')

        with patch.object(self.backend, 'mark_as_failure') as mark_as_failure, \
                patch('celery.app.trace.report_internal_error'):
            self.trace()

        assert self.backend.client.get.call_count == 1
        mark_as_failure.assert_called()
        self.ran.assert_not_called()

    def test_requeue_is_logged(self):
        self.backend.client.get.side_effect = AuthenticationError('bad password')

        with patch('celery.app.trace.logger') as logger:
            with pytest.raises(Reject):
                self.trace()

        logger.warning.assert_called_once()
        assert 'id-1' in logger.warning.call_args[0]

    def test_without_timeout_requeues_at_once(self):
        self.app.conf.result_backend_store_retry_timeout = None
        self.backend.client.get.side_effect = ConnectionError('backend gone')

        with pytest.raises(Reject):
            self.trace()

        assert self.backend.client.get.call_count == 1
        assert self.clock.now == 0

    def test_timeout_only_applies_while_the_task_runs(self):
        self.trace()
        assert store_retry_timeout.get() is None

        self.backend.client.get.side_effect = ConnectionError('backend gone')
        with pytest.raises(ConnectionError):
            self.backend.store_result('id-2', None, states.FAILURE)

        assert self.backend.client.get.call_count == 3  # STARTED, SUCCESS, then 1
        assert self.clock.now == 0


class test_store_result_retry_timeout:

    class FlakyBackend(BaseBackend):
        """Fails to store, like the database backend after its own retries."""

        def __init__(self, failures, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.failures = failures
            self.calls = 0

        def exception_safe_to_retry(self, exc):
            return isinstance(exc, OSError)

        def _store_result(self, *args, **kwargs):
            self.calls += 1
            if self.calls <= self.failures:
                try:
                    raise OSError('database gone')
                except OSError as exc:
                    raise BackendStoreError('failed to store result') from exc

    def store(self, backend, clock):
        backend._sleep = clock.sleep
        token = store_retry_timeout.set(TIMEOUT)
        try:
            return backend.store_result('id-1', 42, states.SUCCESS)
        finally:
            store_retry_timeout.reset(token)

    def test_retries_a_wrapped_transient_error(self, clock):
        backend = self.FlakyBackend(3, app=self.app)

        assert self.store(backend, clock) == 42
        assert backend.calls == 4

    def test_gives_up_after_the_timeout(self, clock):
        backend = self.FlakyBackend(10 ** 6, app=self.app)

        with pytest.raises(BackendStoreError):
            self.store(backend, clock)

        assert clock.now == pytest.approx(TIMEOUT)

    def test_does_not_retry_a_store_error_without_a_transient_cause(self, clock):
        backend = self.FlakyBackend(0, app=self.app)
        backend._store_result = Mock(side_effect=BackendStoreError('value too large'))

        with pytest.raises(BackendStoreError):
            self.store(backend, clock)

        backend._store_result.assert_called_once()
