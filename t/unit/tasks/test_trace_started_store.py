from unittest.mock import MagicMock, Mock, patch

import pytest

from celery import states, uuid
from celery.app.trace import STORE_RETRY_LIMIT_MARGIN, build_tracer
from celery.backends.base import STORE_RETRY_MAX_SLEEP, BaseBackend, store_retry
from celery.exceptions import BackendStoreError, Reject

redis = pytest.importorskip('redis')
from redis.exceptions import AuthenticationError, ConnectionError, ResponseError, TimeoutError  # noqa: E402

from celery.backends.redis import RedisBackend  # noqa: E402

TIMEOUT = 20.0


class FakeClock:
    """Stands in for time.monotonic(); the backend's sleeps advance it."""

    def __init__(self):
        self.now = 0.0
        self.sleeps = []

    def __call__(self):
        return self.now

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds


@pytest.fixture
def clock():
    clock = FakeClock()
    with patch('time.monotonic', clock):
        yield clock


def test_default_timeout(app):
    assert app.conf.result_backend_store_retry_timeout == 20.0


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
        self.add = self.make_task()

    def make_task(self, body=None, **options):
        ran = self.ran

        @self.app.task(name=f'add-{uuid()}', shared=False, bind=True,
                       track_started=True, acks_late=True, **options)
        def add(self_, x, y):
            ran()
            if body:
                body(self_)
            return x + y

        add.backend = self.backend
        return add

    def trace(self, task=None, eager=False):
        task = task or self.add
        tracer = build_tracer(task.name, task, eager=eager, app=self.app)
        return tracer('id-1', (2, 2), {}, None)

    def stored_states(self):
        # Every store publishes the value it writes.
        published = self.backend.client.pipeline.return_value.__enter__.return_value.publish
        return [self.backend.decode(c.args[1])['status'] for c in published.call_args_list]

    def test_short_outage_runs_the_task_once(self):
        self.backend.client.get.side_effect = (
            [ConnectionError('backend gone')] * 5 + [None] * 10
        )

        with patch('celery.backends.base.logger') as logger:
            self.trace()

        self.ran.assert_called_once()
        assert self.stored_states() == [states.STARTED, states.SUCCESS]
        assert self.clock.now < TIMEOUT
        assert logger.warning.call_count == 5

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
        assert self.clock.now == pytest.approx(TIMEOUT)
        mark_as_failure.assert_not_called()
        self.ran.assert_not_called()

    def test_long_outage_backs_off(self):
        self.backend.client.get.side_effect = ConnectionError('backend gone')

        with pytest.raises(Reject):
            self.trace()

        sleeps = self.clock.sleeps
        assert max(sleeps) <= STORE_RETRY_MAX_SLEEP
        # The sleeps grow up to the cap, so a long outage costs few reads.
        assert max(sleeps[-10:]) > 0.1
        assert self.backend.client.get.call_count < 200

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

    def test_eager_tasks_do_not_wait(self):
        task = self.make_task(store_eager_result=True)
        self.backend.client.get.side_effect = ConnectionError('backend gone')

        with patch('celery.app.trace.report_internal_error'), \
                pytest.raises(ConnectionError):
            self.trace(task, eager=True)

        assert self.clock.now == 0

    def test_stops_short_of_the_soft_time_limit(self):
        task = self.make_task(soft_time_limit=5)
        self.backend.client.get.side_effect = ConnectionError('backend gone')

        with pytest.raises(Reject):
            self.trace(task)

        assert self.clock.now == pytest.approx(5 - STORE_RETRY_LIMIT_MARGIN)

    def test_stores_from_the_task_body_fail_fast(self):
        errors = []

        def body(task):
            self.backend.client.get.side_effect = ConnectionError('backend gone')
            try:
                task.update_state(task_id='id-1', state='PROGRESS')
            except ConnectionError as exc:
                errors.append(exc)
            self.backend.client.get.side_effect = None

        self.trace(self.make_task(body))

        assert len(errors) == 1
        assert self.clock.now == 0

    def test_timeout_only_applies_while_the_task_runs(self):
        self.trace()
        assert store_retry.get() is None

        self.backend.client.get.side_effect = ConnectionError('backend gone')
        with pytest.raises(ConnectionError):
            self.backend.store_result('id-2', None, states.FAILURE)

        assert self.backend.client.get.call_count == 3  # STARTED, SUCCESS, then 1
        assert self.clock.now == 0


class test_store_result_retry:
    """A backend that retries its own errors, like the database backend."""

    class FlakyBackend(BaseBackend):

        def __init__(self, failures, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.always_retry = True
            self.max_retries = 3
            self.failures = failures
            self.calls = 0

        def exception_safe_to_retry(self, exc):
            return isinstance(exc, OSError)

        def _store_result(self, *args, **kwargs):
            self.calls += 1
            if self.calls <= self.failures:
                raise OSError('database gone')

    def store(self, backend, clock):
        backend._sleep = clock.sleep
        token = store_retry.set((TIMEOUT, None))
        try:
            return backend.store_result('id-1', 42, states.SUCCESS)
        finally:
            store_retry.reset(token)

    def test_retries_past_max_retries_until_the_timeout(self, clock):
        backend = self.FlakyBackend(10, app=self.app)

        assert self.store(backend, clock) == 42
        assert backend.calls == 11

    def test_gives_up_after_the_timeout(self, clock):
        backend = self.FlakyBackend(10 ** 6, app=self.app)

        with pytest.raises(BackendStoreError) as excinfo:
            self.store(backend, clock)

        assert isinstance(excinfo.value.__cause__, OSError)
        assert backend.store_error_safe_to_retry(excinfo.value)
        assert clock.now == pytest.approx(TIMEOUT)

    def test_one_retry_loop_logs_the_backend_error(self, clock):
        backend = self.FlakyBackend(10, app=self.app)
        backend.on_backend_retryable_error = Mock(name='on_backend_retryable_error')

        with patch('celery.backends.base.logger') as logger:
            self.store(backend, clock)

        assert logger.warning.call_count == 10
        for c in logger.warning.call_args_list:
            assert 'exc_info' not in c.kwargs
            assert any(isinstance(arg, OSError) for arg in c.args)
        assert backend.on_backend_retryable_error.call_count == 10

    def test_retries_left_after_the_deadline_still_sleep(self, clock):
        # The time limit is already up, but result_backend_always_retry still
        # allows max_retries tries: they keep the backoff instead of looping
        # without a pause.
        backend = self.FlakyBackend(10, app=self.app)
        backend._sleep = clock.sleep
        token = store_retry.set((TIMEOUT, clock.now - 1))
        try:
            with pytest.raises(BackendStoreError):
                backend.store_result('id-1', 42, states.SUCCESS)
        finally:
            store_retry.reset(token)

        assert backend.calls == 4
        assert len(clock.sleeps) == 3
        assert sum(clock.sleeps) > 0

    def test_without_the_timeout_keeps_max_retries(self, clock):
        backend = self.FlakyBackend(10, app=self.app)
        backend._sleep = clock.sleep

        with pytest.raises(BackendStoreError):
            backend.store_result('id-1', 42, states.SUCCESS)

        assert backend.calls == 4

    def test_does_not_retry_a_store_error_without_a_transient_cause(self, clock):
        backend = self.FlakyBackend(0, app=self.app)
        backend._store_result = Mock(side_effect=BackendStoreError('value too large'))

        with pytest.raises(BackendStoreError) as excinfo:
            self.store(backend, clock)

        backend._store_result.assert_called_once()
        assert not backend.store_error_safe_to_retry(excinfo.value)
