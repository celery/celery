from unittest.mock import MagicMock, Mock, patch

import pytest

from celery.app.trace import build_tracer
from celery.exceptions import Reject

redis = pytest.importorskip('redis')
from redis.exceptions import AuthenticationError, ConnectionError, ResponseError, TimeoutError  # noqa: E402

from celery.backends.redis import RedisBackend  # noqa: E402

# One read plus this many retries, with no sleep between them.
MAX_RETRIES = 3


class test_started_store_with_redis_backend:

    def setup_method(self):
        self.app.conf.result_backend_transport_options = {
            'retry_policy': {
                'max_retries': MAX_RETRIES,
                'interval_start': 0,
                'interval_step': 0,
                'interval_max': 0,
            },
        }
        self.backend = RedisBackend(app=self.app)
        self.backend.client = MagicMock(name='client')
        self.backend.client.get.return_value = None
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

    @pytest.mark.parametrize('error', [ConnectionError, TimeoutError])
    def test_requeues_for_connection_errors(self, error):
        self.backend.client.get.side_effect = error('backend gone')

        with patch.object(self.backend, 'mark_as_failure') as mark_as_failure:
            with pytest.raises(Reject) as excinfo:
                self.trace()

        assert excinfo.value.requeue is True
        assert isinstance(excinfo.value.reason, error)
        mark_as_failure.assert_not_called()
        self.ran.assert_not_called()

    def test_does_not_requeue_other_errors(self):
        self.backend.client.get.side_effect = ResponseError('bad reply')

        with patch.object(self.backend, 'mark_as_failure') as mark_as_failure, \
                patch('celery.app.trace.report_internal_error'):
            self.trace()

        mark_as_failure.assert_called()
        self.ran.assert_not_called()

    def test_requeue_is_logged(self):
        self.backend.client.get.side_effect = AuthenticationError('bad password')

        with patch('celery.app.trace.logger') as logger:
            with pytest.raises(Reject):
                self.trace()

        logger.warning.assert_called_once()
        assert 'id-1' in logger.warning.call_args[0]

    @pytest.mark.parametrize('failed_reads', [0, 1, MAX_RETRIES, MAX_RETRIES + 1, 10])
    def test_outage_length(self, failed_reads):
        self.backend.client.get.side_effect = (
            [ConnectionError('backend gone')] * failed_reads + [None] * 10
        )

        if failed_reads <= MAX_RETRIES:
            self.trace()
            self.ran.assert_called_once()
            # The STARTED read and its retries, then the SUCCESS read.
            assert self.backend.client.get.call_count == failed_reads + 2
        else:
            with pytest.raises(Reject) as excinfo:
                self.trace()
            assert excinfo.value.requeue is True
            self.ran.assert_not_called()
