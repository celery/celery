"""Integration tests for the ETA/countdown fix for #5321.

These tests exercise :mod:`celery.worker.strategy`,
:mod:`celery.worker.state`, :mod:`celery.worker.control` and
:mod:`celery.worker.consumer.consumer` together, the way a real worker
would use them, instead of asserting on each module in isolation. They
verify that a task scheduled with an ETA/countdown is visible to
``inspect query_task`` throughout its whole lifecycle: while it's
waiting on its timer, once it's handed off to the pool, after it
completes, and if the connection is lost while it's still pending.
"""
import socket
from collections import defaultdict
from unittest.mock import Mock

from celery.utils.functional import maybe_list
from celery.worker import WorkController as _WC
from celery.worker import consumer as consumer_module
from celery.worker import control
from celery.worker import state as worker_state
from celery.worker.strategy import default as default_strategy

hostname = socket.gethostname()


class _RealConsumer(consumer_module.Consumer):
    """A consumer with a real ``apply_eta_task``/``on_close``, but a
    stubbed-out timer/pool/broker so the strategy and control-command
    plumbing can be driven synchronously in a test.
    """

    def __init__(self, app):
        self.app = app
        self.hostname = hostname
        self.connection_errors = ()
        self.timer = Mock()
        self.event_dispatcher = Mock(enabled=False)
        self.controller = _WC(app=app)
        self.controller.consumer = self
        self.controller.semaphore = Mock()
        # NOTE: ``_WC.state`` is the shared ``celery.worker.state`` module
        # (a class attribute), so it must never be reassigned here -- doing
        # so would replace the real global ``revoked`` LimitedSet for the
        # whole test session. It already starts out empty.
        self.task_consumer = Mock()
        self.task_buckets = defaultdict(lambda: None)
        self.qos = Mock()
        self.on_task_request = Mock()
        self.disable_rate_limits = True
        self.pool = Mock()
        self.hub = None


class test_query_task_eta_lifecycle:

    def setup_method(self):
        worker_state.reset_state()

        @self.app.task(shared=False)
        def add(x, y):
            return x + y

        self.add = add
        self.consumer = _RealConsumer(self.app)
        self.panel = self.app.control.mailbox.Node(
            hostname=hostname,
            state=Mock(),
            handlers=control.Panel.data,
        )

    def teardown_method(self):
        worker_state.reset_state()

    def _schedule(self, countdown=10):
        strategy = default_strategy(self.add, self.app, self.consumer)
        message = self.task_message_from_sig(
            self.app, self.add.s(2, 2).set(countdown=countdown),
        )
        body = message.payload if not message.headers.get('id') else None
        strategy(message, body, message.ack, message.reject, [])
        # the strategy hands the scheduled request + callback off to the
        # (mocked) timer instead of running it inline.
        assert self.consumer.timer.call_at.called
        return self.consumer.timer.call_at.call_args[0][2][0]

    def _query(self, req):
        ret = self.panel.handle('query_task', {'ids': maybe_list(req.id)})
        return ret.get(req.id)

    def test_scheduled_task_is_visible_before_eta_elapses(self):
        req = self._schedule()

        result = self._query(req)

        assert result is not None
        assert result[0] == 'scheduled'
        assert req not in worker_state.reserved_requests
        assert req not in worker_state.active_requests

    def test_task_transitions_to_reserved_when_eta_fires(self):
        req = self._schedule()
        assert self._query(req)[0] == 'scheduled'

        # simulate the timer firing, as celery.worker.loops does.
        self.consumer.apply_eta_task(req)

        result = self._query(req)
        assert result is not None
        assert result[0] == 'reserved'
        assert req in worker_state.reserved_requests
        assert req not in worker_state.scheduled_requests
        self.consumer.on_task_request.assert_called_once_with(req)

    def test_task_disappears_once_ready(self):
        req = self._schedule()
        self.consumer.apply_eta_task(req)
        assert self._query(req)[0] == 'reserved'

        worker_state.task_ready(req)

        assert self._query(req) is None
        assert req.id not in worker_state.requests
        assert req not in worker_state.scheduled_requests
        assert req not in worker_state.reserved_requests

    def test_connection_loss_purges_pending_eta_task(self):
        req = self._schedule()
        assert self._query(req)[0] == 'scheduled'

        # broker connection drops before the ETA elapses.
        self.consumer.on_close()

        assert self._query(req) is None
        assert req.id not in worker_state.requests
        assert req not in worker_state.scheduled_requests
