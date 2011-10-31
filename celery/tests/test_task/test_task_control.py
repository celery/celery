from __future__ import absolute_import
from __future__ import with_statement

from functools import wraps

from kombu.pidbox import Mailbox

from celery.app import app_or_default
from celery.task import control
from celery.task import PingTask
from celery.utils import uuid
from celery.tests.utils import unittest


class MockMailbox(Mailbox):
    sent = []

    def _publish(self, command, *args, **kwargs):
        self.__class__.sent.append(command)

    def close(self):
        pass

    def _collect(self, *args, **kwargs):
        pass


class Control(control.Control):
    Mailbox = MockMailbox


def with_mock_broadcast(fun):

    @wraps(fun)
    def _resets(*args, **kwargs):
        MockMailbox.sent = []
        try:
            return fun(*args, **kwargs)
        finally:
            MockMailbox.sent = []
    return _resets


class test_inspect(unittest.TestCase):

    def setUp(self):
        app = app_or_default()
        self.i = Control(app=app).inspect()

    def test_prepare_reply(self):
        self.assertDictEqual(self.i._prepare([{"w1": {"ok": 1}},
                                              {"w2": {"ok": 1}}]),
                             {"w1": {"ok": 1}, "w2": {"ok": 1}})

        i = control.inspect(destination="w1")
        self.assertEqual(i._prepare([{"w1": {"ok": 1}}]),
                         {"ok": 1})

    @with_mock_broadcast
    def test_active(self):
        self.i.active()
        self.assertIn("dump_active", MockMailbox.sent)

    @with_mock_broadcast
    def test_scheduled(self):
        self.i.scheduled()
        self.assertIn("dump_schedule", MockMailbox.sent)

    @with_mock_broadcast
    def test_reserved(self):
        self.i.reserved()
        self.assertIn("dump_reserved", MockMailbox.sent)

    @with_mock_broadcast
    def test_stats(self):
        self.i.stats()
        self.assertIn("stats", MockMailbox.sent)

    @with_mock_broadcast
    def test_revoked(self):
        self.i.revoked()
        self.assertIn("dump_revoked", MockMailbox.sent)

    @with_mock_broadcast
    def test_asks(self):
        self.i.registered()
        self.assertIn("dump_tasks", MockMailbox.sent)

    @with_mock_broadcast
    def test_enable_events(self):
        self.i.enable_events()
        self.assertIn("enable_events", MockMailbox.sent)

    @with_mock_broadcast
    def test_disable_events(self):
        self.i.disable_events()
        self.assertIn("disable_events", MockMailbox.sent)

    @with_mock_broadcast
    def test_ping(self):
        self.i.ping()
        self.assertIn("ping", MockMailbox.sent)

    @with_mock_broadcast
    def test_add_consumer(self):
        self.i.add_consumer("foo")
        self.assertIn("add_consumer", MockMailbox.sent)

    @with_mock_broadcast
    def test_cancel_consumer(self):
        self.i.cancel_consumer("foo")
        self.assertIn("cancel_consumer", MockMailbox.sent)


class test_Broadcast(unittest.TestCase):

    def setUp(self):
        self.app = app_or_default()
        self.control = Control(app=self.app)
        self.app.control = self.control

    def tearDown(self):
        del(self.app.control)

    def test_discard_all(self):
        self.control.discard_all()

    @with_mock_broadcast
    def test_broadcast(self):
        self.control.broadcast("foobarbaz", arguments=[])
        self.assertIn("foobarbaz", MockMailbox.sent)

    @with_mock_broadcast
    def test_broadcast_limit(self):
        self.control.broadcast("foobarbaz1", arguments=[], limit=None,
                destination=[1, 2, 3])
        self.assertIn("foobarbaz1", MockMailbox.sent)

    @with_mock_broadcast
    def test_broadcast_validate(self):
        with self.assertRaises(ValueError):
            self.control.broadcast("foobarbaz2",
                                   destination="foo")

    @with_mock_broadcast
    def test_rate_limit(self):
        self.control.rate_limit(PingTask.name, "100/m")
        self.assertIn("rate_limit", MockMailbox.sent)

    @with_mock_broadcast
    def test_revoke(self):
        self.control.revoke("foozbaaz")
        self.assertIn("revoke", MockMailbox.sent)

    @with_mock_broadcast
    def test_ping(self):
        self.control.ping()
        self.assertIn("ping", MockMailbox.sent)

    @with_mock_broadcast
    def test_revoke_from_result(self):
        self.app.AsyncResult("foozbazzbar").revoke()
        self.assertIn("revoke", MockMailbox.sent)

    @with_mock_broadcast
    def test_revoke_from_resultset(self):
        r = self.app.TaskSetResult(uuid(),
                                   map(self.app.AsyncResult,
                                        [uuid() for i in range(10)]))
        r.revoke()
        self.assertIn("revoke", MockMailbox.sent)
