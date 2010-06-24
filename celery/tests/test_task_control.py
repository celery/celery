import unittest2 as unittest

from celery.task import control
from celery.task.builtins import PingTask
from celery.utils import gen_unique_id
from celery.utils.functional import wraps


class MockBroadcastPublisher(object):
    sent = []

    def __init__(self, *args, **kwargs):
        pass

    def send(self, command, *args, **kwargs):
        self.__class__.sent.append(command)

    def close(self):
        pass


class MockControlReplyConsumer(object):

    def __init__(self, *args, **kwarg):
        pass

    def collect(self, *args, **kwargs):
        pass

    def close(self):
        pass


def with_mock_broadcast(fun):

    @wraps(fun)
    def _mocked(*args, **kwargs):
        old_pub = control.BroadcastPublisher
        old_rep = control.ControlReplyConsumer
        control.BroadcastPublisher = MockBroadcastPublisher
        control.ControlReplyConsumer = MockControlReplyConsumer
        try:
            return fun(*args, **kwargs)
        finally:
            MockBroadcastPublisher.sent = []
            control.BroadcastPublisher = old_pub
            control.ControlReplyConsumer = old_rep
    return _mocked


class test_Broadcast(unittest.TestCase):

    def test_discard_all(self):
        control.discard_all()

    @with_mock_broadcast
    def test_broadcast(self):
        control.broadcast("foobarbaz", arguments=[])
        self.assertIn("foobarbaz", MockBroadcastPublisher.sent)

    @with_mock_broadcast
    def test_broadcast_limit(self):
        control.broadcast("foobarbaz1", arguments=[], limit=None,
                destination=[1, 2, 3])
        self.assertIn("foobarbaz1", MockBroadcastPublisher.sent)

    @with_mock_broadcast
    def test_broadcast_validate(self):
        self.assertRaises(ValueError, control.broadcast, "foobarbaz2",
                          destination="foo")

    @with_mock_broadcast
    def test_rate_limit(self):
        control.rate_limit(PingTask.name, "100/m")
        self.assertIn("rate_limit", MockBroadcastPublisher.sent)

    @with_mock_broadcast
    def test_revoke(self):
        control.revoke("foozbaaz")
        self.assertIn("revoke", MockBroadcastPublisher.sent)

    @with_mock_broadcast
    def test_ping(self):
        control.ping()
        self.assertIn("ping", MockBroadcastPublisher.sent)

    @with_mock_broadcast
    def test_revoke_from_result(self):
        from celery.result import AsyncResult
        AsyncResult("foozbazzbar").revoke()
        self.assertIn("revoke", MockBroadcastPublisher.sent)

    @with_mock_broadcast
    def test_revoke_from_resultset(self):
        from celery.result import TaskSetResult, AsyncResult
        r = TaskSetResult(gen_unique_id(), map(AsyncResult, [gen_unique_id()
                                                        for i in range(10)]))
        r.revoke()
        self.assertIn("revoke", MockBroadcastPublisher.sent)
