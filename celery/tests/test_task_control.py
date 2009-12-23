import unittest

from celery.task import control
from celery.task.builtins import PingTask
from celery.utils import gen_unique_id


class MockBroadcastPublisher(object):
    sent = []

    def __init__(self, *args, **kwargs):
        pass

    def send(self, command, *args, **kwargs):
        self.__class__.sent.append(command)

    def close(self):
        pass


def with_mock_broadcast(fun):

    def _mocked(*args, **kwargs):
        old_pub = control.BroadcastPublisher
        control.BroadcastPublisher = MockBroadcastPublisher
        try:
            return fun(*args, **kwargs)
        finally:
            MockBroadcastPublisher.sent = []
            control.BroadcastPublisher = old_pub
    return _mocked


class TestBroadcast(unittest.TestCase):

    @with_mock_broadcast
    def test_broadcast(self):
        control.broadcast("foobarbaz", arguments=[])
        self.assertTrue("foobarbaz" in MockBroadcastPublisher.sent)

    @with_mock_broadcast
    def test_rate_limit(self):
        control.rate_limit(PingTask.name, "100/m")
        self.assertTrue("rate_limit" in MockBroadcastPublisher.sent)

    @with_mock_broadcast
    def test_revoke(self):
        control.revoke("foozbaaz")
        self.assertTrue("revoke" in MockBroadcastPublisher.sent)

    @with_mock_broadcast
    def test_revoke_from_result(self):
        from celery.result import AsyncResult
        AsyncResult("foozbazzbar").revoke()
        self.assertTrue("revoke" in MockBroadcastPublisher.sent)

    @with_mock_broadcast
    def test_revoke_from_resultset(self):
        from celery.result import TaskSetResult, AsyncResult
        r = TaskSetResult(gen_unique_id(), map(AsyncResult, [gen_unique_id()
                                                            for i in range(10)]))
        r.revoke()
        self.assertTrue("revoke" in MockBroadcastPublisher.sent)
