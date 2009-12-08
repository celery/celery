import unittest
from Queue import Queue, Empty
from datetime import datetime, timedelta

from celery.worker import revoke


class TestRevokeRegistry(unittest.TestCase):

    def test_is_working(self):
        revoke.revoked.add("foo")
        self.assertTrue("foo" in revoke.revoked)
        revoke.revoked.pop_value("foo")
        self.assertTrue("foo" not in revoke.revoked)
