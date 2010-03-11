import unittest2 as unittest

from celery.worker import revoke


class TestRevokeRegistry(unittest.TestCase):

    def test_is_working(self):
        revoke.revoked.add("foo")
        self.assertIn("foo", revoke.revoked)
        revoke.revoked.pop_value("foo")
        self.assertNotIn("foo", revoke.revoked)
