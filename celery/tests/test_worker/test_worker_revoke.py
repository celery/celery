from celery.tests.utils import unittest

from celery.worker import state


class TestRevokeRegistry(unittest.TestCase):

    def test_is_working(self):
        state.revoked.add("foo")
        self.assertIn("foo", state.revoked)
        state.revoked.pop_value("foo")
        self.assertNotIn("foo", state.revoked)
