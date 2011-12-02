from __future__ import absolute_import

from celery.worker import state
from celery.tests.utils import unittest


class test_revoked(unittest.TestCase):

    def test_is_working(self):
        state.revoked.add("foo")
        self.assertIn("foo", state.revoked)
        state.revoked.pop_value("foo")
        self.assertNotIn("foo", state.revoked)
