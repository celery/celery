from __future__ import absolute_import

from celery.worker import state
from celery.tests.utils import Case


class test_revoked(Case):

    def test_is_working(self):
        state.revoked.add("foo")
        self.assertIn("foo", state.revoked)
        state.revoked.pop_value("foo")
        self.assertNotIn("foo", state.revoked)
