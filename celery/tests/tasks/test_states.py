from __future__ import absolute_import, unicode_literals

from celery import states

from celery.tests.case import Case


class test_state_precedence(Case):

    def test_gt(self):
        self.assertGreater(
            states.state(states.SUCCESS), states.state(states.PENDING),
        )
        self.assertGreater(
            states.state(states.FAILURE), states.state(states.RECEIVED),
        )
        self.assertGreater(
            states.state(states.REVOKED), states.state(states.STARTED),
        )
        self.assertGreater(
            states.state(states.SUCCESS), states.state('CRASHED'),
        )
        self.assertGreater(
            states.state(states.FAILURE), states.state('CRASHED'),
        )
        self.assertLessEqual(
            states.state(states.REVOKED), states.state('CRASHED'),
        )

    def test_lt(self):
        self.assertLess(
            states.state(states.PENDING), states.state(states.SUCCESS),
        )
        self.assertLess(
            states.state(states.RECEIVED), states.state(states.FAILURE),
        )
        self.assertLess(
            states.state(states.STARTED), states.state(states.REVOKED),
        )
        self.assertLess(
            states.state('CRASHED'), states.state(states.SUCCESS),
        )
        self.assertLess(
            states.state('CRASHED'), states.state(states.FAILURE),
        )
        self.assertLess(
            states.state(states.REVOKED), states.state('CRASHED'),
        )
        self.assertLessEqual(
            states.state(states.REVOKED), states.state('CRASHED'),
        )
        self.assertGreaterEqual(
            states.state('CRASHED'), states.state(states.REVOKED),
        )
