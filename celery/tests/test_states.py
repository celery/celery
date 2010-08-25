import unittest2 as unittest

from celery import states


class test_state_precedence(unittest.TestCase):

    def test_gt(self):
        self.assertGreater(states.SUCCESS, states.PENDING)
        self.assertGreater(states.FAILURE, states.RECEIVED)
        self.assertGreater(states.REVOKED, states.STARTED)
        self.assertGreater(states.SUCCESS, states.state("CRASHED"))
        self.assertGreater(states.FAILURE, states.state("CRASHED"))
        self.assertFalse(states.REVOKED > states.state("CRASHED"))

    def test_lt(self):
        self.assertLess(states.PENDING, states.SUCCESS)
        self.assertLess(states.RECEIVED, states.FAILURE)
        self.assertLess(states.STARTED, states.REVOKED)
        self.assertLess(states.state("CRASHED"), states.SUCCESS)
        self.assertLess(states.state("CRASHED"), states.FAILURE)
        self.assertTrue(states.REVOKED < states.state("CRASHED"))

