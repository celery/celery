import unittest
import time
from celery.monitoring import TaskTimerStats


class TestTaskTimerStats(unittest.TestCase):

    def test_time(self):
        self.assertTimeElapsed(0.5, 1, 0, "0.5")
        self.assertTimeElapsed(0.002, 0.05, 0, "0.0")
        self.assertTimeElapsed(0.1, 0.5, 0, "0.1")

    def assertTimeElapsed(self, time_sleep, max_appx, min_appx, appx):
        t = TaskTimerStats()
        t.enabled = True
        t.run("foo", "bar", [], {})
        self.assertTrue(t.time_start)
        time.sleep(time_sleep)
        time_stop = t.stop()
        self.assertTrue(time_stop)
        self.assertFalse(time_stop > max_appx)
        self.assertFalse(time_stop <= min_appx)

        strstop = str(time_stop)[0:3]
        # Time elapsed is approximately 0.1 seconds.
        self.assertTrue(strstop == appx)
