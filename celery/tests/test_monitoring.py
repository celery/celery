import unittest
import time
from celery.monitoring import TaskTimerStats, Statistics, StatsCollector


class PartialStatistics(Statistics):
    type = "c.u.partial"


class TestStatisticsInterface(unittest.TestCase):

    def test_must_have_type(self):
        self.assertRaises(NotImplementedError, Statistics)

    def test_must_have_on_start(self):
        self.assertRaises(NotImplementedError, PartialStatistics().on_start)

    def test_must_have_on_stop(self):
        self.assertRaises(NotImplementedError, PartialStatistics().on_stop)


class TestTaskTimerStats(unittest.TestCase):

    def test_time(self):
        self.assertTimeElapsed(0.5, 1, 0, "0.5")
        self.assertTimeElapsed(0.002, 0.05, 0, "0.0")
        self.assertTimeElapsed(0.1, 0.5, 0, "0.1")

    def test_not_enabled(self):
        t = TaskTimerStats()
        t.enabled = False
        self.assertFalse(t.publish(isnot="enabled"))
        self.assertFalse(getattr(t, "time_start", None))
        t.run("foo", "bar", [], {})
        t.stop()

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


class TestStatsCollector(unittest.TestCase):

    def test_attrs(self):
        s = StatsCollector()
        self.assertEquals(s.total_tasks_processed, 0)
        self.assertEquals(s.total_tasks_processed_by_type, {})
        self.assertEquals(s.total_task_time_running, 0.0)
        self.assertEquals(s.total_task_time_running_by_type, {})

    def test_collect(self):
        s = StatsCollector()
        s.collect()
        self.assertEquals(s.total_tasks_processed, 0)
