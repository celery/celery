import unittest
import logging
from datetime import datetime, timedelta

from celery import log
from celery import beat
from celery import conf
from celery.utils import gen_unique_id
from celery.task.base import PeriodicTask
from celery.registry import TaskRegistry
from celery.result import AsyncResult


class MockShelve(dict):
    closed = False
    synced = False

    def close(self):
        self.closed = True

    def sync(self):
        self.synced = True


class MockClockService(object):
    started = False
    stopped = False

    def __init__(self, *args, **kwargs):
        pass

    def start(self, **kwargs):
        self.started = True

    def stop(self, **kwargs):
        self.stopped = True


class DuePeriodicTask(PeriodicTask):
    run_every = timedelta(seconds=1)
    applied = False

    def is_due(self, *args, **kwargs):
        return True, 100

    @classmethod
    def apply_async(self, *args, **kwargs):
        self.applied = True
        return AsyncResult(gen_unique_id())


class DuePeriodicTaskRaising(PeriodicTask):
    run_every = timedelta(seconds=1)
    applied = False

    def is_due(self, *args, **kwargs):
        return True, 0

    @classmethod
    def apply_async(self, *args, **kwargs):
        raise Exception("FoozBaaz")


class PendingPeriodicTask(PeriodicTask):
    run_every = timedelta(seconds=1)
    applied = False

    def is_due(self, *args, **kwargs):
        return False, 100

    @classmethod
    def apply_async(self, *args, **kwargs):
        self.applied = True
        return AsyncResult(gen_unique_id())


class AdditionalTask(PeriodicTask):
    run_every = timedelta(days=7)

    @classmethod
    def apply_async(self, *args, **kwargs):
        raise Exception("FoozBaaz")


class TestScheduleEntry(unittest.TestCase):

    def test_constructor(self):
        s = beat.ScheduleEntry(DuePeriodicTask.name)
        self.assertEquals(s.name, DuePeriodicTask.name)
        self.assertTrue(isinstance(s.last_run_at, datetime))
        self.assertEquals(s.total_run_count, 0)

        now = datetime.now()
        s = beat.ScheduleEntry(DuePeriodicTask.name, now, 300)
        self.assertEquals(s.name, DuePeriodicTask.name)
        self.assertEquals(s.last_run_at, now)
        self.assertEquals(s.total_run_count, 300)

    def test_next(self):
        s = beat.ScheduleEntry(DuePeriodicTask.name, None, 300)
        n = s.next()
        self.assertEquals(n.name, s.name)
        self.assertEquals(n.total_run_count, 301)
        self.assertTrue(n.last_run_at > s.last_run_at)

    def test_is_due(self):
        due = beat.ScheduleEntry(DuePeriodicTask.name)
        pending = beat.ScheduleEntry(PendingPeriodicTask.name)

        self.assertTrue(due.is_due(DuePeriodicTask())[0])
        self.assertFalse(pending.is_due(PendingPeriodicTask())[0])


class TestScheduler(unittest.TestCase):

    def setUp(self):
        self.registry = TaskRegistry()
        self.registry.register(DuePeriodicTask)
        self.registry.register(PendingPeriodicTask)
        self.scheduler = beat.Scheduler(self.registry,
                                        max_interval=0.0001,
                                        logger=log.get_default_logger())

    def test_constructor(self):
        s = beat.Scheduler()
        self.assertTrue(isinstance(s.registry, TaskRegistry))
        self.assertTrue(isinstance(s.schedule, dict))
        self.assertTrue(isinstance(s.logger, logging.Logger))
        self.assertEquals(s.max_interval, conf.CELERYBEAT_MAX_LOOP_INTERVAL)

    def test_cleanup(self):
        self.scheduler.schedule["fbz"] = beat.ScheduleEntry("fbz")
        self.scheduler.cleanup()
        self.assertTrue("fbz" not in self.scheduler.schedule)

    def test_schedule_registry(self):
        self.registry.register(AdditionalTask)
        self.scheduler.schedule_registry()
        self.assertTrue(AdditionalTask.name in self.scheduler.schedule)

    def test_apply_async(self):
        due_task = self.registry[DuePeriodicTask.name]
        self.scheduler.apply_async(self.scheduler[due_task.name])
        self.assertTrue(due_task.applied)

    def test_apply_async_raises_SchedulingError_on_error(self):
        self.registry.register(AdditionalTask)
        self.scheduler.schedule_registry()
        add_task = self.registry[AdditionalTask.name]
        self.assertRaises(beat.SchedulingError,
                          self.scheduler.apply_async,
                          self.scheduler[add_task.name])

    def test_is_due(self):
        due = self.scheduler[DuePeriodicTask.name]
        pending = self.scheduler[PendingPeriodicTask.name]

        self.assertTrue(self.scheduler.is_due(due)[0])
        self.assertFalse(self.scheduler.is_due(pending)[0])

    def test_tick(self):
        self.scheduler.schedule.pop(DuePeriodicTaskRaising.name, None)
        self.registry.pop(DuePeriodicTaskRaising.name, None)
        self.assertEquals(self.scheduler.tick(),
                            self.scheduler.max_interval)

    def test_quick_schedulingerror(self):
        self.registry.register(DuePeriodicTaskRaising)
        self.scheduler.schedule_registry()
        self.assertEquals(self.scheduler.tick(),
                            self.scheduler.max_interval)


class TestClockService(unittest.TestCase):

    def test_start(self):
        s = beat.ClockService()
        sh = MockShelve()
        s.open_schedule = lambda *a, **kw: sh

        self.assertTrue(isinstance(s.schedule, dict))
        self.assertTrue(isinstance(s.schedule, dict))
        self.assertTrue(isinstance(s.scheduler, beat.Scheduler))
        self.assertTrue(isinstance(s.scheduler, beat.Scheduler))

        self.assertTrue(s.schedule is sh)
        self.assertTrue(s._schedule is sh)

        s._in_sync = False
        s.sync()
        self.assertTrue(sh.closed)
        self.assertTrue(sh.synced)
        self.assertTrue(s._stopped.isSet())
        s.sync()

        s.stop(wait=False)
        self.assertTrue(s._shutdown.isSet())
        s.stop(wait=True)
        self.assertTrue(s._shutdown.isSet())


class TestEmbeddedClockService(unittest.TestCase):

    def test_start_stop_process(self):
        s = beat.EmbeddedClockService()
        from multiprocessing import Process
        self.assertTrue(isinstance(s, Process))
        self.assertTrue(isinstance(s.clockservice, beat.ClockService))
        s.clockservice = MockClockService()

        s.run()
        self.assertTrue(s.clockservice.started)

        s.stop()
        self.assertTrue(s.clockservice.stopped)

    def test_start_stop_threaded(self):
        s = beat.EmbeddedClockService(thread=True)
        from threading import Thread
        self.assertTrue(isinstance(s, Thread))
        self.assertTrue(isinstance(s.clockservice, beat.ClockService))
        s.clockservice = MockClockService()

        s.run()
        self.assertTrue(s.clockservice.started)

        s.stop()
        self.assertTrue(s.clockservice.stopped)
