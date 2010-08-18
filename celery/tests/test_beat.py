import logging
import unittest2 as unittest
from datetime import datetime, timedelta

from celery import log
from celery import beat
from celery import conf
from celery.task.base import Task
from celery.schedules import schedule
from celery.utils import gen_unique_id
from celery.task.base import PeriodicTask
from celery.registry import TaskRegistry
from celery.result import AsyncResult


class Object(object):
    pass


class MockShelve(dict):
    closed = False
    synced = False

    def close(self):
        self.closed = True

    def sync(self):
        self.synced = True


class MockService(object):
    started = False
    stopped = False

    def __init__(self, *args, **kwargs):
        pass

    def start(self, **kwargs):
        self.started = True

    def stop(self, **kwargs):
        self.stopped = True



class test_Service(unittest.TestCase):

    def test_start(self):
        sh = MockShelve()

        class PersistentScheduler(beat.PersistentScheduler):
            persistence = Object()
            persistence.open = lambda *a, **kw: sh

        s = beat.Service(scheduler_cls=PersistentScheduler)
        self.assertIsInstance(s.schedule, dict)
        self.assertIsInstance(s.scheduler, beat.Scheduler)
        self.assertListEqual(s.schedule.keys(), sh.keys())

        s.sync()
        self.assertTrue(sh.closed)
        self.assertTrue(sh.synced)
        self.assertTrue(s._stopped.isSet())
        s.sync()
        s.stop(wait=False)
        self.assertTrue(s._shutdown.isSet())
        s.stop(wait=True)
        self.assertTrue(s._shutdown.isSet())


class TestEmbeddedService(unittest.TestCase):

    def test_start_stop_process(self):
        s = beat.EmbeddedService()
        from multiprocessing import Process
        self.assertIsInstance(s, Process)
        self.assertIsInstance(s.service, beat.Service)
        s.service = MockService()

        class _Popen(object):
            terminated = False

            def terminate(self):
                self.terminated = True

        s.run()
        self.assertTrue(s.service.started)

        s._popen = _Popen()
        s.stop()
        self.assertTrue(s.service.stopped)
        self.assertTrue(s._popen.terminated)

    def test_start_stop_threaded(self):
        s = beat.EmbeddedService(thread=True)
        from threading import Thread
        self.assertIsInstance(s, Thread)
        self.assertIsInstance(s.service, beat.Service)
        s.service = MockService()

        s.run()
        self.assertTrue(s.service.started)

        s.stop()
        self.assertTrue(s.service.stopped)
