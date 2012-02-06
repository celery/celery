from __future__ import absolute_import

import logging

from datetime import datetime, timedelta
from nose import SkipTest

from celery import beat
from celery import registry
from celery.result import AsyncResult
from celery.schedules import schedule
from celery.task.base import Task
from celery.utils import uuid
from celery.tests.utils import Case


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


class test_ScheduleEntry(Case):
    Entry = beat.ScheduleEntry

    def create_entry(self, **kwargs):
        entry = dict(name="celery.unittest.add",
                     schedule=schedule(timedelta(seconds=10)),
                     args=(2, 2),
                     options={"routing_key": "cpu"})
        return self.Entry(**dict(entry, **kwargs))

    def test_next(self):
        entry = self.create_entry(schedule=10)
        self.assertTrue(entry.last_run_at)
        self.assertIsInstance(entry.last_run_at, datetime)
        self.assertEqual(entry.total_run_count, 0)

        next_run_at = entry.last_run_at + timedelta(seconds=10)
        next = entry.next(next_run_at)
        self.assertGreaterEqual(next.last_run_at, next_run_at)
        self.assertEqual(next.total_run_count, 1)

    def test_is_due(self):
        entry = self.create_entry(schedule=timedelta(seconds=10))
        due1, next_time_to_run1 = entry.is_due()
        self.assertFalse(due1)
        self.assertGreater(next_time_to_run1, 9)

        next_run_at = entry.last_run_at - timedelta(seconds=10)
        next = entry.next(next_run_at)
        due2, next_time_to_run2 = next.is_due()
        self.assertTrue(due2)
        self.assertGreater(next_time_to_run2, 9)

    def test_repr(self):
        entry = self.create_entry()
        self.assertIn("<Entry:", repr(entry))

    def test_update(self):
        entry = self.create_entry()
        self.assertEqual(entry.schedule, timedelta(seconds=10))
        self.assertTupleEqual(entry.args, (2, 2))
        self.assertDictEqual(entry.kwargs, {})
        self.assertDictEqual(entry.options, {"routing_key": "cpu"})

        entry2 = self.create_entry(schedule=timedelta(minutes=20),
                                   args=(16, 16),
                                   kwargs={"callback": "foo.bar.baz"},
                                   options={"routing_key": "urgent"})
        entry.update(entry2)
        self.assertEqual(entry.schedule, schedule(timedelta(minutes=20)))
        self.assertTupleEqual(entry.args, (16, 16))
        self.assertDictEqual(entry.kwargs, {"callback": "foo.bar.baz"})
        self.assertDictEqual(entry.options, {"routing_key": "urgent"})


class MockLogger(logging.Logger):

    def __init__(self, *args, **kwargs):
        self.logged = []
        logging.Logger.__init__(self, *args, **kwargs)

    def _log(self, level, msg, args, **kwargs):
        self.logged.append((level, msg, args, kwargs))


class mScheduler(beat.Scheduler):

    def __init__(self, *args, **kwargs):
        self.sent = []
        beat.Scheduler.__init__(self, *args, **kwargs)
        self.logger = MockLogger("celery.beat", logging.ERROR)

    def send_task(self, name=None, args=None, kwargs=None, **options):
        self.sent.append({"name": name,
                          "args": args,
                          "kwargs": kwargs,
                          "options": options})
        return AsyncResult(uuid())


class mSchedulerSchedulingError(mScheduler):

    def send_task(self, *args, **kwargs):
        raise beat.SchedulingError("Could not apply task")


class mSchedulerRuntimeError(mScheduler):

    def maybe_due(self, *args, **kwargs):
        raise RuntimeError("dict modified while itervalues")


class mocked_schedule(schedule):

    def __init__(self, is_due, next_run_at):
        self._is_due = is_due
        self._next_run_at = next_run_at
        self.run_every = timedelta(seconds=1)

    def is_due(self, last_run_at):
        return self._is_due, self._next_run_at


always_due = mocked_schedule(True, 1)
always_pending = mocked_schedule(False, 1)


class test_Scheduler(Case):

    def test_custom_schedule_dict(self):
        custom = {"foo": "bar"}
        scheduler = mScheduler(schedule=custom, lazy=True)
        self.assertIs(scheduler.data, custom)

    def test_apply_async_uses_registered_task_instances(self):
        through_task = [False]

        class MockTask(Task):

            @classmethod
            def apply_async(cls, *args, **kwargs):
                through_task[0] = True

        assert MockTask.name in registry.tasks

        scheduler = mScheduler()
        scheduler.apply_async(scheduler.Entry(task=MockTask.name))
        self.assertTrue(through_task[0])

    def test_info(self):
        scheduler = mScheduler()
        self.assertIsInstance(scheduler.info, basestring)

    def test_due_tick(self):
        scheduler = mScheduler()
        scheduler.add(name="test_due_tick",
                      schedule=always_due,
                      args=(1, 2),
                      kwargs={"foo": "bar"})
        self.assertEqual(scheduler.tick(), 1)

    def test_due_tick_SchedulingError(self):
        scheduler = mSchedulerSchedulingError()
        scheduler.add(name="test_due_tick_SchedulingError",
                      schedule=always_due)
        self.assertEqual(scheduler.tick(), 1)
        self.assertTrue(scheduler.logger.logged[0])
        level, msg, args, kwargs = scheduler.logger.logged[0]
        self.assertEqual(level, logging.ERROR)
        self.assertIn("Couldn't apply scheduled task",
                      repr(args[0].args[0]))

    def test_due_tick_RuntimeError(self):
        scheduler = mSchedulerRuntimeError()
        scheduler.add(name="test_due_tick_RuntimeError",
                      schedule=always_due)
        self.assertEqual(scheduler.tick(), scheduler.max_interval)

    def test_pending_tick(self):
        scheduler = mScheduler()
        scheduler.add(name="test_pending_tick",
                      schedule=always_pending)
        self.assertEqual(scheduler.tick(), 1)

    def test_honors_max_interval(self):
        scheduler = mScheduler()
        maxi = scheduler.max_interval
        scheduler.add(name="test_honors_max_interval",
                      schedule=mocked_schedule(False, maxi * 4))
        self.assertEqual(scheduler.tick(), maxi)

    def test_ticks(self):
        scheduler = mScheduler()
        nums = [600, 300, 650, 120, 250, 36]
        s = dict(("test_ticks%s" % i,
                 {"schedule": mocked_schedule(False, j)})
                    for i, j in enumerate(nums))
        scheduler.update_from_dict(s)
        self.assertEqual(scheduler.tick(), min(nums))

    def test_schedule_no_remain(self):
        scheduler = mScheduler()
        scheduler.add(name="test_schedule_no_remain",
                      schedule=mocked_schedule(False, None))
        self.assertEqual(scheduler.tick(), scheduler.max_interval)

    def test_interface(self):
        scheduler = mScheduler()
        scheduler.sync()
        scheduler.setup_schedule()
        scheduler.close()

    def test_merge_inplace(self):
        a = mScheduler()
        b = mScheduler()
        a.update_from_dict({"foo": {"schedule": mocked_schedule(True, 10)},
                            "bar": {"schedule": mocked_schedule(True, 20)}})
        b.update_from_dict({"bar": {"schedule": mocked_schedule(True, 40)},
                            "baz": {"schedule": mocked_schedule(True, 10)}})
        a.merge_inplace(b.schedule)

        self.assertNotIn("foo", a.schedule)
        self.assertIn("baz", a.schedule)
        self.assertEqual(a.schedule["bar"].schedule._next_run_at, 40)


class test_Service(Case):

    def get_service(self):
        sh = MockShelve()

        class PersistentScheduler(beat.PersistentScheduler):
            persistence = Object()
            persistence.open = lambda *a, **kw: sh
            tick_raises_exit = False
            shutdown_service = None

            def tick(self):
                if self.tick_raises_exit:
                    raise SystemExit()
                if self.shutdown_service:
                    self.shutdown_service._is_shutdown.set()
                return 0.0

        return beat.Service(scheduler_cls=PersistentScheduler), sh

    def test_start(self):
        s, sh = self.get_service()
        schedule = s.scheduler.schedule
        self.assertIsInstance(schedule, dict)
        self.assertIsInstance(s.scheduler, beat.Scheduler)
        scheduled = schedule.keys()
        for task_name in sh["entries"].keys():
            self.assertIn(task_name, scheduled)

        s.sync()
        self.assertTrue(sh.closed)
        self.assertTrue(sh.synced)
        self.assertTrue(s._is_stopped.isSet())
        s.sync()
        s.stop(wait=False)
        self.assertTrue(s._is_shutdown.isSet())
        s.stop(wait=True)
        self.assertTrue(s._is_shutdown.isSet())

        p = s.scheduler._store
        s.scheduler._store = None
        try:
            s.scheduler.sync()
        finally:
            s.scheduler._store = p

    def test_start_embedded_process(self):
        s, sh = self.get_service()
        s._is_shutdown.set()
        s.start(embedded_process=True)

    def test_start_thread(self):
        s, sh = self.get_service()
        s._is_shutdown.set()
        s.start(embedded_process=False)

    def test_start_tick_raises_exit_error(self):
        s, sh = self.get_service()
        s.scheduler.tick_raises_exit = True
        s.start()
        self.assertTrue(s._is_shutdown.isSet())

    def test_start_manages_one_tick_before_shutdown(self):
        s, sh = self.get_service()
        s.scheduler.shutdown_service = s
        s.start()
        self.assertTrue(s._is_shutdown.isSet())


class test_EmbeddedService(Case):

    def test_start_stop_process(self):
        try:
            from multiprocessing import Process
        except ImportError:
            raise SkipTest("multiprocessing not available")

        s = beat.EmbeddedService()
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
