from __future__ import absolute_import
from __future__ import with_statement

import errno

from datetime import datetime, timedelta
from mock import Mock, call, patch
from nose import SkipTest

from celery import current_app
from celery import beat
from celery import task
from celery.result import AsyncResult
from celery.schedules import schedule
from celery.task.base import Task
from celery.utils import uuid
from celery.tests.utils import Case, patch_settings


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
        entry = dict(name='celery.unittest.add',
                     schedule=schedule(timedelta(seconds=10)),
                     args=(2, 2),
                     options={'routing_key': 'cpu'})
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
        self.assertIn('<Entry:', repr(entry))

    def test_update(self):
        entry = self.create_entry()
        self.assertEqual(entry.schedule, timedelta(seconds=10))
        self.assertTupleEqual(entry.args, (2, 2))
        self.assertDictEqual(entry.kwargs, {})
        self.assertDictEqual(entry.options, {'routing_key': 'cpu'})

        entry2 = self.create_entry(schedule=timedelta(minutes=20),
                                   args=(16, 16),
                                   kwargs={'callback': 'foo.bar.baz'},
                                   options={'routing_key': 'urgent'})
        entry.update(entry2)
        self.assertEqual(entry.schedule, schedule(timedelta(minutes=20)))
        self.assertTupleEqual(entry.args, (16, 16))
        self.assertDictEqual(entry.kwargs, {'callback': 'foo.bar.baz'})
        self.assertDictEqual(entry.options, {'routing_key': 'urgent'})


class mScheduler(beat.Scheduler):

    def __init__(self, *args, **kwargs):
        self.sent = []
        beat.Scheduler.__init__(self, *args, **kwargs)

    def send_task(self, name=None, args=None, kwargs=None, **options):
        self.sent.append({'name': name,
                          'args': args,
                          'kwargs': kwargs,
                          'options': options})
        return AsyncResult(uuid())


class mSchedulerSchedulingError(mScheduler):

    def send_task(self, *args, **kwargs):
        raise beat.SchedulingError('Could not apply task')


class mSchedulerRuntimeError(mScheduler):

    def maybe_due(self, *args, **kwargs):
        raise RuntimeError('dict modified while itervalues')


class mocked_schedule(schedule):

    def __init__(self, is_due, next_run_at):
        self._is_due = is_due
        self._next_run_at = next_run_at
        self.run_every = timedelta(seconds=1)
        self.nowfun = datetime.utcnow

    def is_due(self, last_run_at):
        return self._is_due, self._next_run_at


always_due = mocked_schedule(True, 1)
always_pending = mocked_schedule(False, 1)


class test_Scheduler(Case):

    def test_custom_schedule_dict(self):
        custom = {'foo': 'bar'}
        scheduler = mScheduler(schedule=custom, lazy=True)
        self.assertIs(scheduler.data, custom)

    def test_apply_async_uses_registered_task_instances(self):
        through_task = [False]

        class MockTask(Task):

            @classmethod
            def apply_async(cls, *args, **kwargs):
                through_task[0] = True

        assert MockTask.name in MockTask._get_app().tasks

        scheduler = mScheduler()
        scheduler.apply_async(scheduler.Entry(task=MockTask.name))
        self.assertTrue(through_task[0])

    def test_apply_async_should_not_sync(self):

        @task()
        def not_sync():
            pass
        not_sync.apply_async = Mock()

        s = mScheduler()
        s._do_sync = Mock()
        s.should_sync = Mock()
        s.should_sync.return_value = True
        s.apply_async(s.Entry(task=not_sync.name))
        s._do_sync.assert_called_with()

        s._do_sync = Mock()
        s.should_sync.return_value = False
        s.apply_async(s.Entry(task=not_sync.name))
        self.assertFalse(s._do_sync.called)

    @patch('celery.app.base.Celery.send_task')
    def test_send_task(self, send_task):
        b = beat.Scheduler()
        b.send_task('tasks.add', countdown=10)
        send_task.assert_called_with('tasks.add', countdown=10)

    def test_info(self):
        scheduler = mScheduler()
        self.assertIsInstance(scheduler.info, basestring)

    def test_maybe_entry(self):
        s = mScheduler()
        entry = s.Entry(name='add every', task='tasks.add')
        self.assertIs(s._maybe_entry(entry.name, entry), entry)
        self.assertTrue(s._maybe_entry('add every', {
            'task': 'tasks.add',
        }))

    def test_set_schedule(self):
        s = mScheduler()
        s.schedule = {'foo': 'bar'}
        self.assertEqual(s.data, {'foo': 'bar'})

    @patch('kombu.connection.Connection.ensure_connection')
    def test_ensure_connection_error_handler(self, ensure):
        s = mScheduler()
        self.assertTrue(s._ensure_connected())
        self.assertTrue(ensure.called)
        callback = ensure.call_args[0][0]

        callback(KeyError(), 5)

    def test_install_default_entries(self):
        with patch_settings(CELERY_TASK_RESULT_EXPIRES=None,
                            CELERYBEAT_SCHEDULE={}):
            s = mScheduler()
            s.install_default_entries({})
            self.assertNotIn('celery.backend_cleanup', s.data)
        current_app.backend.supports_autoexpire = False
        with patch_settings(CELERY_TASK_RESULT_EXPIRES=30,
                            CELERYBEAT_SCHEDULE={}):
            s = mScheduler()
            s.install_default_entries({})
            self.assertIn('celery.backend_cleanup', s.data)
        current_app.backend.supports_autoexpire = True
        try:
            with patch_settings(CELERY_TASK_RESULT_EXPIRES=31,
                                CELERYBEAT_SCHEDULE={}):
                s = mScheduler()
                s.install_default_entries({})
                self.assertNotIn('celery.backend_cleanup', s.data)
        finally:
            current_app.backend.supports_autoexpire = False

    def test_due_tick(self):
        scheduler = mScheduler()
        scheduler.add(name='test_due_tick',
                      schedule=always_due,
                      args=(1, 2),
                      kwargs={'foo': 'bar'})
        self.assertEqual(scheduler.tick(), 1)

    @patch('celery.beat.error')
    def test_due_tick_SchedulingError(self, error):
        scheduler = mSchedulerSchedulingError()
        scheduler.add(name='test_due_tick_SchedulingError',
                      schedule=always_due)
        self.assertEqual(scheduler.tick(), 1)
        self.assertTrue(error.called)

    def test_due_tick_RuntimeError(self):
        scheduler = mSchedulerRuntimeError()
        scheduler.add(name='test_due_tick_RuntimeError',
                      schedule=always_due)
        self.assertEqual(scheduler.tick(), scheduler.max_interval)

    def test_pending_tick(self):
        scheduler = mScheduler()
        scheduler.add(name='test_pending_tick',
                      schedule=always_pending)
        self.assertEqual(scheduler.tick(), 1)

    def test_honors_max_interval(self):
        scheduler = mScheduler()
        maxi = scheduler.max_interval
        scheduler.add(name='test_honors_max_interval',
                      schedule=mocked_schedule(False, maxi * 4))
        self.assertEqual(scheduler.tick(), maxi)

    def test_ticks(self):
        scheduler = mScheduler()
        nums = [600, 300, 650, 120, 250, 36]
        s = dict(('test_ticks%s' % i,
                 {'schedule': mocked_schedule(False, j)})
                 for i, j in enumerate(nums))
        scheduler.update_from_dict(s)
        self.assertEqual(scheduler.tick(), min(nums))

    def test_schedule_no_remain(self):
        scheduler = mScheduler()
        scheduler.add(name='test_schedule_no_remain',
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
        a.update_from_dict({'foo': {'schedule': mocked_schedule(True, 10)},
                            'bar': {'schedule': mocked_schedule(True, 20)}})
        b.update_from_dict({'bar': {'schedule': mocked_schedule(True, 40)},
                            'baz': {'schedule': mocked_schedule(True, 10)}})
        a.merge_inplace(b.schedule)

        self.assertNotIn('foo', a.schedule)
        self.assertIn('baz', a.schedule)
        self.assertEqual(a.schedule['bar'].schedule._next_run_at, 40)


def create_persistent_scheduler(shelv=None):
    if shelv is None:
        shelv = MockShelve()

    class MockPersistentScheduler(beat.PersistentScheduler):
        sh = shelv
        persistence = Object()
        persistence.open = lambda *a, **kw: shelv
        tick_raises_exit = False
        shutdown_service = None

        def tick(self):
            if self.tick_raises_exit:
                raise SystemExit()
            if self.shutdown_service:
                self.shutdown_service._is_shutdown.set()
            return 0.0

    return MockPersistentScheduler, shelv


class test_PersistentScheduler(Case):

    @patch('os.remove')
    def test_remove_db(self, remove):
        s = create_persistent_scheduler()[0](schedule_filename='schedule')
        s._remove_db()
        remove.assert_has_calls(
            [call('schedule' + suffix) for suffix in s.known_suffixes]
        )
        err = OSError()
        err.errno = errno.ENOENT
        remove.side_effect = err
        s._remove_db()
        err.errno = errno.EPERM
        with self.assertRaises(OSError):
            s._remove_db()

    def test_setup_schedule(self):
        s = create_persistent_scheduler()[0](schedule_filename='schedule')
        opens = s.persistence.open = Mock()
        s._remove_db = Mock()

        def effect(*args, **kwargs):
            if opens.call_count > 1:
                return s.sh
            raise OSError()
        opens.side_effect = effect
        s.setup_schedule()
        s._remove_db.assert_called_with()

        s._store = {'__version__': 1}
        s.setup_schedule()

    def test_get_schedule(self):
        s = create_persistent_scheduler()[0](schedule_filename='schedule')
        s._store = {'entries': {}}
        s.schedule = {'foo': 'bar'}
        self.assertDictEqual(s.schedule, {'foo': 'bar'})
        self.assertDictEqual(s._store['entries'], s.schedule)


class test_Service(Case):

    def get_service(self):
        Scheduler, mock_shelve = create_persistent_scheduler()
        return beat.Service(scheduler_cls=Scheduler), mock_shelve

    def test_start(self):
        s, sh = self.get_service()
        schedule = s.scheduler.schedule
        self.assertIsInstance(schedule, dict)
        self.assertIsInstance(s.scheduler, beat.Scheduler)
        scheduled = schedule.keys()
        for task_name in sh['entries'].keys():
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
            import _multiprocessing  # noqa
        except ImportError:
            raise SkipTest('multiprocessing not available')

        from billiard.process import Process

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
