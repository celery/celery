from __future__ import absolute_import, unicode_literals

import errno
from datetime import datetime, timedelta
from pickle import dumps, loads

import pytest
import pytz

from case import Mock, call, patch, skip
from celery import __version__, beat, uuid
from celery.beat import BeatLazyFunc, event_t
from celery.five import keys, string_t
from celery.schedules import crontab, schedule
from celery.utils.objects import Bunch


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


class test_BeatLazyFunc:

    def test_beat_lazy_func(self):
        def add(a, b):
            return a + b
        result = BeatLazyFunc(add, 1, 2)
        assert add(1, 2) == result()
        assert add(1, 2) == result.delay()


class test_ScheduleEntry:
    Entry = beat.ScheduleEntry

    def create_entry(self, **kwargs):
        entry = {
            'name': 'celery.unittest.add',
            'schedule': timedelta(seconds=10),
            'args': (2, 2),
            'options': {'routing_key': 'cpu'},
            'app': self.app,
        }
        return self.Entry(**dict(entry, **kwargs))

    def test_next(self):
        entry = self.create_entry(schedule=10)
        assert entry.last_run_at
        assert isinstance(entry.last_run_at, datetime)
        assert entry.total_run_count == 0

        next_run_at = entry.last_run_at + timedelta(seconds=10)
        next_entry = entry.next(next_run_at)
        assert next_entry.last_run_at >= next_run_at
        assert next_entry.total_run_count == 1

    def test_is_due(self):
        entry = self.create_entry(schedule=timedelta(seconds=10))
        assert entry.app is self.app
        assert entry.schedule.app is self.app
        due1, next_time_to_run1 = entry.is_due()
        assert not due1
        assert next_time_to_run1 > 9

        next_run_at = entry.last_run_at - timedelta(seconds=10)
        next_entry = entry.next(next_run_at)
        due2, next_time_to_run2 = next_entry.is_due()
        assert due2
        assert next_time_to_run2 > 9

    def test_repr(self):
        entry = self.create_entry()
        assert '<ScheduleEntry:' in repr(entry)

    def test_reduce(self):
        entry = self.create_entry(schedule=timedelta(seconds=10))
        fun, args = entry.__reduce__()
        res = fun(*args)
        assert res.schedule == entry.schedule

    def test_lt(self):
        e1 = self.create_entry(schedule=timedelta(seconds=10))
        e2 = self.create_entry(schedule=timedelta(seconds=2))
        # order doesn't matter, see comment in __lt__
        res1 = e1 < e2  # noqa
        try:
            res2 = e1 < object()  # noqa
        except TypeError:
            pass

    def test_update(self):
        entry = self.create_entry()
        assert entry.schedule == timedelta(seconds=10)
        assert entry.args == (2, 2)
        assert entry.kwargs == {}
        assert entry.options == {'routing_key': 'cpu'}

        entry2 = self.create_entry(schedule=timedelta(minutes=20),
                                   args=(16, 16),
                                   kwargs={'callback': 'foo.bar.baz'},
                                   options={'routing_key': 'urgent'})
        entry.update(entry2)
        assert entry.schedule == schedule(timedelta(minutes=20))
        assert entry.args == (16, 16)
        assert entry.kwargs == {'callback': 'foo.bar.baz'}
        assert entry.options == {'routing_key': 'urgent'}


class mScheduler(beat.Scheduler):

    def __init__(self, *args, **kwargs):
        self.sent = []
        beat.Scheduler.__init__(self, *args, **kwargs)

    def send_task(self, name=None, args=None, kwargs=None, **options):
        self.sent.append({'name': name,
                          'args': args,
                          'kwargs': kwargs,
                          'options': options})
        return self.app.AsyncResult(uuid())


class mSchedulerSchedulingError(mScheduler):

    def send_task(self, *args, **kwargs):
        raise beat.SchedulingError('Could not apply task')


class mSchedulerRuntimeError(mScheduler):

    def is_due(self, *args, **kwargs):
        raise RuntimeError('dict modified while itervalues')


class mocked_schedule(schedule):

    def __init__(self, is_due, next_run_at, nowfun=datetime.utcnow):
        self._is_due = is_due
        self._next_run_at = next_run_at
        self.run_every = timedelta(seconds=1)
        self.nowfun = nowfun
        self.default_now = self.nowfun

    def is_due(self, last_run_at):
        return self._is_due, self._next_run_at


always_due = mocked_schedule(True, 1)
always_pending = mocked_schedule(False, 1)


class test_Scheduler:

    def test_custom_schedule_dict(self):
        custom = {'foo': 'bar'}
        scheduler = mScheduler(app=self.app, schedule=custom, lazy=True)
        assert scheduler.data is custom

    def test_apply_async_uses_registered_task_instances(self):

        @self.app.task(shared=False)
        def foo():
            pass
        foo.apply_async = Mock(name='foo.apply_async')
        assert foo.name in foo._get_app().tasks

        scheduler = mScheduler(app=self.app)
        scheduler.apply_async(scheduler.Entry(task=foo.name, app=self.app))
        foo.apply_async.assert_called()

    def test_apply_async_with_null_args(self):

        @self.app.task(shared=False)
        def foo():
            pass
        foo.apply_async = Mock(name='foo.apply_async')

        scheduler = mScheduler(app=self.app)
        scheduler.apply_async(scheduler.Entry(task=foo.name, app=self.app, args=None, kwargs=None))
        foo.apply_async.assert_called()

    def test_should_sync(self):

        @self.app.task(shared=False)
        def not_sync():
            pass
        not_sync.apply_async = Mock()

        s = mScheduler(app=self.app)
        s._do_sync = Mock()
        s.should_sync = Mock()
        s.should_sync.return_value = True
        s.apply_async(s.Entry(task=not_sync.name, app=self.app))
        s._do_sync.assert_called_with()

        s._do_sync = Mock()
        s.should_sync.return_value = False
        s.apply_async(s.Entry(task=not_sync.name, app=self.app))
        s._do_sync.assert_not_called()

    def test_should_sync_increments_sync_every_counter(self):
        self.app.conf.beat_sync_every = 2

        @self.app.task(shared=False)
        def not_sync():
            pass
        not_sync.apply_async = Mock()

        s = mScheduler(app=self.app)
        assert s.sync_every_tasks == 2
        s._do_sync = Mock()

        s.apply_async(s.Entry(task=not_sync.name, app=self.app))
        assert s._tasks_since_sync == 1
        s.apply_async(s.Entry(task=not_sync.name, app=self.app))
        s._do_sync.assert_called_with()

        self.app.conf.beat_sync_every = 0

    def test_sync_task_counter_resets_on_do_sync(self):
        self.app.conf.beat_sync_every = 1

        @self.app.task(shared=False)
        def not_sync():
            pass
        not_sync.apply_async = Mock()

        s = mScheduler(app=self.app)
        assert s.sync_every_tasks == 1

        s.apply_async(s.Entry(task=not_sync.name, app=self.app))
        assert s._tasks_since_sync == 0

        self.app.conf.beat_sync_every = 0

    @patch('celery.app.base.Celery.send_task')
    def test_send_task(self, send_task):
        b = beat.Scheduler(app=self.app)
        b.send_task('tasks.add', countdown=10)
        send_task.assert_called_with('tasks.add', countdown=10)

    def test_info(self):
        scheduler = mScheduler(app=self.app)
        assert isinstance(scheduler.info, string_t)

    def test_maybe_entry(self):
        s = mScheduler(app=self.app)
        entry = s.Entry(name='add every', task='tasks.add', app=self.app)
        assert s._maybe_entry(entry.name, entry) is entry
        assert s._maybe_entry('add every', {'task': 'tasks.add'})

    def test_set_schedule(self):
        s = mScheduler(app=self.app)
        s.schedule = {'foo': 'bar'}
        assert s.data == {'foo': 'bar'}

    @patch('kombu.connection.Connection.ensure_connection')
    def test_ensure_connection_error_handler(self, ensure):
        s = mScheduler(app=self.app)
        assert s._ensure_connected()
        ensure.assert_called()
        callback = ensure.call_args[0][0]

        callback(KeyError(), 5)

    def test_install_default_entries(self):
        self.app.conf.result_expires = None
        self.app.conf.beat_schedule = {}
        s = mScheduler(app=self.app)
        s.install_default_entries({})
        assert 'celery.backend_cleanup' not in s.data
        self.app.backend.supports_autoexpire = False

        self.app.conf.result_expires = 30
        s = mScheduler(app=self.app)
        s.install_default_entries({})
        assert 'celery.backend_cleanup' in s.data

        self.app.backend.supports_autoexpire = True
        self.app.conf.result_expires = 31
        s = mScheduler(app=self.app)
        s.install_default_entries({})
        assert 'celery.backend_cleanup' not in s.data

    def test_due_tick(self):
        scheduler = mScheduler(app=self.app)
        scheduler.add(name='test_due_tick',
                      schedule=always_due,
                      args=(1, 2),
                      kwargs={'foo': 'bar'})
        assert scheduler.tick() == 0

    @patch('celery.beat.error')
    def test_due_tick_SchedulingError(self, error):
        scheduler = mSchedulerSchedulingError(app=self.app)
        scheduler.add(name='test_due_tick_SchedulingError',
                      schedule=always_due)
        assert scheduler.tick() == 0
        error.assert_called()

    def test_pending_tick(self):
        scheduler = mScheduler(app=self.app)
        scheduler.add(name='test_pending_tick',
                      schedule=always_pending)
        assert scheduler.tick() == 1 - 0.010

    def test_honors_max_interval(self):
        scheduler = mScheduler(app=self.app)
        maxi = scheduler.max_interval
        scheduler.add(name='test_honors_max_interval',
                      schedule=mocked_schedule(False, maxi * 4))
        assert scheduler.tick() == maxi

    def test_ticks(self):
        scheduler = mScheduler(app=self.app)
        nums = [600, 300, 650, 120, 250, 36]
        s = {'test_ticks%s' % i: {'schedule': mocked_schedule(False, j)}
             for i, j in enumerate(nums)}
        scheduler.update_from_dict(s)
        assert scheduler.tick() == min(nums) - 0.010

    def test_ticks_microseconds(self):
        scheduler = mScheduler(app=self.app)

        now_ts = 1514797200.2
        now = datetime.utcfromtimestamp(now_ts)
        schedule_half = schedule(timedelta(seconds=0.5), nowfun=lambda: now)
        scheduler.add(name='half_second_schedule', schedule=schedule_half)

        scheduler.tick()
        # ensure those 0.2 seconds on now_ts don't get dropped
        expected_time = now_ts + 0.5 - 0.010
        assert scheduler._heap[0].time == expected_time

    def test_ticks_schedule_change(self):
        # initialise schedule and check heap is not initialized
        scheduler = mScheduler(app=self.app)
        assert scheduler._heap is None

        # set initial schedule and check heap is updated
        schedule_5 = schedule(5)
        scheduler.add(name='test_schedule', schedule=schedule_5)
        scheduler.tick()
        assert scheduler._heap[0].entry.schedule == schedule_5

        # update schedule and check heap is updated
        schedule_10 = schedule(10)
        scheduler.add(name='test_schedule', schedule=schedule(10))
        scheduler.tick()
        assert scheduler._heap[0].entry.schedule == schedule_10

    def test_schedule_no_remain(self):
        scheduler = mScheduler(app=self.app)
        scheduler.add(name='test_schedule_no_remain',
                      schedule=mocked_schedule(False, None))
        assert scheduler.tick() == scheduler.max_interval

    def test_interface(self):
        scheduler = mScheduler(app=self.app)
        scheduler.sync()
        scheduler.setup_schedule()
        scheduler.close()

    def test_merge_inplace(self):
        a = mScheduler(app=self.app)
        b = mScheduler(app=self.app)
        a.update_from_dict({'foo': {'schedule': mocked_schedule(True, 10)},
                            'bar': {'schedule': mocked_schedule(True, 20)}})
        b.update_from_dict({'bar': {'schedule': mocked_schedule(True, 40)},
                            'baz': {'schedule': mocked_schedule(True, 10)}})
        a.merge_inplace(b.schedule)

        assert 'foo' not in a.schedule
        assert 'baz' in a.schedule
        assert a.schedule['bar'].schedule._next_run_at == 40

    def test_when(self):
        now_time_utc = datetime(2000, 10, 10, 10, 10, 10, 10, tzinfo=pytz.utc)
        now_time_casey = now_time_utc.astimezone(
            pytz.timezone('Antarctica/Casey')
        )
        scheduler = mScheduler(app=self.app)
        result_utc = scheduler._when(
            mocked_schedule(True, 10, lambda: now_time_utc),
            10
        )
        result_casey = scheduler._when(
            mocked_schedule(True, 10, lambda: now_time_casey),
            10
        )
        assert result_utc == result_casey

    @patch('celery.beat.Scheduler._when', return_value=1)
    def test_populate_heap(self, _when):
        scheduler = mScheduler(app=self.app)
        scheduler.update_from_dict(
            {'foo': {'schedule': mocked_schedule(True, 10)}}
        )
        scheduler.populate_heap()
        assert scheduler._heap == [event_t(1, 5, scheduler.schedule['foo'])]

    def create_schedule_entry(self, schedule=None, args=(), kwargs={},
                              options={}, task=None):
        entry = {
            'name': 'celery.unittest.add',
            'schedule': schedule,
            'app': self.app,
            'args': args,
            'kwargs': kwargs,
            'options': options,
            'task': task
        }
        return beat.ScheduleEntry(**dict(entry))

    def test_schedule_equal_schedule_vs_schedule_success(self):
        scheduler = beat.Scheduler(app=self.app)
        a = {'a': self.create_schedule_entry(schedule=schedule(5))}
        b = {'a': self.create_schedule_entry(schedule=schedule(5))}
        assert scheduler.schedules_equal(a, b)

    def test_schedule_equal_schedule_vs_schedule_fail(self):
        scheduler = beat.Scheduler(app=self.app)
        a = {'a': self.create_schedule_entry(schedule=schedule(5))}
        b = {'a': self.create_schedule_entry(schedule=schedule(10))}
        assert not scheduler.schedules_equal(a, b)

    def test_schedule_equal_crontab_vs_crontab_success(self):
        scheduler = beat.Scheduler(app=self.app)
        a = {'a': self.create_schedule_entry(schedule=crontab(minute=5))}
        b = {'a': self.create_schedule_entry(schedule=crontab(minute=5))}
        assert scheduler.schedules_equal(a, b)

    def test_schedule_equal_crontab_vs_crontab_fail(self):
        scheduler = beat.Scheduler(app=self.app)
        a = {'a': self.create_schedule_entry(schedule=crontab(minute=5))}
        b = {'a': self.create_schedule_entry(schedule=crontab(minute=10))}
        assert not scheduler.schedules_equal(a, b)

    def test_schedule_equal_crontab_vs_schedule_fail(self):
        scheduler = beat.Scheduler(app=self.app)
        a = {'a': self.create_schedule_entry(schedule=crontab(minute=5))}
        b = {'a': self.create_schedule_entry(schedule=schedule(5))}
        assert not scheduler.schedules_equal(a, b)

    def test_schedule_equal_different_key_fail(self):
        scheduler = beat.Scheduler(app=self.app)
        a = {'a': self.create_schedule_entry(schedule=schedule(5))}
        b = {'b': self.create_schedule_entry(schedule=schedule(5))}
        assert not scheduler.schedules_equal(a, b)

    def test_schedule_equal_args_vs_args_success(self):
        scheduler = beat.Scheduler(app=self.app)
        a = {'a': self.create_schedule_entry(args='a')}
        b = {'a': self.create_schedule_entry(args='a')}
        assert scheduler.schedules_equal(a, b)

    def test_schedule_equal_args_vs_args_fail(self):
        scheduler = beat.Scheduler(app=self.app)
        a = {'a': self.create_schedule_entry(args='a')}
        b = {'a': self.create_schedule_entry(args='b')}
        assert not scheduler.schedules_equal(a, b)

    def test_schedule_equal_kwargs_vs_kwargs_success(self):
        scheduler = beat.Scheduler(app=self.app)
        a = {'a': self.create_schedule_entry(kwargs={'a': 'a'})}
        b = {'a': self.create_schedule_entry(kwargs={'a': 'a'})}
        assert scheduler.schedules_equal(a, b)

    def test_schedule_equal_kwargs_vs_kwargs_fail(self):
        scheduler = beat.Scheduler(app=self.app)
        a = {'a': self.create_schedule_entry(kwargs={'a': 'a'})}
        b = {'a': self.create_schedule_entry(kwargs={'b': 'b'})}
        assert not scheduler.schedules_equal(a, b)

    def test_schedule_equal_options_vs_options_success(self):
        scheduler = beat.Scheduler(app=self.app)
        a = {'a': self.create_schedule_entry(options={'a': 'a'})}
        b = {'a': self.create_schedule_entry(options={'a': 'a'})}
        assert scheduler.schedules_equal(a, b)

    def test_schedule_equal_options_vs_options_fail(self):
        scheduler = beat.Scheduler(app=self.app)
        a = {'a': self.create_schedule_entry(options={'a': 'a'})}
        b = {'a': self.create_schedule_entry(options={'b': 'b'})}
        assert not scheduler.schedules_equal(a, b)

    def test_schedule_equal_task_vs_task_success(self):
        scheduler = beat.Scheduler(app=self.app)
        a = {'a': self.create_schedule_entry(task='a')}
        b = {'a': self.create_schedule_entry(task='a')}
        assert scheduler.schedules_equal(a, b)

    def test_schedule_equal_task_vs_task_fail(self):
        scheduler = beat.Scheduler(app=self.app)
        a = {'a': self.create_schedule_entry(task='a')}
        b = {'a': self.create_schedule_entry(task='b')}
        assert not scheduler.schedules_equal(a, b)

    def test_schedule_equal_none_entry_vs_entry(self):
        scheduler = beat.Scheduler(app=self.app)
        a = None
        b = {'a': self.create_schedule_entry(task='b')}
        assert not scheduler.schedules_equal(a, b)

    def test_schedule_equal_entry_vs_none_entry(self):
        scheduler = beat.Scheduler(app=self.app)
        a = {'a': self.create_schedule_entry(task='a')}
        b = None
        assert not scheduler.schedules_equal(a, b)

    def test_schedule_equal_none_entry_vs_none_entry(self):
        scheduler = beat.Scheduler(app=self.app)
        a = None
        b = None
        assert scheduler.schedules_equal(a, b)


def create_persistent_scheduler(shelv=None):
    if shelv is None:
        shelv = MockShelve()

    class MockPersistentScheduler(beat.PersistentScheduler):
        sh = shelv
        persistence = Bunch(
            open=lambda *a, **kw: shelv,
        )
        tick_raises_exit = False
        shutdown_service = None

        def tick(self):
            if self.tick_raises_exit:
                raise SystemExit()
            if self.shutdown_service:
                self.shutdown_service._is_shutdown.set()
            return 0.0

    return MockPersistentScheduler, shelv


def create_persistent_scheduler_w_call_logging(shelv=None):
    if shelv is None:
        shelv = MockShelve()

    class MockPersistentScheduler(beat.PersistentScheduler):
        sh = shelv
        persistence = Bunch(
            open=lambda *a, **kw: shelv,
        )

        def __init__(self, *args, **kwargs):
            self.sent = []
            beat.PersistentScheduler.__init__(self, *args, **kwargs)

        def send_task(self, task=None, args=None, kwargs=None, **options):
            self.sent.append({'task': task,
                              'args': args,
                              'kwargs': kwargs,
                              'options': options})
            return self.app.AsyncResult(uuid())
    return MockPersistentScheduler, shelv


class test_PersistentScheduler:

    @patch('os.remove')
    def test_remove_db(self, remove):
        s = create_persistent_scheduler()[0](app=self.app,
                                             schedule_filename='schedule')
        s._remove_db()
        remove.assert_has_calls(
            [call('schedule' + suffix) for suffix in s.known_suffixes]
        )
        err = OSError()
        err.errno = errno.ENOENT
        remove.side_effect = err
        s._remove_db()
        err.errno = errno.EPERM
        with pytest.raises(OSError):
            s._remove_db()

    def test_setup_schedule(self):
        s = create_persistent_scheduler()[0](app=self.app,
                                             schedule_filename='schedule')
        opens = s.persistence.open = Mock()
        s._remove_db = Mock()

        def effect(*args, **kwargs):
            if opens.call_count > 1:
                return s.sh
            raise OSError()
        opens.side_effect = effect
        s.setup_schedule()
        s._remove_db.assert_called_with()

        s._store = {str('__version__'): 1}
        s.setup_schedule()

        s._store.clear = Mock()
        op = s.persistence.open = Mock()
        op.return_value = s._store
        s._store[str('tz')] = 'FUNKY'
        s.setup_schedule()
        op.assert_called_with(s.schedule_filename, writeback=True)
        s._store.clear.assert_called_with()
        s._store[str('utc_enabled')] = False
        s._store.clear = Mock()
        s.setup_schedule()
        s._store.clear.assert_called_with()

    def test_get_schedule(self):
        s = create_persistent_scheduler()[0](
            schedule_filename='schedule', app=self.app,
        )
        s._store = {str('entries'): {}}
        s.schedule = {'foo': 'bar'}
        assert s.schedule == {'foo': 'bar'}
        assert s._store[str('entries')] == s.schedule

    def test_run_all_due_tasks_after_restart(self):
        scheduler_class, shelve = create_persistent_scheduler_w_call_logging()

        shelve['tz'] = 'UTC'
        shelve['utc_enabled'] = True
        shelve['__version__'] = __version__
        cur_seconds = 20

        def now_func():
            return datetime(2018, 1, 1, 1, 11, cur_seconds)
        app_schedule = {
            'first_missed': {'schedule': crontab(
                minute='*/10', nowfun=now_func), 'task': 'first_missed'},
            'second_missed': {'schedule': crontab(
                minute='*/1', nowfun=now_func), 'task': 'second_missed'},
            'non_missed': {'schedule': crontab(
                minute='*/13', nowfun=now_func), 'task': 'non_missed'}
        }
        shelve['entries'] = {
            'first_missed': beat.ScheduleEntry(
                'first_missed', 'first_missed',
                last_run_at=now_func() - timedelta(minutes=2),
                total_run_count=10,
                schedule=app_schedule['first_missed']['schedule']),
            'second_missed': beat.ScheduleEntry(
                'second_missed', 'second_missed',
                last_run_at=now_func() - timedelta(minutes=2),
                total_run_count=10,
                schedule=app_schedule['second_missed']['schedule']),
            'non_missed': beat.ScheduleEntry(
                'non_missed', 'non_missed',
                last_run_at=now_func() - timedelta(minutes=2),
                total_run_count=10,
                schedule=app_schedule['non_missed']['schedule']),
        }

        self.app.conf.beat_schedule = app_schedule

        scheduler = scheduler_class(self.app)

        max_iter_number = 5
        for i in range(max_iter_number):
            delay = scheduler.tick()
            if delay > 0:
                break
        assert {'first_missed', 'second_missed'} == {
            item['task'] for item in scheduler.sent}
        # ensure next call on the beginning of next min
        assert abs(60 - cur_seconds - delay) < 1


class test_Service:

    def get_service(self):
        Scheduler, mock_shelve = create_persistent_scheduler()
        return beat.Service(app=self.app, scheduler_cls=Scheduler), mock_shelve

    def test_pickleable(self):
        s = beat.Service(app=self.app, scheduler_cls=Mock)
        assert loads(dumps(s))

    def test_start(self):
        s, sh = self.get_service()
        schedule = s.scheduler.schedule
        assert isinstance(schedule, dict)
        assert isinstance(s.scheduler, beat.Scheduler)
        scheduled = list(schedule.keys())
        for task_name in keys(sh[str('entries')]):
            assert task_name in scheduled

        s.sync()
        assert sh.closed
        assert sh.synced
        assert s._is_stopped.isSet()
        s.sync()
        s.stop(wait=False)
        assert s._is_shutdown.isSet()
        s.stop(wait=True)
        assert s._is_shutdown.isSet()

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
        assert s._is_shutdown.isSet()

    def test_start_manages_one_tick_before_shutdown(self):
        s, sh = self.get_service()
        s.scheduler.shutdown_service = s
        s.start()
        assert s._is_shutdown.isSet()


class test_EmbeddedService:

    @skip.unless_module('_multiprocessing', name='multiprocessing')
    def xxx_start_stop_process(self):
        from billiard.process import Process

        s = beat.EmbeddedService(self.app)
        assert isinstance(s, Process)
        assert isinstance(s.service, beat.Service)
        s.service = MockService()

        class _Popen(object):
            terminated = False

            def terminate(self):
                self.terminated = True

        with patch('celery.platforms.close_open_fds'):
            s.run()
        assert s.service.started

        s._popen = _Popen()
        s.stop()
        assert s.service.stopped
        assert s._popen.terminated

    def test_start_stop_threaded(self):
        s = beat.EmbeddedService(self.app, thread=True)
        from threading import Thread
        assert isinstance(s, Thread)
        assert isinstance(s.service, beat.Service)
        s.service = MockService()

        s.run()
        assert s.service.started

        s.stop()
        assert s.service.stopped


class test_schedule:

    def test_maybe_make_aware(self):
        x = schedule(10, app=self.app)
        x.utc_enabled = True
        d = x.maybe_make_aware(datetime.utcnow())
        assert d.tzinfo
        x.utc_enabled = False
        d2 = x.maybe_make_aware(datetime.utcnow())
        assert d2.tzinfo

    def test_to_local(self):
        x = schedule(10, app=self.app)
        x.utc_enabled = True
        d = x.to_local(datetime.utcnow())
        assert d.tzinfo is None
        x.utc_enabled = False
        d = x.to_local(datetime.utcnow())
        assert d.tzinfo
