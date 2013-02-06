from __future__ import absolute_import
from __future__ import with_statement

from datetime import datetime, timedelta
from functools import wraps
from mock import patch
from pickle import loads, dumps

from celery.task import (
    current,
    task,
    Task,
    BaseTask,
    TaskSet,
    periodic_task,
    PeriodicTask
)
from celery import current_app
from celery.app import app_or_default
from celery.exceptions import RetryTaskError
from celery.execute import send_task
from celery.result import EagerResult
from celery.schedules import crontab, crontab_parser, ParseException
from celery.utils import uuid
from celery.utils.timeutils import parse_iso8601, timedelta_seconds

from celery.tests.utils import Case, with_eager_tasks, WhateverIO


def now():
    return current_app.now()


def return_True(*args, **kwargs):
    # Task run functions can't be closures/lambdas, as they're pickled.
    return True


return_True_task = task()(return_True)


def raise_exception(self, **kwargs):
    raise Exception('%s error' % self.__class__)


class MockApplyTask(Task):
    applied = 0

    def run(self, x, y):
        return x * y

    @classmethod
    def apply_async(self, *args, **kwargs):
        self.applied += 1


@task(name='c.unittest.increment_counter_task', count=0)
def increment_counter(increment_by=1):
    increment_counter.count += increment_by or 1
    return increment_counter.count


@task(name='c.unittest.raising_task')
def raising():
    raise KeyError('foo')


@task(max_retries=3, iterations=0)
def retry_task(arg1, arg2, kwarg=1, max_retries=None, care=True):
    current.iterations += 1
    rmax = current.max_retries if max_retries is None else max_retries

    assert repr(current.request)
    retries = current.request.retries
    if care and retries >= rmax:
        return arg1
    else:
        raise current.retry(countdown=0, max_retries=rmax)


@task(max_retries=3, iterations=0, accept_magic_kwargs=True)
def retry_task_noargs(**kwargs):
    current.iterations += 1

    retries = kwargs['task_retries']
    if retries >= 3:
        return 42
    else:
        raise current.retry(countdown=0)


@task(max_retries=3, iterations=0, base=MockApplyTask,
      accept_magic_kwargs=True)
def retry_task_mockapply(arg1, arg2, kwarg=1, **kwargs):
    current.iterations += 1

    retries = kwargs['task_retries']
    if retries >= 3:
        return arg1
    else:
        kwargs.update(kwarg=kwarg)
    raise current.retry(countdown=0)


class MyCustomException(Exception):
    """Random custom exception."""


@task(max_retries=3, iterations=0, accept_magic_kwargs=True)
def retry_task_customexc(arg1, arg2, kwarg=1, **kwargs):
    current.iterations += 1

    retries = kwargs['task_retries']
    if retries >= 3:
        return arg1 + kwarg
    else:
        try:
            raise MyCustomException('Elaine Marie Benes')
        except MyCustomException, exc:
            kwargs.update(kwarg=kwarg)
            raise current.retry(countdown=0, exc=exc)


class test_task_retries(Case):

    def test_retry(self):
        retry_task.__class__.max_retries = 3
        retry_task.iterations = 0
        retry_task.apply([0xFF, 0xFFFF])
        self.assertEqual(retry_task.iterations, 4)

        retry_task.__class__.max_retries = 3
        retry_task.iterations = 0
        retry_task.apply([0xFF, 0xFFFF], {'max_retries': 10})
        self.assertEqual(retry_task.iterations, 11)

    def test_retry_no_args(self):
        assert retry_task_noargs.accept_magic_kwargs
        retry_task_noargs.__class__.max_retries = 3
        retry_task_noargs.iterations = 0
        retry_task_noargs.apply()
        self.assertEqual(retry_task_noargs.iterations, 4)

    def test_retry_kwargs_can_be_empty(self):
        retry_task_mockapply.push_request()
        try:
            with self.assertRaises(RetryTaskError):
                retry_task_mockapply.retry(args=[4, 4], kwargs=None)
        finally:
            retry_task_mockapply.pop_request()

    def test_retry_not_eager(self):
        retry_task_mockapply.push_request()
        try:
            retry_task_mockapply.request.called_directly = False
            exc = Exception('baz')
            try:
                retry_task_mockapply.retry(
                    args=[4, 4], kwargs={'task_retries': 0},
                    exc=exc, throw=False,
                )
                self.assertTrue(retry_task_mockapply.__class__.applied)
            finally:
                retry_task_mockapply.__class__.applied = 0

            try:
                with self.assertRaises(RetryTaskError):
                    retry_task_mockapply.retry(
                        args=[4, 4], kwargs={'task_retries': 0},
                        exc=exc, throw=True)
                self.assertTrue(retry_task_mockapply.__class__.applied)
            finally:
                retry_task_mockapply.__class__.applied = 0
        finally:
            retry_task_mockapply.pop_request()

    def test_retry_with_kwargs(self):
        retry_task_customexc.__class__.max_retries = 3
        retry_task_customexc.iterations = 0
        retry_task_customexc.apply([0xFF, 0xFFFF], {'kwarg': 0xF})
        self.assertEqual(retry_task_customexc.iterations, 4)

    def test_retry_with_custom_exception(self):
        retry_task_customexc.__class__.max_retries = 2
        retry_task_customexc.iterations = 0
        result = retry_task_customexc.apply([0xFF, 0xFFFF], {'kwarg': 0xF})
        with self.assertRaises(MyCustomException):
            result.get()
        self.assertEqual(retry_task_customexc.iterations, 3)

    def test_max_retries_exceeded(self):
        retry_task.__class__.max_retries = 2
        retry_task.iterations = 0
        result = retry_task.apply([0xFF, 0xFFFF], {'care': False})
        with self.assertRaises(retry_task.MaxRetriesExceededError):
            result.get()
        self.assertEqual(retry_task.iterations, 3)

        retry_task.__class__.max_retries = 1
        retry_task.iterations = 0
        result = retry_task.apply([0xFF, 0xFFFF], {'care': False})
        with self.assertRaises(retry_task.MaxRetriesExceededError):
            result.get()
        self.assertEqual(retry_task.iterations, 2)


class test_canvas_utils(Case):

    def test_si(self):
        self.assertTrue(retry_task.si())
        self.assertTrue(retry_task.si().immutable)

    def test_chunks(self):
        self.assertTrue(retry_task.chunks(range(100), 10))

    def test_map(self):
        self.assertTrue(retry_task.map(range(100)))

    def test_starmap(self):
        self.assertTrue(retry_task.starmap(range(100)))

    def test_on_success(self):
        retry_task.on_success(1, 1, (), {})


class test_tasks(Case):

    def test_unpickle_task(self):
        import pickle

        @task
        def xxx():
            pass

        self.assertIs(pickle.loads(pickle.dumps(xxx)), xxx.app.tasks[xxx.name])

    def createTask(self, name):
        return task(__module__=self.__module__, name=name)(return_True)

    def test_AsyncResult(self):
        task_id = uuid()
        result = retry_task.AsyncResult(task_id)
        self.assertEqual(result.backend, retry_task.backend)
        self.assertEqual(result.id, task_id)

    def assertNextTaskDataEqual(self, consumer, presult, task_name,
                                test_eta=False, test_expires=False, **kwargs):
        next_task = consumer.queues[0].get()
        task_data = next_task.decode()
        self.assertEqual(task_data['id'], presult.id)
        self.assertEqual(task_data['task'], task_name)
        task_kwargs = task_data.get('kwargs', {})
        if test_eta:
            self.assertIsInstance(task_data.get('eta'), basestring)
            to_datetime = parse_iso8601(task_data.get('eta'))
            self.assertIsInstance(to_datetime, datetime)
        if test_expires:
            self.assertIsInstance(task_data.get('expires'), basestring)
            to_datetime = parse_iso8601(task_data.get('expires'))
            self.assertIsInstance(to_datetime, datetime)
        for arg_name, arg_value in kwargs.items():
            self.assertEqual(task_kwargs.get(arg_name), arg_value)

    def test_incomplete_task_cls(self):

        class IncompleteTask(Task):
            name = 'c.unittest.t.itask'

        with self.assertRaises(NotImplementedError):
            IncompleteTask().run()

    def test_task_kwargs_must_be_dictionary(self):
        with self.assertRaises(ValueError):
            increment_counter.apply_async([], 'str')

    def test_task_args_must_be_list(self):
        with self.assertRaises(ValueError):
            increment_counter.apply_async('str', {})

    def test_regular_task(self):
        T1 = self.createTask('c.unittest.t.t1')
        self.assertIsInstance(T1, BaseTask)
        self.assertTrue(T1.run())
        self.assertTrue(callable(T1), 'Task class is callable()')
        self.assertTrue(T1(), 'Task class runs run() when called')

        consumer = T1.get_consumer()
        with self.assertRaises(NotImplementedError):
            consumer.receive('foo', 'foo')
        consumer.purge()
        self.assertIsNone(consumer.queues[0].get())

        # Without arguments.
        presult = T1.delay()
        self.assertNextTaskDataEqual(consumer, presult, T1.name)

        # With arguments.
        presult2 = T1.apply_async(kwargs=dict(name='George Costanza'))
        self.assertNextTaskDataEqual(
            consumer, presult2, T1.name, name='George Costanza',
        )

        # send_task
        sresult = send_task(T1.name, kwargs=dict(name='Elaine M. Benes'))
        self.assertNextTaskDataEqual(
            consumer, sresult, T1.name, name='Elaine M. Benes',
        )

        # With eta.
        presult2 = T1.apply_async(
            kwargs=dict(name='George Costanza'),
            eta=now() + timedelta(days=1),
            expires=now() + timedelta(days=2),
        )
        self.assertNextTaskDataEqual(
            consumer, presult2, T1.name,
            name='George Costanza', test_eta=True, test_expires=True,
        )

        # With countdown.
        presult2 = T1.apply_async(kwargs=dict(name='George Costanza'),
                                  countdown=10, expires=12)
        self.assertNextTaskDataEqual(
            consumer, presult2, T1.name,
            name='George Costanza', test_eta=True, test_expires=True,
        )

        # Discarding all tasks.
        consumer.purge()
        T1.apply_async()
        self.assertEqual(consumer.purge(), 1)
        self.assertIsNone(consumer.queues[0].get())

        self.assertFalse(presult.successful())
        T1.backend.mark_as_done(presult.id, result=None)
        self.assertTrue(presult.successful())

        publisher = T1.get_publisher()
        self.assertTrue(publisher.exchange)

    def test_context_get(self):
        task = self.createTask('c.unittest.t.c.g')
        task.push_request()
        try:
            request = task.request
            request.foo = 32
            self.assertEqual(request.get('foo'), 32)
            self.assertEqual(request.get('bar', 36), 36)
            request.clear()
        finally:
            task.pop_request()

    def test_task_class_repr(self):
        task = self.createTask('c.unittest.t.repr')
        self.assertIn('class Task of', repr(task.app.Task))
        prev, task.app.Task._app = task.app.Task._app, None
        try:
            self.assertIn('unbound', repr(task.app.Task, ))
        finally:
            task.app.Task._app = prev

    def test_bind_no_magic_kwargs(self):
        task = self.createTask('c.unittest.t.magic_kwargs')
        task.__class__.accept_magic_kwargs = None
        task.bind(task.app)

    def test_annotate(self):
        with patch('celery.app.task.resolve_all_annotations') as anno:
            anno.return_value = [{'FOO': 'BAR'}]
            Task.annotate()
            self.assertEqual(Task.FOO, 'BAR')

    def test_after_return(self):
        task = self.createTask('c.unittest.t.after_return')
        task.push_request()
        try:
            task.request.chord = return_True_task.s()
            task.after_return('SUCCESS', 1.0, 'foobar', (), {}, None)
            task.request.clear()
        finally:
            task.pop_request()

    def test_send_task_sent_event(self):
        T1 = self.createTask('c.unittest.t.t1')
        app = T1.app
        with app.connection() as conn:
            app.conf.CELERY_SEND_TASK_SENT_EVENT = True
            del(app.amqp.__dict__['TaskProducer'])
            try:
                self.assertTrue(app.amqp.TaskProducer(conn).send_sent_event)
            finally:
                app.conf.CELERY_SEND_TASK_SENT_EVENT = False
                del(app.amqp.__dict__['TaskProducer'])

    def test_get_publisher(self):
        connection = app_or_default().connection()
        p = increment_counter.get_publisher(connection, auto_declare=False,
                                            exchange='foo')
        self.assertEqual(p.exchange.name, 'foo')
        p = increment_counter.get_publisher(connection, auto_declare=False,
                                            exchange='foo',
                                            exchange_type='fanout')
        self.assertEqual(p.exchange.type, 'fanout')

    def test_update_state(self):

        @task
        def yyy():
            pass

        yyy.push_request()
        try:
            tid = uuid()
            yyy.update_state(tid, 'FROBULATING', {'fooz': 'baaz'})
            self.assertEqual(yyy.AsyncResult(tid).status, 'FROBULATING')
            self.assertDictEqual(yyy.AsyncResult(tid).result, {'fooz': 'baaz'})

            yyy.request.id = tid
            yyy.update_state(state='FROBUZATING', meta={'fooz': 'baaz'})
            self.assertEqual(yyy.AsyncResult(tid).status, 'FROBUZATING')
            self.assertDictEqual(yyy.AsyncResult(tid).result, {'fooz': 'baaz'})
        finally:
            yyy.pop_request()

    def test_repr(self):

        @task
        def task_test_repr():
            pass

        self.assertIn('task_test_repr', repr(task_test_repr))

    def test_has___name__(self):

        @task
        def yyy2():
            pass

        self.assertTrue(yyy2.__name__)

    def test_get_logger(self):
        t1 = self.createTask('c.unittest.t.t1')
        t1.push_request()
        try:
            logfh = WhateverIO()
            logger = t1.get_logger(logfile=logfh, loglevel=0)
            self.assertTrue(logger)

            t1.request.loglevel = 3
            logger = t1.get_logger(logfile=logfh, loglevel=None)
            self.assertTrue(logger)
        finally:
            t1.pop_request()


class test_TaskSet(Case):

    @with_eager_tasks
    def test_function_taskset(self):
        subtasks = [return_True_task.s(i) for i in range(1, 6)]
        ts = TaskSet(subtasks)
        res = ts.apply_async()
        self.assertListEqual(res.join(), [True, True, True, True, True])

    def test_counter_taskset(self):
        increment_counter.count = 0
        ts = TaskSet(tasks=[
            increment_counter.s(),
            increment_counter.s(increment_by=2),
            increment_counter.s(increment_by=3),
            increment_counter.s(increment_by=4),
            increment_counter.s(increment_by=5),
            increment_counter.s(increment_by=6),
            increment_counter.s(increment_by=7),
            increment_counter.s(increment_by=8),
            increment_counter.s(increment_by=9),
        ])
        self.assertEqual(ts.total, 9)

        consumer = increment_counter.get_consumer()
        consumer.purge()
        consumer.close()
        taskset_res = ts.apply_async()
        subtasks = taskset_res.subtasks
        taskset_id = taskset_res.taskset_id
        consumer = increment_counter.get_consumer()
        for subtask in subtasks:
            m = consumer.queues[0].get().payload
            self.assertDictContainsSubset({'taskset': taskset_id,
                                           'task': increment_counter.name,
                                           'id': subtask.id}, m)
            increment_counter(
                increment_by=m.get('kwargs', {}).get('increment_by'))
        self.assertEqual(increment_counter.count, sum(xrange(1, 10)))

    def test_named_taskset(self):
        prefix = 'test_named_taskset-'
        ts = TaskSet([return_True_task.subtask([1])])
        res = ts.apply(taskset_id=prefix + uuid())
        self.assertTrue(res.taskset_id.startswith(prefix))


class test_apply_task(Case):

    def test_apply_throw(self):
        with self.assertRaises(KeyError):
            raising.apply(throw=True)

    def test_apply_no_magic_kwargs(self):
        increment_counter.accept_magic_kwargs = False
        try:
            increment_counter.apply()
        finally:
            increment_counter.accept_magic_kwargs = True

    def test_apply_with_CELERY_EAGER_PROPAGATES_EXCEPTIONS(self):
        raising.app.conf.CELERY_EAGER_PROPAGATES_EXCEPTIONS = True
        try:
            with self.assertRaises(KeyError):
                raising.apply()
        finally:
            raising.app.conf.CELERY_EAGER_PROPAGATES_EXCEPTIONS = False

    def test_apply(self):
        increment_counter.count = 0

        e = increment_counter.apply()
        self.assertIsInstance(e, EagerResult)
        self.assertEqual(e.get(), 1)

        e = increment_counter.apply(args=[1])
        self.assertEqual(e.get(), 2)

        e = increment_counter.apply(kwargs={'increment_by': 4})
        self.assertEqual(e.get(), 6)

        self.assertTrue(e.successful())
        self.assertTrue(e.ready())
        self.assertTrue(repr(e).startswith('<EagerResult:'))

        f = raising.apply()
        self.assertTrue(f.ready())
        self.assertFalse(f.successful())
        self.assertTrue(f.traceback)
        with self.assertRaises(KeyError):
            f.get()


@periodic_task(run_every=timedelta(hours=1))
def my_periodic():
    pass


class test_periodic_tasks(Case):

    def test_must_have_run_every(self):
        with self.assertRaises(NotImplementedError):
            type('Foo', (PeriodicTask, ), {'__module__': __name__})

    def test_remaining_estimate(self):
        s = my_periodic.run_every
        self.assertIsInstance(
            s.remaining_estimate(s.maybe_make_aware(now())),
            timedelta)

    def test_is_due_not_due(self):
        due, remaining = my_periodic.run_every.is_due(now())
        self.assertFalse(due)
        # This assertion may fail if executed in the
        # first minute of an hour, thus 59 instead of 60
        self.assertGreater(remaining, 59)

    def test_is_due(self):
        p = my_periodic
        due, remaining = p.run_every.is_due(
            now() - p.run_every.run_every)
        self.assertTrue(due)
        self.assertEqual(remaining,
                         timedelta_seconds(p.run_every.run_every))

    def test_schedule_repr(self):
        p = my_periodic
        self.assertTrue(repr(p.run_every))


@periodic_task(run_every=crontab())
def every_minute():
    pass


@periodic_task(run_every=crontab(minute='*/15'))
def quarterly():
    pass


@periodic_task(run_every=crontab(minute=30))
def hourly():
    pass


@periodic_task(run_every=crontab(hour=7, minute=30))
def daily():
    pass


@periodic_task(run_every=crontab(hour=7, minute=30,
                                 day_of_week='thursday'))
def weekly():
    pass


@periodic_task(run_every=crontab(hour=7, minute=30,
                                 day_of_week='thursday',
                                 day_of_month='8-14'))
def monthly():
    pass


@periodic_task(run_every=crontab(hour=7, minute=30,
                                 day_of_week='thursday',
                                 day_of_month='8-14',
                                 month_of_year=3))
def yearly():
    pass


def patch_crontab_nowfun(cls, retval):

    def create_patcher(fun):

        @wraps(fun)
        def __inner(*args, **kwargs):
            prev_nowfun = cls.run_every.nowfun
            cls.run_every.nowfun = lambda: retval
            try:
                return fun(*args, **kwargs)
            finally:
                cls.run_every.nowfun = prev_nowfun

        return __inner

    return create_patcher


class test_crontab_parser(Case):

    def test_crontab_reduce(self):
        self.assertTrue(loads(dumps(crontab('*'))))

    def test_range_steps_not_enough(self):
        with self.assertRaises(crontab_parser.ParseException):
            crontab_parser(24)._range_steps([1])

    def test_parse_star(self):
        self.assertEqual(crontab_parser(24).parse('*'), set(range(24)))
        self.assertEqual(crontab_parser(60).parse('*'), set(range(60)))
        self.assertEqual(crontab_parser(7).parse('*'), set(range(7)))
        self.assertEqual(crontab_parser(31, 1).parse('*'),
                         set(range(1, 31 + 1)))
        self.assertEqual(crontab_parser(12, 1).parse('*'),
                         set(range(1, 12 + 1)))

    def test_parse_range(self):
        self.assertEqual(crontab_parser(60).parse('1-10'),
                         set(range(1, 10 + 1)))
        self.assertEqual(crontab_parser(24).parse('0-20'),
                         set(range(0, 20 + 1)))
        self.assertEqual(crontab_parser().parse('2-10'),
                         set(range(2, 10 + 1)))
        self.assertEqual(crontab_parser(60, 1).parse('1-10'),
                         set(range(1, 10 + 1)))

    def test_parse_range_wraps(self):
        self.assertEqual(crontab_parser(12).parse('11-1'),
                         set([11, 0, 1]))
        self.assertEqual(crontab_parser(60, 1).parse('2-1'),
                         set(range(1, 60 + 1)))

    def test_parse_groups(self):
        self.assertEqual(crontab_parser().parse('1,2,3,4'),
                         set([1, 2, 3, 4]))
        self.assertEqual(crontab_parser().parse('0,15,30,45'),
                         set([0, 15, 30, 45]))
        self.assertEqual(crontab_parser(min_=1).parse('1,2,3,4'),
                         set([1, 2, 3, 4]))

    def test_parse_steps(self):
        self.assertEqual(crontab_parser(8).parse('*/2'),
                         set([0, 2, 4, 6]))
        self.assertEqual(crontab_parser().parse('*/2'),
                         set(i * 2 for i in xrange(30)))
        self.assertEqual(crontab_parser().parse('*/3'),
                         set(i * 3 for i in xrange(20)))
        self.assertEqual(crontab_parser(8, 1).parse('*/2'),
                         set([1, 3, 5, 7]))
        self.assertEqual(crontab_parser(min_=1).parse('*/2'),
                         set(i * 2 + 1 for i in xrange(30)))
        self.assertEqual(crontab_parser(min_=1).parse('*/3'),
                         set(i * 3 + 1 for i in xrange(20)))

    def test_parse_composite(self):
        self.assertEqual(crontab_parser(8).parse('*/2'), set([0, 2, 4, 6]))
        self.assertEqual(crontab_parser().parse('2-9/5'), set([2, 7]))
        self.assertEqual(crontab_parser().parse('2-10/5'), set([2, 7]))
        self.assertEqual(
            crontab_parser(min_=1).parse('55-5/3'),
            set([55, 58, 1, 4]),
        )
        self.assertEqual(crontab_parser().parse('2-11/5,3'), set([2, 3, 7]))
        self.assertEqual(
            crontab_parser().parse('2-4/3,*/5,0-21/4'),
            set([0, 2, 4, 5, 8, 10, 12, 15, 16,
                 20, 25, 30, 35, 40, 45, 50, 55]),
        )
        self.assertEqual(
            crontab_parser().parse('1-9/2'),
            set([1, 3, 5, 7, 9]),
        )
        self.assertEqual(crontab_parser(8, 1).parse('*/2'), set([1, 3, 5, 7]))
        self.assertEqual(crontab_parser(min_=1).parse('2-9/5'), set([2, 7]))
        self.assertEqual(crontab_parser(min_=1).parse('2-10/5'), set([2, 7]))
        self.assertEqual(
            crontab_parser(min_=1).parse('2-11/5,3'),
            set([2, 3, 7]),
        )
        self.assertEqual(
            crontab_parser(min_=1).parse('2-4/3,*/5,1-21/4'),
            set([1, 2, 5, 6, 9, 11, 13, 16, 17,
                 21, 26, 31, 36, 41, 46, 51, 56]),
        )
        self.assertEqual(
            crontab_parser(min_=1).parse('1-9/2'),
            set([1, 3, 5, 7, 9]),
        )

    def test_parse_errors_on_empty_string(self):
        with self.assertRaises(ParseException):
            crontab_parser(60).parse('')

    def test_parse_errors_on_empty_group(self):
        with self.assertRaises(ParseException):
            crontab_parser(60).parse('1,,2')

    def test_parse_errors_on_empty_steps(self):
        with self.assertRaises(ParseException):
            crontab_parser(60).parse('*/')

    def test_parse_errors_on_negative_number(self):
        with self.assertRaises(ParseException):
            crontab_parser(60).parse('-20')

    def test_parse_errors_on_lt_min(self):
        crontab_parser(min_=1).parse('1')
        with self.assertRaises(ValueError):
            crontab_parser(12, 1).parse('0')
        with self.assertRaises(ValueError):
            crontab_parser(24, 1).parse('12-0')

    def test_parse_errors_on_gt_max(self):
        crontab_parser(1).parse('0')
        with self.assertRaises(ValueError):
            crontab_parser(1).parse('1')
        with self.assertRaises(ValueError):
            crontab_parser(60).parse('61-0')

    def test_expand_cronspec_eats_iterables(self):
        self.assertEqual(crontab._expand_cronspec(iter([1, 2, 3]), 100),
                         set([1, 2, 3]))
        self.assertEqual(crontab._expand_cronspec(iter([1, 2, 3]), 100, 1),
                         set([1, 2, 3]))

    def test_expand_cronspec_invalid_type(self):
        with self.assertRaises(TypeError):
            crontab._expand_cronspec(object(), 100)

    def test_repr(self):
        self.assertIn('*', repr(crontab('*')))

    def test_eq(self):
        self.assertEqual(crontab(day_of_week='1, 2'),
                         crontab(day_of_week='1-2'))
        self.assertEqual(crontab(day_of_month='1, 16, 31'),
                         crontab(day_of_month='*/15'))
        self.assertEqual(crontab(minute='1', hour='2', day_of_week='5',
                                 day_of_month='10', month_of_year='5'),
                         crontab(minute='1', hour='2', day_of_week='5',
                                 day_of_month='10', month_of_year='5'))
        self.assertNotEqual(crontab(minute='1'), crontab(minute='2'))
        self.assertNotEqual(crontab(month_of_year='1'),
                            crontab(month_of_year='2'))
        self.assertFalse(object() == crontab(minute='1'))
        self.assertFalse(crontab(minute='1') == object())


class test_crontab_remaining_estimate(Case):

    def next_ocurrance(self, crontab, now):
        crontab.nowfun = lambda: now
        return now + crontab.remaining_estimate(now)

    def test_next_minute(self):
        next = self.next_ocurrance(crontab(),
                                   datetime(2010, 9, 11, 14, 30, 15))
        self.assertEqual(next, datetime(2010, 9, 11, 14, 31))

    def test_not_next_minute(self):
        next = self.next_ocurrance(crontab(),
                                   datetime(2010, 9, 11, 14, 59, 15))
        self.assertEqual(next, datetime(2010, 9, 11, 15, 0))

    def test_this_hour(self):
        next = self.next_ocurrance(crontab(minute=[5, 42]),
                                   datetime(2010, 9, 11, 14, 30, 15))
        self.assertEqual(next, datetime(2010, 9, 11, 14, 42))

    def test_not_this_hour(self):
        next = self.next_ocurrance(crontab(minute=[5, 10, 15]),
                                   datetime(2010, 9, 11, 14, 30, 15))
        self.assertEqual(next, datetime(2010, 9, 11, 15, 5))

    def test_today(self):
        next = self.next_ocurrance(crontab(minute=[5, 42], hour=[12, 17]),
                                   datetime(2010, 9, 11, 14, 30, 15))
        self.assertEqual(next, datetime(2010, 9, 11, 17, 5))

    def test_not_today(self):
        next = self.next_ocurrance(crontab(minute=[5, 42], hour=[12]),
                                   datetime(2010, 9, 11, 14, 30, 15))
        self.assertEqual(next, datetime(2010, 9, 12, 12, 5))

    def test_weekday(self):
        next = self.next_ocurrance(crontab(minute=30,
                                           hour=14,
                                           day_of_week='sat'),
                                   datetime(2010, 9, 11, 14, 30, 15))
        self.assertEqual(next, datetime(2010, 9, 18, 14, 30))

    def test_not_weekday(self):
        next = self.next_ocurrance(crontab(minute=[5, 42],
                                           day_of_week='mon-fri'),
                                   datetime(2010, 9, 11, 14, 30, 15))
        self.assertEqual(next, datetime(2010, 9, 13, 0, 5))

    def test_monthday(self):
        next = self.next_ocurrance(crontab(minute=30,
                                           hour=14,
                                           day_of_month=18),
                                   datetime(2010, 9, 11, 14, 30, 15))
        self.assertEqual(next, datetime(2010, 9, 18, 14, 30))

    def test_not_monthday(self):
        next = self.next_ocurrance(crontab(minute=[5, 42],
                                           day_of_month=29),
                                   datetime(2010, 1, 22, 14, 30, 15))
        self.assertEqual(next, datetime(2010, 1, 29, 0, 5))

    def test_weekday_monthday(self):
        next = self.next_ocurrance(crontab(minute=30,
                                           hour=14,
                                           day_of_week='mon',
                                           day_of_month=18),
                                   datetime(2010, 1, 18, 14, 30, 15))
        self.assertEqual(next, datetime(2010, 10, 18, 14, 30))

    def test_monthday_not_weekday(self):
        next = self.next_ocurrance(crontab(minute=[5, 42],
                                           day_of_week='sat',
                                           day_of_month=29),
                                   datetime(2010, 1, 29, 0, 5, 15))
        self.assertEqual(next, datetime(2010, 5, 29, 0, 5))

    def test_weekday_not_monthday(self):
        next = self.next_ocurrance(crontab(minute=[5, 42],
                                           day_of_week='mon',
                                           day_of_month=18),
                                   datetime(2010, 1, 11, 0, 5, 15))
        self.assertEqual(next, datetime(2010, 1, 18, 0, 5))

    def test_not_weekday_not_monthday(self):
        next = self.next_ocurrance(crontab(minute=[5, 42],
                                           day_of_week='mon',
                                           day_of_month=18),
                                   datetime(2010, 1, 10, 0, 5, 15))
        self.assertEqual(next, datetime(2010, 1, 18, 0, 5))

    def test_leapday(self):
        next = self.next_ocurrance(crontab(minute=30,
                                           hour=14,
                                           day_of_month=29),
                                   datetime(2012, 1, 29, 14, 30, 15))
        self.assertEqual(next, datetime(2012, 2, 29, 14, 30))

    def test_not_leapday(self):
        next = self.next_ocurrance(crontab(minute=30,
                                           hour=14,
                                           day_of_month=29),
                                   datetime(2010, 1, 29, 14, 30, 15))
        self.assertEqual(next, datetime(2010, 3, 29, 14, 30))

    def test_weekmonthdayyear(self):
        next = self.next_ocurrance(crontab(minute=30,
                                           hour=14,
                                           day_of_week='fri',
                                           day_of_month=29,
                                           month_of_year=1),
                                   datetime(2010, 1, 22, 14, 30, 15))
        self.assertEqual(next, datetime(2010, 1, 29, 14, 30))

    def test_monthdayyear_not_week(self):
        next = self.next_ocurrance(crontab(minute=[5, 42],
                                           day_of_week='wed,thu',
                                           day_of_month=29,
                                           month_of_year='1,4,7'),
                                   datetime(2010, 1, 29, 14, 30, 15))
        self.assertEqual(next, datetime(2010, 4, 29, 0, 5))

    def test_weekdaymonthyear_not_monthday(self):
        next = self.next_ocurrance(crontab(minute=30,
                                           hour=14,
                                           day_of_week='fri',
                                           day_of_month=29,
                                           month_of_year='1-10'),
                                   datetime(2010, 1, 29, 14, 30, 15))
        self.assertEqual(next, datetime(2010, 10, 29, 14, 30))

    def test_weekmonthday_not_monthyear(self):
        next = self.next_ocurrance(crontab(minute=[5, 42],
                                           day_of_week='fri',
                                           day_of_month=29,
                                           month_of_year='2-10'),
                                   datetime(2010, 1, 29, 14, 30, 15))
        self.assertEqual(next, datetime(2010, 10, 29, 0, 5))

    def test_weekday_not_monthdayyear(self):
        next = self.next_ocurrance(crontab(minute=[5, 42],
                                           day_of_week='mon',
                                           day_of_month=18,
                                           month_of_year='2-10'),
                                   datetime(2010, 1, 11, 0, 5, 15))
        self.assertEqual(next, datetime(2010, 10, 18, 0, 5))

    def test_monthday_not_weekdaymonthyear(self):
        next = self.next_ocurrance(crontab(minute=[5, 42],
                                           day_of_week='mon',
                                           day_of_month=29,
                                           month_of_year='2-4'),
                                   datetime(2010, 1, 29, 0, 5, 15))
        self.assertEqual(next, datetime(2010, 3, 29, 0, 5))

    def test_monthyear_not_weekmonthday(self):
        next = self.next_ocurrance(crontab(minute=[5, 42],
                                           day_of_week='mon',
                                           day_of_month=29,
                                           month_of_year='2-4'),
                                   datetime(2010, 2, 28, 0, 5, 15))
        self.assertEqual(next, datetime(2010, 3, 29, 0, 5))

    def test_not_weekmonthdayyear(self):
        next = self.next_ocurrance(crontab(minute=[5, 42],
                                           day_of_week='fri,sat',
                                           day_of_month=29,
                                           month_of_year='2-10'),
                                   datetime(2010, 1, 28, 14, 30, 15))
        self.assertEqual(next, datetime(2010, 5, 29, 0, 5))


class test_crontab_is_due(Case):

    def setUp(self):
        self.now = now()
        self.next_minute = 60 - self.now.second - 1e-6 * self.now.microsecond

    def test_default_crontab_spec(self):
        c = crontab()
        self.assertEqual(c.minute, set(range(60)))
        self.assertEqual(c.hour, set(range(24)))
        self.assertEqual(c.day_of_week, set(range(7)))
        self.assertEqual(c.day_of_month, set(range(1, 32)))
        self.assertEqual(c.month_of_year, set(range(1, 13)))

    def test_simple_crontab_spec(self):
        c = crontab(minute=30)
        self.assertEqual(c.minute, set([30]))
        self.assertEqual(c.hour, set(range(24)))
        self.assertEqual(c.day_of_week, set(range(7)))
        self.assertEqual(c.day_of_month, set(range(1, 32)))
        self.assertEqual(c.month_of_year, set(range(1, 13)))

    def test_crontab_spec_minute_formats(self):
        c = crontab(minute=30)
        self.assertEqual(c.minute, set([30]))
        c = crontab(minute='30')
        self.assertEqual(c.minute, set([30]))
        c = crontab(minute=(30, 40, 50))
        self.assertEqual(c.minute, set([30, 40, 50]))
        c = crontab(minute=set([30, 40, 50]))
        self.assertEqual(c.minute, set([30, 40, 50]))

    def test_crontab_spec_invalid_minute(self):
        with self.assertRaises(ValueError):
            crontab(minute=60)
        with self.assertRaises(ValueError):
            crontab(minute='0-100')

    def test_crontab_spec_hour_formats(self):
        c = crontab(hour=6)
        self.assertEqual(c.hour, set([6]))
        c = crontab(hour='5')
        self.assertEqual(c.hour, set([5]))
        c = crontab(hour=(4, 8, 12))
        self.assertEqual(c.hour, set([4, 8, 12]))

    def test_crontab_spec_invalid_hour(self):
        with self.assertRaises(ValueError):
            crontab(hour=24)
        with self.assertRaises(ValueError):
            crontab(hour='0-30')

    def test_crontab_spec_dow_formats(self):
        c = crontab(day_of_week=5)
        self.assertEqual(c.day_of_week, set([5]))
        c = crontab(day_of_week='5')
        self.assertEqual(c.day_of_week, set([5]))
        c = crontab(day_of_week='fri')
        self.assertEqual(c.day_of_week, set([5]))
        c = crontab(day_of_week='tuesday,sunday,fri')
        self.assertEqual(c.day_of_week, set([0, 2, 5]))
        c = crontab(day_of_week='mon-fri')
        self.assertEqual(c.day_of_week, set([1, 2, 3, 4, 5]))
        c = crontab(day_of_week='*/2')
        self.assertEqual(c.day_of_week, set([0, 2, 4, 6]))

    def test_crontab_spec_invalid_dow(self):
        with self.assertRaises(ValueError):
            crontab(day_of_week='fooday-barday')
        with self.assertRaises(ValueError):
            crontab(day_of_week='1,4,foo')
        with self.assertRaises(ValueError):
            crontab(day_of_week='7')
        with self.assertRaises(ValueError):
            crontab(day_of_week='12')

    def test_crontab_spec_dom_formats(self):
        c = crontab(day_of_month=5)
        self.assertEqual(c.day_of_month, set([5]))
        c = crontab(day_of_month='5')
        self.assertEqual(c.day_of_month, set([5]))
        c = crontab(day_of_month='2,4,6')
        self.assertEqual(c.day_of_month, set([2, 4, 6]))
        c = crontab(day_of_month='*/5')
        self.assertEqual(c.day_of_month, set([1, 6, 11, 16, 21, 26, 31]))

    def test_crontab_spec_invalid_dom(self):
        with self.assertRaises(ValueError):
            crontab(day_of_month=0)
        with self.assertRaises(ValueError):
            crontab(day_of_month='0-10')
        with self.assertRaises(ValueError):
            crontab(day_of_month=32)
        with self.assertRaises(ValueError):
            crontab(day_of_month='31,32')

    def test_crontab_spec_moy_formats(self):
        c = crontab(month_of_year=1)
        self.assertEqual(c.month_of_year, set([1]))
        c = crontab(month_of_year='1')
        self.assertEqual(c.month_of_year, set([1]))
        c = crontab(month_of_year='2,4,6')
        self.assertEqual(c.month_of_year, set([2, 4, 6]))
        c = crontab(month_of_year='*/2')
        self.assertEqual(c.month_of_year, set([1, 3, 5, 7, 9, 11]))
        c = crontab(month_of_year='2-12/2')
        self.assertEqual(c.month_of_year, set([2, 4, 6, 8, 10, 12]))

    def test_crontab_spec_invalid_moy(self):
        with self.assertRaises(ValueError):
            crontab(month_of_year=0)
        with self.assertRaises(ValueError):
            crontab(month_of_year='0-5')
        with self.assertRaises(ValueError):
            crontab(month_of_year=13)
        with self.assertRaises(ValueError):
            crontab(month_of_year='12,13')

    def seconds_almost_equal(self, a, b, precision):
        for index, skew in enumerate((+0.1, 0, -0.1)):
            try:
                self.assertAlmostEqual(a, b + skew, precision)
            except AssertionError:
                if index + 1 >= 3:
                    raise
            else:
                break

    def test_every_minute_execution_is_due(self):
        last_ran = self.now - timedelta(seconds=61)
        due, remaining = every_minute.run_every.is_due(last_ran)
        self.assertTrue(due)
        self.seconds_almost_equal(remaining, self.next_minute, 1)

    def test_every_minute_execution_is_not_due(self):
        last_ran = self.now - timedelta(seconds=self.now.second)
        due, remaining = every_minute.run_every.is_due(last_ran)
        self.assertFalse(due)
        self.seconds_almost_equal(remaining, self.next_minute, 1)

    # 29th of May 2010 is a saturday
    @patch_crontab_nowfun(hourly, datetime(2010, 5, 29, 10, 30))
    def test_execution_is_due_on_saturday(self):
        last_ran = self.now - timedelta(seconds=61)
        due, remaining = every_minute.run_every.is_due(last_ran)
        self.assertTrue(due)
        self.seconds_almost_equal(remaining, self.next_minute, 1)

    # 30th of May 2010 is a sunday
    @patch_crontab_nowfun(hourly, datetime(2010, 5, 30, 10, 30))
    def test_execution_is_due_on_sunday(self):
        last_ran = self.now - timedelta(seconds=61)
        due, remaining = every_minute.run_every.is_due(last_ran)
        self.assertTrue(due)
        self.seconds_almost_equal(remaining, self.next_minute, 1)

    # 31st of May 2010 is a monday
    @patch_crontab_nowfun(hourly, datetime(2010, 5, 31, 10, 30))
    def test_execution_is_due_on_monday(self):
        last_ran = self.now - timedelta(seconds=61)
        due, remaining = every_minute.run_every.is_due(last_ran)
        self.assertTrue(due)
        self.seconds_almost_equal(remaining, self.next_minute, 1)

    @patch_crontab_nowfun(hourly, datetime(2010, 5, 10, 10, 30))
    def test_every_hour_execution_is_due(self):
        due, remaining = hourly.run_every.is_due(
            datetime(2010, 5, 10, 6, 30))
        self.assertTrue(due)
        self.assertEqual(remaining, 60 * 60)

    @patch_crontab_nowfun(hourly, datetime(2010, 5, 10, 10, 29))
    def test_every_hour_execution_is_not_due(self):
        due, remaining = hourly.run_every.is_due(
            datetime(2010, 5, 10, 9, 30))
        self.assertFalse(due)
        self.assertEqual(remaining, 60)

    @patch_crontab_nowfun(quarterly, datetime(2010, 5, 10, 10, 15))
    def test_first_quarter_execution_is_due(self):
        due, remaining = quarterly.run_every.is_due(
            datetime(2010, 5, 10, 6, 30))
        self.assertTrue(due)
        self.assertEqual(remaining, 15 * 60)

    @patch_crontab_nowfun(quarterly, datetime(2010, 5, 10, 10, 30))
    def test_second_quarter_execution_is_due(self):
        due, remaining = quarterly.run_every.is_due(
            datetime(2010, 5, 10, 6, 30))
        self.assertTrue(due)
        self.assertEqual(remaining, 15 * 60)

    @patch_crontab_nowfun(quarterly, datetime(2010, 5, 10, 10, 14))
    def test_first_quarter_execution_is_not_due(self):
        due, remaining = quarterly.run_every.is_due(
            datetime(2010, 5, 10, 10, 0))
        self.assertFalse(due)
        self.assertEqual(remaining, 60)

    @patch_crontab_nowfun(quarterly, datetime(2010, 5, 10, 10, 29))
    def test_second_quarter_execution_is_not_due(self):
        due, remaining = quarterly.run_every.is_due(
            datetime(2010, 5, 10, 10, 15))
        self.assertFalse(due)
        self.assertEqual(remaining, 60)

    @patch_crontab_nowfun(daily, datetime(2010, 5, 10, 7, 30))
    def test_daily_execution_is_due(self):
        due, remaining = daily.run_every.is_due(
            datetime(2010, 5, 9, 7, 30))
        self.assertTrue(due)
        self.assertEqual(remaining, 24 * 60 * 60)

    @patch_crontab_nowfun(daily, datetime(2010, 5, 10, 10, 30))
    def test_daily_execution_is_not_due(self):
        due, remaining = daily.run_every.is_due(
            datetime(2010, 5, 10, 7, 30))
        self.assertFalse(due)
        self.assertEqual(remaining, 21 * 60 * 60)

    @patch_crontab_nowfun(weekly, datetime(2010, 5, 6, 7, 30))
    def test_weekly_execution_is_due(self):
        due, remaining = weekly.run_every.is_due(
            datetime(2010, 4, 30, 7, 30))
        self.assertTrue(due)
        self.assertEqual(remaining, 7 * 24 * 60 * 60)

    @patch_crontab_nowfun(weekly, datetime(2010, 5, 7, 10, 30))
    def test_weekly_execution_is_not_due(self):
        due, remaining = weekly.run_every.is_due(
            datetime(2010, 5, 6, 7, 30))
        self.assertFalse(due)
        self.assertEqual(remaining, 6 * 24 * 60 * 60 - 3 * 60 * 60)

    @patch_crontab_nowfun(monthly, datetime(2010, 5, 13, 7, 30))
    def test_monthly_execution_is_due(self):
        due, remaining = monthly.run_every.is_due(
            datetime(2010, 4, 8, 7, 30))
        self.assertTrue(due)
        self.assertEqual(remaining, 28 * 24 * 60 * 60)

    @patch_crontab_nowfun(monthly, datetime(2010, 5, 9, 10, 30))
    def test_monthly_execution_is_not_due(self):
        due, remaining = monthly.run_every.is_due(
            datetime(2010, 4, 8, 7, 30))
        self.assertFalse(due)
        self.assertEqual(remaining, 4 * 24 * 60 * 60 - 3 * 60 * 60)

    @patch_crontab_nowfun(yearly, datetime(2010, 3, 11, 7, 30))
    def test_yearly_execution_is_due(self):
        due, remaining = yearly.run_every.is_due(
            datetime(2009, 3, 12, 7, 30))
        self.assertTrue(due)
        self.assertEqual(remaining, 364 * 24 * 60 * 60)

    @patch_crontab_nowfun(yearly, datetime(2010, 3, 7, 10, 30))
    def test_yearly_execution_is_not_due(self):
        due, remaining = yearly.run_every.is_due(
            datetime(2009, 3, 12, 7, 30))
        self.assertFalse(due)
        self.assertEqual(remaining, 4 * 24 * 60 * 60 - 3 * 60 * 60)
