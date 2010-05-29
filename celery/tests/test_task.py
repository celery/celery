import unittest2 as unittest
from StringIO import StringIO
from datetime import datetime, timedelta

from pyparsing import ParseException

from billiard.utils.functional import wraps

from celery import conf
from celery import task
from celery import messaging
from celery.task.schedules import crontab, crontab_parser
from celery.utils import timeutils
from celery.utils import gen_unique_id
from celery.result import EagerResult
from celery.execute import send_task
from celery.backends import default_backend
from celery.decorators import task as task_dec
from celery.exceptions import RetryTaskError
from celery.worker.listener import parse_iso8601

def return_True(*args, **kwargs):
    # Task run functions can't be closures/lambdas, as they're pickled.
    return True


return_True_task = task_dec()(return_True)


def raise_exception(self, **kwargs):
    raise Exception("%s error" % self.__class__)


class MockApplyTask(task.Task):

    def run(self, x, y):
        return x * y

    @classmethod
    def apply_async(self, *args, **kwargs):
        pass


class IncrementCounterTask(task.Task):
    name = "c.unittest.increment_counter_task"
    count = 0

    def run(self, increment_by=1, **kwargs):
        increment_by = increment_by or 1
        self.__class__.count += increment_by
        return self.__class__.count


class RaisingTask(task.Task):
    name = "c.unittest.raising_task"

    def run(self, **kwargs):
        raise KeyError("foo")


class RetryTask(task.Task):
    max_retries = 3
    iterations = 0

    def run(self, arg1, arg2, kwarg=1, **kwargs):
        self.__class__.iterations += 1

        retries = kwargs["task_retries"]
        if retries >= 3:
            return arg1
        else:
            kwargs.update({"kwarg": kwarg})
            return self.retry(args=[arg1, arg2], kwargs=kwargs, countdown=0)


class RetryTaskNoArgs(task.Task):
    max_retries = 3
    iterations = 0

    def run(self, **kwargs):
        self.__class__.iterations += 1

        retries = kwargs["task_retries"]
        if retries >= 3:
            return 42
        else:
            return self.retry(kwargs=kwargs, countdown=0)


class RetryTaskMockApply(task.Task):
    max_retries = 3
    iterations = 0
    applied = 0

    def run(self, arg1, arg2, kwarg=1, **kwargs):
        self.__class__.iterations += 1

        retries = kwargs["task_retries"]
        if retries >= 3:
            return arg1
        else:
            kwargs.update({"kwarg": kwarg})
            return self.retry(args=[arg1, arg2], kwargs=kwargs, countdown=0)

    @classmethod
    def apply_async(self, *args, **kwargs):
        self.applied = 1


class MyCustomException(Exception):
    """Random custom exception."""


class RetryTaskCustomExc(task.Task):
    max_retries = 3
    iterations = 0

    def run(self, arg1, arg2, kwarg=1, **kwargs):
        self.__class__.iterations += 1

        retries = kwargs["task_retries"]
        if retries >= 3:
            return arg1 + kwarg
        else:
            try:
                raise MyCustomException("Elaine Marie Benes")
            except MyCustomException, exc:
                kwargs.update({"kwarg": kwarg})
                return self.retry(args=[arg1, arg2], kwargs=kwargs,
                                  countdown=0, exc=exc)


class TestTaskRetries(unittest.TestCase):

    def test_retry(self):
        RetryTask.max_retries = 3
        RetryTask.iterations = 0
        result = RetryTask.apply([0xFF, 0xFFFF])
        self.assertEqual(result.get(), 0xFF)
        self.assertEqual(RetryTask.iterations, 4)

    def test_retry_no_args(self):
        RetryTaskNoArgs.max_retries = 3
        RetryTaskNoArgs.iterations = 0
        result = RetryTaskNoArgs.apply()
        self.assertEqual(result.get(), 42)
        self.assertEqual(RetryTaskNoArgs.iterations, 4)

    def test_retry_not_eager(self):
        exc = Exception("baz")
        try:
            RetryTaskMockApply.retry(args=[4, 4], kwargs={},
                                     exc=exc, throw=False)
            self.assertTrue(RetryTaskMockApply.applied)
        finally:
            RetryTaskMockApply.applied = 0

        try:
            self.assertRaises(RetryTaskError, RetryTaskMockApply.retry,
                    args=[4, 4], kwargs={}, exc=exc, throw=True)
            self.assertTrue(RetryTaskMockApply.applied)
        finally:
            RetryTaskMockApply.applied = 0

    def test_retry_with_kwargs(self):
        RetryTaskCustomExc.max_retries = 3
        RetryTaskCustomExc.iterations = 0
        result = RetryTaskCustomExc.apply([0xFF, 0xFFFF], {"kwarg": 0xF})
        self.assertEqual(result.get(), 0xFF + 0xF)
        self.assertEqual(RetryTaskCustomExc.iterations, 4)

    def test_retry_with_custom_exception(self):
        RetryTaskCustomExc.max_retries = 2
        RetryTaskCustomExc.iterations = 0
        result = RetryTaskCustomExc.apply([0xFF, 0xFFFF], {"kwarg": 0xF})
        self.assertRaises(MyCustomException,
                          result.get)
        self.assertEqual(RetryTaskCustomExc.iterations, 3)

    def test_max_retries_exceeded(self):
        RetryTask.max_retries = 2
        RetryTask.iterations = 0
        result = RetryTask.apply([0xFF, 0xFFFF])
        self.assertRaises(RetryTask.MaxRetriesExceededError,
                          result.get)
        self.assertEqual(RetryTask.iterations, 3)

        RetryTask.max_retries = 1
        RetryTask.iterations = 0
        result = RetryTask.apply([0xFF, 0xFFFF])
        self.assertRaises(RetryTask.MaxRetriesExceededError,
                          result.get)
        self.assertEqual(RetryTask.iterations, 2)


class MockPublisher(object):

    def __init__(self, *args, **kwargs):
        self.kwargs = kwargs


class TestCeleryTasks(unittest.TestCase):

    def createTaskCls(self, cls_name, task_name=None):
        attrs = {"__module__": self.__module__}
        if task_name:
            attrs["name"] = task_name

        cls = type(cls_name, (task.Task, ), attrs)
        cls.run = return_True
        return cls

    def test_AsyncResult(self):
        task_id = gen_unique_id()
        result = RetryTask.AsyncResult(task_id)
        self.assertEqual(result.backend, RetryTask.backend)
        self.assertEqual(result.task_id, task_id)

    def test_ping(self):
        from celery import conf
        conf.ALWAYS_EAGER = True
        self.assertEqual(task.ping(), 'pong')
        conf.ALWAYS_EAGER = False

    def test_execute_remote(self):
        from celery import conf
        conf.ALWAYS_EAGER = True
        self.assertEqual(task.execute_remote(return_True, ["foo"]).get(),
                          True)
        conf.ALWAYS_EAGER = False

    def test_dmap(self):
        from celery import conf
        import operator
        conf.ALWAYS_EAGER = True
        res = task.dmap(operator.add, zip(xrange(10), xrange(10)))
        self.assertEqual(sum(res), sum(operator.add(x, x)
                                        for x in xrange(10)))
        conf.ALWAYS_EAGER = False

    def test_dmap_async(self):
        from celery import conf
        import operator
        conf.ALWAYS_EAGER = True
        res = task.dmap_async(operator.add, zip(xrange(10), xrange(10)))
        self.assertEqual(sum(res.get()), sum(operator.add(x, x)
                                                for x in xrange(10)))
        conf.ALWAYS_EAGER = False

    def assertNextTaskDataEqual(self, consumer, presult, task_name,
            test_eta=False, **kwargs):
        next_task = consumer.fetch()
        task_data = next_task.decode()
        self.assertEqual(task_data["id"], presult.task_id)
        self.assertEqual(task_data["task"], task_name)
        task_kwargs = task_data.get("kwargs", {})
        if test_eta:
            self.assertIsInstance(task_data.get("eta"), basestring)
            to_datetime = parse_iso8601(task_data.get("eta"))
            self.assertIsInstance(to_datetime, datetime)
        for arg_name, arg_value in kwargs.items():
            self.assertEqual(task_kwargs.get(arg_name), arg_value)

    def test_incomplete_task_cls(self):

        class IncompleteTask(task.Task):
            name = "c.unittest.t.itask"

        self.assertRaises(NotImplementedError, IncompleteTask().run)

    def test_task_kwargs_must_be_dictionary(self):
        self.assertRaises(ValueError, IncrementCounterTask.apply_async,
                          [], "str")

    def test_task_args_must_be_list(self):
        self.assertRaises(ValueError, IncrementCounterTask.apply_async,
                          "str", {})

    def test_regular_task(self):
        T1 = self.createTaskCls("T1", "c.unittest.t.t1")
        self.assertIsInstance(T1(), T1)
        self.assertTrue(T1().run())
        self.assertTrue(callable(T1()),
                "Task class is callable()")
        self.assertTrue(T1()(),
                "Task class runs run() when called")

        # task name generated out of class module + name.
        T2 = self.createTaskCls("T2")
        self.assertTrue(T2().name.endswith("test_task.T2"))

        t1 = T1()
        consumer = t1.get_consumer()
        self.assertRaises(NotImplementedError, consumer.receive, "foo", "foo")
        consumer.discard_all()
        self.assertIsNone(consumer.fetch())

        # Without arguments.
        presult = t1.delay()
        self.assertNextTaskDataEqual(consumer, presult, t1.name)

        # With arguments.
        presult2 = t1.apply_async(kwargs=dict(name="George Constanza"))
        self.assertNextTaskDataEqual(consumer, presult2, t1.name,
                name="George Constanza")

        # send_task
        sresult = send_task(t1.name, kwargs=dict(name="Elaine M. Benes"))
        self.assertNextTaskDataEqual(consumer, sresult, t1.name,
                name="Elaine M. Benes")

        # With eta.
        presult2 = task.apply_async(t1, kwargs=dict(name="George Constanza"),
                                    eta=datetime.now() + timedelta(days=1))
        self.assertNextTaskDataEqual(consumer, presult2, t1.name,
                name="George Constanza", test_eta=True)

        # With countdown.
        presult2 = task.apply_async(t1, kwargs=dict(name="George Constanza"),
                                    countdown=10)
        self.assertNextTaskDataEqual(consumer, presult2, t1.name,
                name="George Constanza", test_eta=True)

        # Discarding all tasks.
        consumer.discard_all()
        task.apply_async(t1)
        self.assertEqual(consumer.discard_all(), 1)
        self.assertIsNone(consumer.fetch())

        self.assertFalse(presult.successful())
        default_backend.mark_as_done(presult.task_id, result=None)
        self.assertTrue(presult.successful())

        publisher = t1.get_publisher()
        self.assertIsInstance(publisher, messaging.TaskPublisher)

    def test_get_publisher(self):
        from celery.task import base
        old_pub = base.TaskPublisher
        base.TaskPublisher = MockPublisher
        try:
            p = IncrementCounterTask.get_publisher(exchange="foo",
                                                   connection="bar")
            self.assertEqual(p.kwargs["exchange"], "foo")
        finally:
            base.TaskPublisher = old_pub

    def test_get_logger(self):
        T1 = self.createTaskCls("T1", "c.unittest.t.t1")
        t1 = T1()
        logfh = StringIO()
        logger = t1.get_logger(logfile=logfh, loglevel=0)
        self.assertTrue(logger)


class TestTaskSet(unittest.TestCase):

    def test_function_taskset(self):
        from celery import conf
        conf.ALWAYS_EAGER = True
        ts = task.TaskSet(return_True_task.name, [
            ([1], {}), [[2], {}], [[3], {}], [[4], {}], [[5], {}]])
        res = ts.apply_async()
        self.assertListEqual(res.join(), [True, True, True, True, True])

        conf.ALWAYS_EAGER = False

    def test_counter_taskset(self):
        IncrementCounterTask.count = 0
        ts = task.TaskSet(IncrementCounterTask, [
            ([], {}),
            ([], {"increment_by": 2}),
            ([], {"increment_by": 3}),
            ([], {"increment_by": 4}),
            ([], {"increment_by": 5}),
            ([], {"increment_by": 6}),
            ([], {"increment_by": 7}),
            ([], {"increment_by": 8}),
            ([], {"increment_by": 9}),
        ])
        self.assertEqual(ts.task_name, IncrementCounterTask.name)
        self.assertEqual(ts.total, 9)


        consumer = IncrementCounterTask().get_consumer()
        consumer.discard_all()
        taskset_res = ts.apply_async()
        subtasks = taskset_res.subtasks
        taskset_id = taskset_res.taskset_id
        for subtask in subtasks:
            m = consumer.fetch().payload
            self.assertDictContainsSubset({"taskset": taskset_id,
                                           "task": IncrementCounterTask.name,
                                           "id": subtask.task_id}, m)
            IncrementCounterTask().run(
                    increment_by=m.get("kwargs", {}).get("increment_by"))
        self.assertEqual(IncrementCounterTask.count, sum(xrange(1, 10)))


class TestTaskApply(unittest.TestCase):

    def test_apply_throw(self):
        self.assertRaises(KeyError, RaisingTask.apply, throw=True)

    def test_apply_with_CELERY_EAGER_PROPAGATES_EXCEPTIONS(self):
        conf.EAGER_PROPAGATES_EXCEPTIONS = True
        try:
            self.assertRaises(KeyError, RaisingTask.apply)
        finally:
            conf.EAGER_PROPAGATES_EXCEPTIONS = False

    def test_apply(self):
        IncrementCounterTask.count = 0

        e = IncrementCounterTask.apply()
        self.assertIsInstance(e, EagerResult)
        self.assertEqual(e.get(), 1)

        e = IncrementCounterTask.apply(args=[1])
        self.assertEqual(e.get(), 2)

        e = IncrementCounterTask.apply(kwargs={"increment_by": 4})
        self.assertEqual(e.get(), 6)

        self.assertTrue(e.successful())
        self.assertTrue(e.ready())
        self.assertTrue(repr(e).startswith("<EagerResult:"))

        f = RaisingTask.apply()
        self.assertTrue(f.ready())
        self.assertFalse(f.successful())
        self.assertTrue(f.traceback)
        self.assertRaises(KeyError, f.get)


class MyPeriodic(task.PeriodicTask):
    run_every = timedelta(hours=1)


class TestPeriodicTask(unittest.TestCase):

    def test_must_have_run_every(self):
        self.assertRaises(NotImplementedError, type, "Foo",
            (task.PeriodicTask, ), {"__module__": __name__})

    def test_remaining_estimate(self):
        self.assertIsInstance(
            MyPeriodic().remaining_estimate(datetime.now()),
            timedelta)

    def test_timedelta_seconds_returns_0_on_negative_time(self):
        delta = timedelta(days=-2)
        self.assertEqual(MyPeriodic().timedelta_seconds(delta), 0)

    def test_timedelta_seconds(self):
        deltamap = ((timedelta(seconds=1), 1),
                    (timedelta(seconds=27), 27),
                    (timedelta(minutes=3), 3 * 60),
                    (timedelta(hours=4), 4 * 60 * 60),
                    (timedelta(days=3), 3 * 86400))
        for delta, seconds in deltamap:
            self.assertEqual(MyPeriodic().timedelta_seconds(delta), seconds)

    def test_delta_resolution(self):
        D = timeutils.delta_resolution

        dt = datetime(2010, 3, 30, 11, 50, 58, 41065)
        deltamap = ((timedelta(days=2), datetime(2010, 3, 30, 0, 0)),
                    (timedelta(hours=2), datetime(2010, 3, 30, 11, 0)),
                    (timedelta(minutes=2), datetime(2010, 3, 30, 11, 50)),
                    (timedelta(seconds=2), dt))
        for delta, shoulda in deltamap:
            self.assertEqual(D(dt, delta), shoulda)

    def test_is_due_not_due(self):
        due, remaining = MyPeriodic().is_due(datetime.now())
        self.assertFalse(due)
        self.assertGreater(remaining, 60)

    def test_is_due(self):
        p = MyPeriodic()
        due, remaining = p.is_due(datetime.now() - p.run_every.run_every)
        self.assertTrue(due)
        self.assertEqual(remaining,
                         p.timedelta_seconds(p.run_every.run_every))


class EveryMinutePeriodic(task.PeriodicTask):
    run_every = crontab()


class QuarterlyPeriodic(task.PeriodicTask):
    run_every = crontab(minute="*/15")


class HourlyPeriodic(task.PeriodicTask):
    run_every = crontab(minute=30)


class DailyPeriodic(task.PeriodicTask):
    run_every = crontab(hour=7, minute=30)


class WeeklyPeriodic(task.PeriodicTask):
    run_every = crontab(hour=7, minute=30, day_of_week="thursday")


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


class test_crontab_parser(unittest.TestCase):

    def test_parse_star(self):
        self.assertEquals(crontab_parser(24).parse('*'), set(range(24)))
        self.assertEquals(crontab_parser(60).parse('*'), set(range(60)))
        self.assertEquals(crontab_parser(7).parse('*'), set(range(7)))

    def test_parse_range(self):
        self.assertEquals(crontab_parser(60).parse('1-10'), set(range(1,10+1)))
        self.assertEquals(crontab_parser(24).parse('0-20'), set(range(0,20+1)))
        self.assertEquals(crontab_parser().parse('2-10'), set(range(2,10+1)))

    def test_parse_groups(self):
        self.assertEquals(crontab_parser().parse('1,2,3,4'), set([1,2,3,4]))
        self.assertEquals(crontab_parser().parse('0,15,30,45'), set([0,15,30,45]))

    def test_parse_steps(self):
        self.assertEquals(crontab_parser(8).parse('*/2'), set([0,2,4,6]))
        self.assertEquals(crontab_parser().parse('*/2'), set([ i*2 for i in xrange(30) ]))
        self.assertEquals(crontab_parser().parse('*/3'), set([ i*3 for i in xrange(20) ]))

    def test_parse_composite(self):
        self.assertEquals(crontab_parser(8).parse('*/2'), set([0,2,4,6]))
        self.assertEquals(crontab_parser().parse('2-9/5'), set([5]))
        self.assertEquals(crontab_parser().parse('2-10/5'), set([5,10]))
        self.assertEquals(crontab_parser().parse('2-11/5,3'), set([3,5,10]))
        self.assertEquals(crontab_parser().parse('2-4/3,*/5,0-21/4'), set([0,3,4,5,8,10,12,15,16,20,25,30,35,40,45,50,55]))

    def test_parse_errors_on_empty_string(self):
        self.assertRaises(ParseException, crontab_parser(60).parse, '')

    def test_parse_errors_on_empty_group(self):
        self.assertRaises(ParseException, crontab_parser(60).parse, '1,,2')

    def test_parse_errors_on_empty_steps(self):
        self.assertRaises(ParseException, crontab_parser(60).parse, '*/')

    def test_parse_errors_on_negative_number(self):
        self.assertRaises(ParseException, crontab_parser(60).parse, '-20')


class test_crontab_is_due(unittest.TestCase):

    def test_default_crontab_spec(self):
        c = crontab()
        self.assertEquals(c.minute, set(range(60)))
        self.assertEquals(c.hour, set(range(24)))
        self.assertEquals(c.day_of_week, set(range(7)))

    def test_simple_crontab_spec(self):
        c = crontab(minute=30)
        self.assertEquals(c.minute, set([30]))
        self.assertEquals(c.hour, set(range(24)))
        self.assertEquals(c.day_of_week, set(range(7)))

    def test_crontab_spec_minute_formats(self):
        c = crontab(minute=30)
        self.assertEquals(c.minute, set([30]))
        c = crontab(minute='30')
        self.assertEquals(c.minute, set([30]))
        c = crontab(minute=(30,40,50))
        self.assertEquals(c.minute, set([30,40,50]))
        c = crontab(minute=set([30,40,50]))
        self.assertEquals(c.minute, set([30,40,50]))

    def test_crontab_spec_invalid_minute(self):
        self.assertRaises(ValueError, crontab, minute=60)
        self.assertRaises(ValueError, crontab, minute='0-100')

    def test_crontab_spec_hour_formats(self):
        c = crontab(hour=6)
        self.assertEquals(c.hour, set([6]))
        c = crontab(hour='5')
        self.assertEquals(c.hour, set([5]))
        c = crontab(hour=(4,8,12))
        self.assertEquals(c.hour, set([4,8,12]))

    def test_crontab_spec_invalid_hour(self):
        self.assertRaises(ValueError, crontab, hour=24)
        self.assertRaises(ValueError, crontab, hour='0-30')

    def test_crontab_spec_dow_formats(self):
        c = crontab(day_of_week=5)
        self.assertEquals(c.day_of_week, set([5]))
        c = crontab(day_of_week='5')
        self.assertEquals(c.day_of_week, set([5]))
        c = crontab(day_of_week='fri')
        self.assertEquals(c.day_of_week, set([5]))
        c = crontab(day_of_week='tuesday,sunday,fri')
        self.assertEquals(c.day_of_week, set([0,2,5]))
        c = crontab(day_of_week='mon-fri')
        self.assertEquals(c.day_of_week, set([1,2,3,4,5]))
        c = crontab(day_of_week='*/2')
        self.assertEquals(c.day_of_week, set([0,2,4,6]))

    def test_every_minute_execution_is_due(self):
        last_ran = datetime.now() - timedelta(seconds=61)
        due, remaining = EveryMinutePeriodic().is_due(last_ran)
        self.assertTrue(due)
        self.assertEquals(remaining, 1)

    def test_every_minute_execution_is_not_due(self):
        last_ran = datetime.now() - timedelta(seconds=30)
        due, remaining = EveryMinutePeriodic().is_due(last_ran)
        self.assertFalse(due)
        self.assertEquals(remaining, 1)

    @patch_crontab_nowfun(HourlyPeriodic, datetime(2010, 5, 10, 10, 30))
    def test_every_hour_execution_is_due(self):
        due, remaining = HourlyPeriodic().is_due(datetime(2010, 5, 10, 6, 30))
        self.assertTrue(due)
        self.assertEquals(remaining, 1)

    @patch_crontab_nowfun(HourlyPeriodic, datetime(2010, 5, 10, 10, 29))
    def test_every_hour_execution_is_not_due(self):
        due, remaining = HourlyPeriodic().is_due(datetime(2010, 5, 10, 6, 30))
        self.assertFalse(due)
        self.assertEquals(remaining, 1)

    @patch_crontab_nowfun(QuarterlyPeriodic, datetime(2010, 5, 10, 10, 15))
    def test_first_quarter_execution_is_due(self):
        due, remaining = QuarterlyPeriodic().is_due(datetime(2010, 5, 10, 6, 30))
        self.assertTrue(due)
        self.assertEquals(remaining, 1)

    @patch_crontab_nowfun(QuarterlyPeriodic, datetime(2010, 5, 10, 10, 30))
    def test_second_quarter_execution_is_due(self):
        due, remaining = QuarterlyPeriodic().is_due(datetime(2010, 5, 10, 6, 30))
        self.assertTrue(due)
        self.assertEquals(remaining, 1)

    @patch_crontab_nowfun(QuarterlyPeriodic, datetime(2010, 5, 10, 10, 14))
    def test_first_quarter_execution_is_not_due(self):
        due, remaining = QuarterlyPeriodic().is_due(datetime(2010, 5, 10, 6, 30))
        self.assertFalse(due)
        self.assertEquals(remaining, 1)

    @patch_crontab_nowfun(QuarterlyPeriodic, datetime(2010, 5, 10, 10, 29))
    def test_second_quarter_execution_is_not_due(self):
        due, remaining = QuarterlyPeriodic().is_due(datetime(2010, 5, 10, 6, 30))
        self.assertFalse(due)
        self.assertEquals(remaining, 1)

    @patch_crontab_nowfun(DailyPeriodic, datetime(2010, 5, 10, 7, 30))
    def test_daily_execution_is_due(self):
        due, remaining = DailyPeriodic().is_due(datetime(2010, 5, 9, 7, 30))
        self.assertTrue(due)
        self.assertEquals(remaining, 1)

    @patch_crontab_nowfun(DailyPeriodic, datetime(2010, 5, 10, 10, 30))
    def test_daily_execution_is_not_due(self):
        due, remaining = DailyPeriodic().is_due(datetime(2010, 5, 10, 6, 29))
        self.assertFalse(due)
        self.assertEquals(remaining, 1)

    @patch_crontab_nowfun(WeeklyPeriodic, datetime(2010, 5, 6, 7, 30))
    def test_weekly_execution_is_due(self):
        due, remaining = WeeklyPeriodic().is_due(datetime(2010, 4, 30, 7, 30))
        self.assertTrue(due)
        self.assertEquals(remaining, 1)

    @patch_crontab_nowfun(WeeklyPeriodic, datetime(2010, 5, 7, 10, 30))
    def test_weekly_execution_is_not_due(self):
        due, remaining = WeeklyPeriodic().is_due(datetime(2010, 4, 30, 6, 29))
        self.assertFalse(due)
        self.assertEquals(remaining, 1)
