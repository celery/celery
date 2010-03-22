import unittest2 as unittest
from StringIO import StringIO
from datetime import datetime, timedelta

from celery import task
from celery import messaging
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

    def test_is_due_not_due(self):
        due, remaining = MyPeriodic().is_due(datetime.now())
        self.assertFalse(due)
        self.assertGreater(remaining, 60)

    def test_is_due(self):
        p = MyPeriodic()
        due, remaining = p.is_due(datetime.now() - p.run_every)
        self.assertTrue(due)
        self.assertEqual(remaining, p.timedelta_seconds(p.run_every))
