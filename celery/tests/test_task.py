import unittest
from StringIO import StringIO

from celery import task
from celery import registry
from celery import messaging
from celery.result import EagerResult
from celery.backends import default_backend
from datetime import datetime, timedelta
from celery.decorators import task as task_dec
from celery.worker.listener import parse_iso8601

def return_True(*args, **kwargs):
    # Task run functions can't be closures/lambdas, as they're pickled.
    return True


return_True_task = task_dec()(return_True)


def raise_exception(self, **kwargs):
    raise Exception("%s error" % self.__class__)


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
        self.assertEquals(result.get(), 0xFF)
        self.assertEquals(RetryTask.iterations, 4)

    def test_retry_with_kwargs(self):
        RetryTaskCustomExc.max_retries = 3
        RetryTaskCustomExc.iterations = 0
        result = RetryTaskCustomExc.apply([0xFF, 0xFFFF], {"kwarg": 0xF})
        self.assertEquals(result.get(), 0xFF + 0xF)
        self.assertEquals(RetryTaskCustomExc.iterations, 4)

    def test_retry_with_custom_exception(self):
        RetryTaskCustomExc.max_retries = 2
        RetryTaskCustomExc.iterations = 0
        result = RetryTaskCustomExc.apply([0xFF, 0xFFFF], {"kwarg": 0xF})
        self.assertRaises(MyCustomException,
                          result.get)
        self.assertEquals(RetryTaskCustomExc.iterations, 3)

    def test_max_retries_exceeded(self):
        RetryTask.max_retries = 2
        RetryTask.iterations = 0
        result = RetryTask.apply([0xFF, 0xFFFF])
        self.assertRaises(RetryTask.MaxRetriesExceededError,
                          result.get)
        self.assertEquals(RetryTask.iterations, 3)

        RetryTask.max_retries = 1
        RetryTask.iterations = 0
        result = RetryTask.apply([0xFF, 0xFFFF])
        self.assertRaises(RetryTask.MaxRetriesExceededError,
                          result.get)
        self.assertEquals(RetryTask.iterations, 2)


class TestCeleryTasks(unittest.TestCase):

    def createTaskCls(self, cls_name, task_name=None):
        attrs = {"__module__": self.__module__}
        if task_name:
            attrs["name"] = task_name

        cls = type(cls_name, (task.Task, ), attrs)
        cls.run = return_True
        return cls

    def test_ping(self):
        from celery import conf
        conf.ALWAYS_EAGER = True
        self.assertEquals(task.ping(), 'pong')
        conf.ALWAYS_EAGER = False

    def test_execute_remote(self):
        from celery import conf
        conf.ALWAYS_EAGER = True
        self.assertEquals(task.execute_remote(return_True, ["foo"]).get(),
                          True)
        conf.ALWAYS_EAGER = False

    def test_dmap(self):
        from celery import conf
        import operator
        conf.ALWAYS_EAGER = True
        res = task.dmap(operator.add, zip(xrange(10), xrange(10)))
        self.assertTrue(res, sum([operator.add(x, x)
                                    for x in xrange(10)]))
        conf.ALWAYS_EAGER = False

    def test_dmap_async(self):
        from celery import conf
        import operator
        conf.ALWAYS_EAGER = True
        res = task.dmap_async(operator.add, zip(xrange(10), xrange(10)))
        self.assertTrue(res.get(), sum([operator.add(x, x)
                                            for x in xrange(10)]))
        conf.ALWAYS_EAGER = False

    def assertNextTaskDataEquals(self, consumer, presult, task_name,
            test_eta=False, **kwargs):
        next_task = consumer.fetch()
        task_data = next_task.decode()
        self.assertEquals(task_data["id"], presult.task_id)
        self.assertEquals(task_data["task"], task_name)
        task_kwargs = task_data.get("kwargs", {})
        if test_eta:
            self.assertTrue(isinstance(task_data.get("eta"), basestring))
            to_datetime = parse_iso8601(task_data.get("eta"))
            self.assertTrue(isinstance(to_datetime, datetime))
        for arg_name, arg_value in kwargs.items():
            self.assertEquals(task_kwargs.get(arg_name), arg_value)

    def test_incomplete_task_cls(self):

        class IncompleteTask(task.Task):
            name = "c.unittest.t.itask"

        self.assertRaises(NotImplementedError, IncompleteTask().run)

    def test_regular_task(self):
        T1 = self.createTaskCls("T1", "c.unittest.t.t1")
        self.assertTrue(isinstance(T1(), T1))
        self.assertTrue(T1().run())
        self.assertTrue(callable(T1()),
                "Task class is callable()")
        self.assertTrue(T1()(),
                "Task class runs run() when called")

        # task name generated out of class module + name.
        T2 = self.createTaskCls("T2")
        self.assertEquals(T2().name, "celery.tests.test_task.T2")

        t1 = T1()
        consumer = t1.get_consumer()
        self.assertRaises(NotImplementedError, consumer.receive, "foo", "foo")
        consumer.discard_all()
        self.assertTrue(consumer.fetch() is None)

        # Without arguments.
        presult = t1.delay()
        self.assertNextTaskDataEquals(consumer, presult, t1.name)

        # With arguments.
        presult2 = t1.apply_async(kwargs=dict(name="George Constanza"))
        self.assertNextTaskDataEquals(consumer, presult2, t1.name,
                name="George Constanza")

        # With eta.
        presult2 = task.apply_async(t1, kwargs=dict(name="George Constanza"),
                                    eta=datetime.now() + timedelta(days=1))
        self.assertNextTaskDataEquals(consumer, presult2, t1.name,
                name="George Constanza", test_eta=True)

        # With countdown.
        presult2 = task.apply_async(t1, kwargs=dict(name="George Constanza"),
                                    countdown=10)
        self.assertNextTaskDataEquals(consumer, presult2, t1.name,
                name="George Constanza", test_eta=True)

        # Discarding all tasks.
        task.discard_all()
        tid3 = task.apply_async(t1)
        self.assertEquals(task.discard_all(), 1)
        self.assertTrue(consumer.fetch() is None)

        self.assertFalse(task.is_successful(presult.task_id))
        self.assertFalse(presult.successful())
        default_backend.mark_as_done(presult.task_id, result=None)
        self.assertTrue(task.is_successful(presult.task_id))
        self.assertTrue(presult.successful())


        publisher = t1.get_publisher()
        self.assertTrue(isinstance(publisher, messaging.TaskPublisher))

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
            [[1], {}], [[2], {}], [[3], {}], [[4], {}], [[5], {}]])
        res = ts.run()
        self.assertEquals(res.join(), [True, True, True, True, True])

        conf.ALWAYS_EAGER = False

    def test_counter_taskset(self):
        IncrementCounterTask.count = 0
        ts = task.TaskSet(IncrementCounterTask, [
            [[], {}],
            [[], {"increment_by": 2}],
            [[], {"increment_by": 3}],
            [[], {"increment_by": 4}],
            [[], {"increment_by": 5}],
            [[], {"increment_by": 6}],
            [[], {"increment_by": 7}],
            [[], {"increment_by": 8}],
            [[], {"increment_by": 9}],
        ])
        self.assertEquals(ts.task_name, IncrementCounterTask.name)
        self.assertEquals(ts.total, 9)


        consumer = IncrementCounterTask().get_consumer()
        consumer.discard_all()
        taskset_res = ts.run()
        subtasks = taskset_res.subtasks
        taskset_id = taskset_res.taskset_id
        for subtask in subtasks:
            m = consumer.fetch().payload
            self.assertEquals(m.get("taskset"), taskset_id)
            self.assertEquals(m.get("task"), IncrementCounterTask.name)
            self.assertEquals(m.get("id"), subtask.task_id)
            IncrementCounterTask().run(
                    increment_by=m.get("kwargs", {}).get("increment_by"))
        self.assertEquals(IncrementCounterTask.count, sum(xrange(1, 10)))


class TestTaskApply(unittest.TestCase):

    def test_apply(self):
        IncrementCounterTask.count = 0

        e = IncrementCounterTask.apply()
        self.assertTrue(isinstance(e, EagerResult))
        self.assertEquals(e.get(), 1)

        e = IncrementCounterTask.apply(args=[1])
        self.assertEquals(e.get(), 2)

        e = IncrementCounterTask.apply(kwargs={"increment_by": 4})
        self.assertEquals(e.get(), 6)

        self.assertTrue(e.successful())
        self.assertTrue(e.ready())
        self.assertTrue(repr(e).startswith("<EagerResult:"))

        f = RaisingTask.apply()
        self.assertTrue(f.ready())
        self.assertFalse(f.successful())
        self.assertTrue(f.traceback)
        self.assertRaises(KeyError, f.get)
