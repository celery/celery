import unittest
import uuid
import logging
from StringIO import StringIO

from celery import task
from celery import registry
from celery.log import setup_logger
from celery import messaging
from celery.backends import default_backend


def return_True(self, **kwargs):
    # Task run functions can't be closures/lambdas, as they're pickled.
    return True


def raise_exception(self, **kwargs):
    raise Exception("%s error" % self.__class__)


class IncrementCounterTask(task.Task):
    name = "c.unittest.increment_counter_task"
    count = 0

    def run(self, increment_by, **kwargs):
        increment_by = increment_by or 1
        self.__class__.count += increment_by


class TestCeleryTasks(unittest.TestCase):

    def createTaskCls(self, cls_name, task_name=None):
        attrs = {}
        if task_name:
            attrs["name"] = task_name
        cls = type(cls_name, (task.Task, ), attrs)
        cls.run = return_True
        return cls

    def assertNextTaskDataEquals(self, consumer, presult, task_name,
            **kwargs):
        next_task = consumer.fetch()
        task_data = consumer.decoder(next_task.body)
        self.assertEquals(task_data["id"], presult.task_id)
        self.assertEquals(task_data["task"], task_name)
        task_kwargs = task_data.get("kwargs", {})
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

        # task without name raises NotImplementedError
        T2 = self.createTaskCls("T2")
        self.assertRaises(NotImplementedError, T2)

        registry.tasks.register(T1)
        t1 = T1()
        consumer = t1.get_consumer()
        self.assertRaises(NotImplementedError, consumer.receive, "foo", "foo")
        consumer.discard_all()
        self.assertTrue(consumer.fetch() is None)

        # Without arguments.
        presult = t1.delay()
        self.assertNextTaskDataEquals(consumer, presult, t1.name)

        # With arguments.
        presult2 = task.delay_task(t1.name, name="George Constanza")
        self.assertNextTaskDataEquals(consumer, presult2, t1.name,
                name="George Constanza")

        self.assertRaises(registry.tasks.NotRegistered, task.delay_task,
                "some.task.that.should.never.exist.X.X.X.X.X")

        # Discarding all tasks.
        task.discard_all()
        tid3 = task.delay_task(t1.name)
        self.assertEquals(task.discard_all(), 1)
        self.assertTrue(consumer.fetch() is None)

        self.assertFalse(task.is_done(presult.task_id))
        self.assertFalse(presult.is_done())
        default_backend.mark_as_done(presult.task_id, result=None)
        self.assertTrue(task.is_done(presult.task_id))
        self.assertTrue(presult.is_done())


        publisher = t1.get_publisher()
        self.assertTrue(isinstance(publisher, messaging.TaskPublisher))


class TestTaskSet(unittest.TestCase):

    def test_counter_taskset(self):
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

        taskset_res = ts.run()
        subtask_ids = taskset_res.subtask_ids
        taskset_id = taskset_res.taskset_id

        consumer = IncrementCounterTask().get_consumer()
        for subtask_id in subtask_ids:
            m = consumer.decoder(consumer.fetch().body)
            self.assertEquals(m.get("taskset"), taskset_id)
            self.assertEquals(m.get("task"), IncrementCounterTask.name)
            self.assertEquals(m.get("id"), subtask_id)
            IncrementCounterTask().run(
                    increment_by=m.get("kwargs", {}).get("increment_by"))
        self.assertEquals(IncrementCounterTask.count, sum(xrange(1, 10)))
