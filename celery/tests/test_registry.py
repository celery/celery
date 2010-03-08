import unittest

from celery import registry
from celery.task import Task, PeriodicTask


class TestTask(Task):
    name = "celery.unittest.test_task"

    def run(self, **kwargs):
        return True


class TestPeriodicTask(PeriodicTask):
    name = "celery.unittest.test_periodic_task"
    run_every = 10

    def run(self, **kwargs):
        return True


class TestTaskRegistry(unittest.TestCase):

    def assertRegisterUnregisterCls(self, r, task):
        self.assertRaises(r.NotRegistered, r.unregister, task)
        r.register(task)
        self.assertTrue(task.name in r)

    def assertRegisterUnregisterFunc(self, r, task, task_name):
        self.assertRaises(r.NotRegistered, r.unregister, task_name)
        r.register(task, task_name)
        self.assertTrue(task_name in r)

    def test_task_registry(self):
        r = registry.TaskRegistry()
        self.assertTrue(isinstance(r.data, dict),
                "TaskRegistry has composited dict")

        self.assertRegisterUnregisterCls(r, TestTask)
        self.assertRegisterUnregisterCls(r, TestPeriodicTask)

        tasks = dict(r)
        self.assertTrue(isinstance(tasks.get(TestTask.name), TestTask))
        self.assertTrue(isinstance(tasks.get(TestPeriodicTask.name),
                                   TestPeriodicTask))

        regular = r.regular()
        self.assertTrue(TestTask.name in regular)
        self.assertFalse(TestPeriodicTask.name in regular)

        periodic = r.periodic()
        self.assertFalse(TestTask.name in periodic)
        self.assertTrue(TestPeriodicTask.name in periodic)

        self.assertTrue(isinstance(r[TestTask.name], TestTask))
        self.assertTrue(isinstance(r[TestPeriodicTask.name],
                                   TestPeriodicTask))

        r.unregister(TestTask)
        self.assertFalse(TestTask.name in r)
        r.unregister(TestPeriodicTask)
        self.assertFalse(TestPeriodicTask.name in r)

        self.assertTrue(TestTask().run())
        self.assertTrue(TestPeriodicTask().run())
