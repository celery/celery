from __future__ import absolute_import
from __future__ import with_statement

from celery import registry
from celery.task import Task, PeriodicTask
from celery.tests.utils import unittest


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
        with self.assertRaises(r.NotRegistered):
            r.unregister(task)
        r.register(task)
        self.assertIn(task.name, r)

    def assertRegisterUnregisterFunc(self, r, task, task_name):
        with self.assertRaises(r.NotRegistered):
            r.unregister(task_name)
        r.register(task, task_name)
        self.assertIn(task_name, r)

    def test_task_registry(self):
        r = registry.TaskRegistry()
        self.assertIsInstance(r, dict,
                "TaskRegistry is mapping")

        self.assertRegisterUnregisterCls(r, TestTask)
        self.assertRegisterUnregisterCls(r, TestPeriodicTask)

        r.register(TestPeriodicTask)
        r.unregister(TestPeriodicTask.name)
        self.assertNotIn(TestPeriodicTask, r)
        r.register(TestPeriodicTask)

        tasks = dict(r)
        self.assertIsInstance(tasks.get(TestTask.name), TestTask)
        self.assertIsInstance(tasks.get(TestPeriodicTask.name),
                                   TestPeriodicTask)

        regular = r.regular()
        self.assertIn(TestTask.name, regular)
        self.assertNotIn(TestPeriodicTask.name, regular)

        periodic = r.periodic()
        self.assertNotIn(TestTask.name, periodic)
        self.assertIn(TestPeriodicTask.name, periodic)

        self.assertIsInstance(r[TestTask.name], TestTask)
        self.assertIsInstance(r[TestPeriodicTask.name],
                                   TestPeriodicTask)

        r.unregister(TestTask)
        self.assertNotIn(TestTask.name, r)
        r.unregister(TestPeriodicTask)
        self.assertNotIn(TestPeriodicTask.name, r)

        self.assertTrue(TestTask().run())
        self.assertTrue(TestPeriodicTask().run())
