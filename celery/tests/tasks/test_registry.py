from __future__ import absolute_import

from celery import Celery
from celery.app.registry import (
    TaskRegistry,
    _unpickle_task,
    _unpickle_task_v2,
)
from celery.task import Task, PeriodicTask
from celery.tests.case import AppCase, Case


class MockTask(Task):
    name = 'celery.unittest.test_task'

    def run(self, **kwargs):
        return True


class MockPeriodicTask(PeriodicTask):
    name = 'celery.unittest.test_periodic_task'
    run_every = 10

    def run(self, **kwargs):
        return True


class test_unpickle_task(AppCase):

    def setup(self):
        self.app = Celery(set_as_current=True)

    def test_unpickle_v1(self):
        self.app.tasks['txfoo'] = 'bar'
        self.assertEqual(_unpickle_task('txfoo'), 'bar')

    def test_unpickle_v2(self):
        self.app.tasks['txfoo1'] = 'bar1'
        self.assertEqual(_unpickle_task_v2('txfoo1'), 'bar1')
        self.assertEqual(_unpickle_task_v2('txfoo1', module='celery'), 'bar1')


class test_TaskRegistry(Case):

    def test_NotRegistered_str(self):
        self.assertTrue(repr(TaskRegistry.NotRegistered('tasks.add')))

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
        r = TaskRegistry()
        self.assertIsInstance(r, dict, 'TaskRegistry is mapping')

        self.assertRegisterUnregisterCls(r, MockTask)
        self.assertRegisterUnregisterCls(r, MockPeriodicTask)

        r.register(MockPeriodicTask)
        r.unregister(MockPeriodicTask.name)
        self.assertNotIn(MockPeriodicTask, r)
        r.register(MockPeriodicTask)

        tasks = dict(r)
        self.assertIsInstance(tasks.get(MockTask.name), MockTask)
        self.assertIsInstance(tasks.get(MockPeriodicTask.name),
                              MockPeriodicTask)

        self.assertIsInstance(r[MockTask.name], MockTask)
        self.assertIsInstance(r[MockPeriodicTask.name],
                              MockPeriodicTask)

        r.unregister(MockTask)
        self.assertNotIn(MockTask.name, r)
        r.unregister(MockPeriodicTask)
        self.assertNotIn(MockPeriodicTask.name, r)

        self.assertTrue(MockTask().run())
        self.assertTrue(MockPeriodicTask().run())

    def test_compat(self):
        r = TaskRegistry()
        r.regular()
        r.periodic()
