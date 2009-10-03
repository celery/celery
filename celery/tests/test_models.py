import unittest
from datetime import datetime, timedelta
from celery.models import TaskMeta, TaskSetMeta, PeriodicTaskMeta
from celery.task import PeriodicTask
from celery.registry import tasks
from celery.utils import gen_unique_id


class TestPeriodicTask(PeriodicTask):
    name = "celery.unittest.test_models.test_periodic_task"
    run_every = timedelta(minutes=30)


class TestModels(unittest.TestCase):

    def createTaskMeta(self):
        id = gen_unique_id()
        taskmeta, created = TaskMeta.objects.get_or_create(task_id=id)
        return taskmeta

    def createTaskSetMeta(self):
        id = gen_unique_id()
        tasksetmeta, created = TaskSetMeta.objects.get_or_create(taskset_id=id)
        return tasksetmeta

    def createPeriodicTaskMeta(self, name):
        ptaskmeta, created = PeriodicTaskMeta.objects.get_or_create(name=name,
                defaults={"last_run_at": datetime.now()})
        return ptaskmeta

    def test_taskmeta(self):
        m1 = self.createTaskMeta()
        m2 = self.createTaskMeta()
        m3 = self.createTaskMeta()
        self.assertTrue(unicode(m1).startswith("<Task:"))
        self.assertTrue(m1.task_id)
        self.assertTrue(isinstance(m1.date_done, datetime))

        self.assertEquals(TaskMeta.objects.get_task(m1.task_id).task_id,
                m1.task_id)
        self.assertFalse(TaskMeta.objects.is_done(m1.task_id))
        TaskMeta.objects.store_result(m1.task_id, True, status="DONE")
        TaskMeta.objects.store_result(m2.task_id, True, status="DONE")
        self.assertTrue(TaskMeta.objects.is_done(m1.task_id))
        self.assertTrue(TaskMeta.objects.is_done(m2.task_id))

        # Have to avoid save() because it applies the auto_now=True.
        TaskMeta.objects.filter(task_id=m1.task_id).update(
                date_done=datetime.now() - timedelta(days=10))

        expired = TaskMeta.objects.get_all_expired()
        self.assertTrue(m1 in expired)
        self.assertFalse(m2 in expired)
        self.assertFalse(m3 in expired)

        TaskMeta.objects.delete_expired()
        self.assertFalse(m1 in TaskMeta.objects.all())
    
    def test_tasksetmeta(self):
        m1 = self.createTaskSetMeta()
        m2 = self.createTaskSetMeta()
        m3 = self.createTaskSetMeta()
        self.assertTrue(unicode(m1).startswith("<TaskSet:"))
        self.assertTrue(m1.taskset_id)
        self.assertTrue(isinstance(m1.date_done, datetime))

        self.assertEquals(TaskSetMeta.objects.get_result(m1.taskset_id).taskset_id,
                m1.taskset_id)

        # Have to avoid save() because it applies the auto_now=True.
        TaskSetMeta.objects.filter(taskset_id=m1.taskset_id).update(
                date_done=datetime.now() - timedelta(days=10))

        expired = TaskSetMeta.objects.get_all_expired()
        self.assertTrue(m1 in expired)
        self.assertFalse(m2 in expired)
        self.assertFalse(m3 in expired)

        TaskSetMeta.objects.delete_expired()
        self.assertFalse(m1 in TaskSetMeta.objects.all())

    def test_periodic_taskmeta(self):
        tasks.register(TestPeriodicTask)
        p = self.createPeriodicTaskMeta(TestPeriodicTask.name)
        # check that repr works.
        self.assertTrue(unicode(p).startswith("<PeriodicTask:"))
        self.assertFalse(p in PeriodicTaskMeta.objects.get_waiting_tasks())
        p.last_run_at = datetime.now() - (TestPeriodicTask.run_every +
                timedelta(seconds=10))
        p.save()
        self.assertTrue(p in PeriodicTaskMeta.objects.get_waiting_tasks())
        self.assertTrue(isinstance(p.task, TestPeriodicTask))

        p.delay()
