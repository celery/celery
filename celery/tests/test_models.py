import unittest
import uuid
from datetime import datetime, timedelta
from celery.models import TaskMeta, PeriodicTaskMeta
from celery.task import PeriodicTask
from celery.registry import tasks


class TestPeriodicTask(PeriodicTask):
    name = "celery.unittest.test_models.test_periodic_task"
    run_every = timedelta(days=1)


class TestModels(unittest.TestCase):

    def createTaskMeta(self):
        id = str(uuid.uuid4())
        taskmeta, created = TaskMeta.objects.get_or_create(task_id=id)
        return taskmeta

    def createPeriodicTaskMeta(self, name):
        ptaskmeta, created = PeriodicTaskMeta.objects.get_or_create(name=name)
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
        TaskMeta.objects.mark_as_done(m1.task_id)
        TaskMeta.objects.mark_as_done(m2.task_id)
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
       
    def test_periodic_taskmeta(self):
        tasks.register(TestPeriodicTask)
        p = self.createPeriodicTaskMeta(TestPeriodicTask.name)
        # check that repr works.
        self.assertTrue(unicode(p).startswith("<PeriodicTask:"))
        self.assertFalse(p in PeriodicTaskMeta.objects.get_waiting_tasks())
        # Have to avoid save() because it applies the auto_now=True.
        PeriodicTaskMeta.objects.filter(name=p.name).update(
                last_run_at=datetime.now() - TestPeriodicTask.run_every)
        self.assertTrue(p in PeriodicTaskMeta.objects.get_waiting_tasks())
        self.assertTrue(isinstance(p.task, TestPeriodicTask))

        p.delay()

