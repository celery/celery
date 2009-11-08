import unittest
from datetime import datetime, timedelta
from celery.models import TaskMeta
from celery.utils import gen_unique_id


class TestModels(unittest.TestCase):

    def createTaskMeta(self):
        id = gen_unique_id()
        taskmeta, created = TaskMeta.objects.get_or_create(task_id=id)
        return taskmeta

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
