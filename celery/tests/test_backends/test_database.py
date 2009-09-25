import unittest
from celery.backends.database import Backend
from celery.utils import gen_unique_id
from celery.task import PeriodicTask
from celery import registry
from celery.models import PeriodicTaskMeta
from datetime import datetime, timedelta


class SomeClass(object):

    def __init__(self, data):
        self.data = data


class MyPeriodicTask(PeriodicTask):
    name = "c.u.my-periodic-task-244"
    run_every = timedelta(seconds=1)

    def run(self, **kwargs):
        return 42


class TestDatabaseBackend(unittest.TestCase):

    def test_run_periodic_tasks(self):
        #obj, created = PeriodicTaskMeta.objects.get_or_create(
        #                    name=MyPeriodicTask.name,
        #                    defaults={"last_run_at": datetime.now() -
        #                        timedelta(days=-4)})
        #if not created:
        #    obj.last_run_at = datetime.now() - timedelta(days=4)
        #    obj.save()
        b = Backend()
        b.run_periodic_tasks()

    def test_backend(self):
        b = Backend()
        tid = gen_unique_id()

        self.assertFalse(b.is_done(tid))
        self.assertEquals(b.get_status(tid), "PENDING")
        self.assertTrue(b.get_result(tid) is None)

        b.mark_as_done(tid, 42)
        self.assertTrue(b.is_done(tid))
        self.assertEquals(b.get_status(tid), "DONE")
        self.assertEquals(b.get_result(tid), 42)
        self.assertTrue(b._cache.get(tid))
        self.assertTrue(b.get_result(tid), 42)

        tid2 = gen_unique_id()
        result = {"foo": "baz", "bar": SomeClass(12345)}
        b.mark_as_done(tid2, result)
        # is serialized properly.
        rindb = b.get_result(tid2)
        self.assertEquals(rindb.get("foo"), "baz")
        self.assertEquals(rindb.get("bar").data, 12345)

        tid3 = gen_unique_id()
        try:
            raise KeyError("foo")
        except KeyError, exception:
            pass
        b.mark_as_failure(tid3, exception)
        self.assertFalse(b.is_done(tid3))
        self.assertEquals(b.get_status(tid3), "FAILURE")
        self.assertTrue(isinstance(b.get_result(tid3), KeyError))
