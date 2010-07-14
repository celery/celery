import operator
import time

from celery.tests.functional import tasks
from celery.tests.functional.case import WorkerCase

from celery.task.control import broadcast

class test_basic(WorkerCase):

    def test_started(self):
        self.assertWorkerAlive()

    def test_roundtrip_simple_task(self):
        publisher = tasks.add.get_publisher()
        results = [(tasks.add.apply_async(i, publisher=publisher), i)
                        for i in zip(xrange(100), xrange(100))]
        for result, i in results:
            self.assertEqual(result.get(timeout=10), operator.add(*i))

    def test_dump_active(self, sleep=1):
        r1 = tasks.sleeptask.delay(sleep)
        r2 = tasks.sleeptask.delay(sleep)
        self.ensure_accepted(r1.task_id)
        active = self.inspect().active(safe=True)
        self.assertEqual(len(active), 2)
        self.assertEqual(active[0]["name"], tasks.sleeptask.name)
        self.assertEqual(active[0]["args"], [sleep])

    def test_dump_reserved(self, sleep=1):
        r1 = tasks.sleeptask.delay(sleep)
        r2 = tasks.sleeptask.delay(sleep)
        r3 = tasks.sleeptask.delay(sleep)
        r4 = tasks.sleeptask.delay(sleep)
        self.ensure_accepted(r1.task_id)
        reserved = self.inspect().reserved(safe=True)
        self.assertTrue(reserved)
        self.assertEqual(reserved[0]["name"], tasks.sleeptask.name)
        self.assertEqual(reserved[0]["args"], [sleep])

    def test_dump_schedule(self, countdown=1):
        r1 = tasks.add.apply_async((2, 2), countdown=countdown)
        r2 = tasks.add.apply_async((2, 2), countdown=countdown)
        self.ensure_scheduled(r1.task_id, interval=0.1)
        schedule = self.inspect().scheduled(safe=True)
        self.assertTrue(schedule)
        self.assertTrue(len(schedule), 2)
        self.assertEqual(schedule[0]["request"]["name"], tasks.add.name)
        self.assertEqual(schedule[0]["request"]["args"], [2, 2])

