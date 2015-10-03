from __future__ import absolute_import

import operator

# funtest config
import suite  # noqa

from celery.five import range
from celery.tests.case import unittest
from celery.tests.functional import tasks
from celery.tests.functional.case import WorkerCase


class test_basic(WorkerCase):

    def test_started(self):
        self.assertWorkerAlive()

    def test_roundtrip_simple_task(self):
        publisher = tasks.add.get_publisher()
        results = [(tasks.add.apply_async(i, publisher=publisher), i)
                   for i in zip(range(100), range(100))]
        for result, i in results:
            self.assertEqual(result.get(timeout=10), operator.add(*i))

    def test_dump_active(self, sleep=1):
        r1 = tasks.sleeptask.delay(sleep)
        tasks.sleeptask.delay(sleep)
        self.ensure_accepted(r1.id)
        active = self.inspect().active(safe=True)
        self.assertTrue(active)
        active = active[self.worker.hostname]
        self.assertEqual(len(active), 2)
        self.assertEqual(active[0]['name'], tasks.sleeptask.name)
        self.assertEqual(active[0]['args'], [sleep])

    def test_dump_reserved(self, sleep=1):
        r1 = tasks.sleeptask.delay(sleep)
        tasks.sleeptask.delay(sleep)
        tasks.sleeptask.delay(sleep)
        tasks.sleeptask.delay(sleep)
        self.ensure_accepted(r1.id)
        reserved = self.inspect().reserved(safe=True)
        self.assertTrue(reserved)
        reserved = reserved[self.worker.hostname]
        self.assertEqual(reserved[0]['name'], tasks.sleeptask.name)
        self.assertEqual(reserved[0]['args'], [sleep])

    def test_dump_schedule(self, countdown=1):
        r1 = tasks.add.apply_async((2, 2), countdown=countdown)
        tasks.add.apply_async((2, 2), countdown=countdown)
        self.ensure_scheduled(r1.id, interval=0.1)
        schedule = self.inspect().scheduled(safe=True)
        self.assertTrue(schedule)
        schedule = schedule[self.worker.hostname]
        self.assertTrue(len(schedule), 2)
        self.assertEqual(schedule[0]['request']['name'], tasks.add.name)
        self.assertEqual(schedule[0]['request']['args'], [2, 2])


if __name__ == '__main__':
    unittest.main()
