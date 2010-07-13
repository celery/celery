import operator
import time

from celery.task.control import broadcast

from celery.tests.functional import tasks
from celery.tests.functional.case import WorkerCase


class test_basic(WorkerCase):

    def test_started(self):
        self.assertWorkerAlive()

    def test_roundtrip_simple_task(self):
        publisher = tasks.add.get_publisher()
        results = [(tasks.add.apply_async(i, publisher=publisher), i)
                        for i in zip(xrange(100), xrange(100))]
        for result, i in results:
            self.assertEqual(result.get(timeout=10), operator.add(*i))

    def test_dump_active(self):
        tasks.sleeptask.delay(2)
        tasks.sleeptask.delay(2)
        time.sleep(0.2)
        r = broadcast("dump_active",
                           arguments={"safe": True}, reply=True)
        active = self.my_response(r)
        self.assertEqual(len(active), 2)
        self.assertEqual(active[0]["name"], tasks.sleeptask.name)
        self.assertEqual(active[0]["args"], [2])

