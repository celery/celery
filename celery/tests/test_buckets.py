from __future__ import generators
import os
import sys
sys.path.insert(0, os.getcwd())
import time
import unittest2 as unittest
from itertools import chain, izip

from billiard.utils.functional import curry

from celery.task.base import Task
from celery.utils import gen_unique_id
from celery.worker import buckets
from celery.registry import TaskRegistry

from celery.tests.utils import skip_if_environ

skip_if_disabled = curry(skip_if_environ("SKIP_RLIMITS"))


class MockJob(object):

    def __init__(self, task_id, task_name, args, kwargs):
        self.task_id = task_id
        self.task_name = task_name
        self.args = args
        self.kwargs = kwargs

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return bool(self.task_id == other.task_id \
                    and self.task_name == other.task_name \
                    and self.args == other.args \
                    and self.kwargs == other.kwargs)
        else:
            return self == other

    def __repr__(self):
        return "<MockJob: task:%s id:%s args:%s kwargs:%s" % (
                self.task_name, self.task_id, self.args, self.kwargs)


class TestTokenBucketQueue(unittest.TestCase):

    @skip_if_disabled
    def empty_queue_yields_QueueEmpty(self):
        x = buckets.TokenBucketQueue(fill_rate=10)
        self.assertRaises(buckets.QueueEmpty, x.get)

    @skip_if_disabled
    def test_bucket__put_get(self):
        x = buckets.TokenBucketQueue(fill_rate=10)
        x.put("The quick brown fox")
        self.assertEqual(x.get(), "The quick brown fox")

        x.put_nowait("The lazy dog")
        time.sleep(0.2)
        self.assertEqual(x.get_nowait(), "The lazy dog")

    @skip_if_disabled
    def test_fill_rate(self):
        x = buckets.TokenBucketQueue(fill_rate=10)
        # 20 items should take at least one second to complete
        time_start = time.time()
        [x.put(str(i)) for i in xrange(20)]
        for i in xrange(20):
            sys.stderr.write("x")
            x.wait()
        self.assertGreater(time.time() - time_start, 1.5)

    @skip_if_disabled
    def test_can_consume(self):
        x = buckets.TokenBucketQueue(fill_rate=1)
        x.put("The quick brown fox")
        self.assertEqual(x.get(), "The quick brown fox")
        time.sleep(0.1)
        # Not yet ready for another token
        x.put("The lazy dog")
        self.assertRaises(x.RateLimitExceeded, x.get)

    @skip_if_disabled
    def test_expected_time(self):
        x = buckets.TokenBucketQueue(fill_rate=1)
        x.put_nowait("The quick brown fox")
        self.assertEqual(x.get_nowait(), "The quick brown fox")
        self.assertTrue(x.expected_time())

    @skip_if_disabled
    def test_qsize(self):
        x = buckets.TokenBucketQueue(fill_rate=1)
        x.put("The quick brown fox")
        self.assertEqual(x.qsize(), 1)
        self.assertEqual(x.get_nowait(), "The quick brown fox")


class TestRateLimitString(unittest.TestCase):

    @skip_if_disabled
    def test_conversion(self):
        self.assertEqual(buckets.parse_ratelimit_string(999), 999)
        self.assertEqual(buckets.parse_ratelimit_string("1456/s"), 1456)
        self.assertEqual(buckets.parse_ratelimit_string("100/m"),
                          100 / 60.0)
        self.assertEqual(buckets.parse_ratelimit_string("10/h"),
                          10 / 60.0 / 60.0)

        for zero in (0, None, "0", "0/m", "0/h", "0/s"):
            self.assertEqual(buckets.parse_ratelimit_string(zero), 0)


class TaskA(Task):
    rate_limit = 10


class TaskB(Task):
    rate_limit = None


class TaskC(Task):
    rate_limit = "1/s"


class TaskD(Task):
    rate_limit = "1000/m"


class TestTaskBuckets(unittest.TestCase):

    def setUp(self):
        self.registry = TaskRegistry()
        self.task_classes = (TaskA, TaskB, TaskC)
        for task_cls in self.task_classes:
            self.registry.register(task_cls)

    @skip_if_disabled
    def test_auto_add_on_missing(self):
        b = buckets.TaskBucket(task_registry=self.registry)
        for task_cls in self.task_classes:
            self.assertIn(task_cls.name, b.buckets.keys())
        self.registry.register(TaskD)
        self.assertTrue(b.get_bucket_for_type(TaskD.name))
        self.assertIn(TaskD.name, b.buckets.keys())
        self.registry.unregister(TaskD)

    @skip_if_disabled
    def test_has_rate_limits(self):
        b = buckets.TaskBucket(task_registry=self.registry)
        self.assertEqual(b.buckets[TaskA.name].fill_rate, 10)
        self.assertIsInstance(b.buckets[TaskB.name], buckets.Queue)
        self.assertEqual(b.buckets[TaskC.name].fill_rate, 1)
        self.registry.register(TaskD)
        b.init_with_registry()
        try:
            self.assertEqual(b.buckets[TaskD.name].fill_rate, 1000 / 60.0)
        finally:
            self.registry.unregister(TaskD)

    @skip_if_disabled
    def test_on_empty_buckets__get_raises_empty(self):
        b = buckets.TaskBucket(task_registry=self.registry)
        self.assertRaises(buckets.QueueEmpty, b.get)
        self.assertEqual(b.qsize(), 0)

    @skip_if_disabled
    def test_put__get(self):
        b = buckets.TaskBucket(task_registry=self.registry)
        job = MockJob(gen_unique_id(), TaskA.name, ["theqbf"], {"foo": "bar"})
        b.put(job)
        self.assertEqual(b.get(), job)

    @skip_if_disabled
    def test_fill_rate(self):
        b = buckets.TaskBucket(task_registry=self.registry)

        cjob = lambda i: MockJob(gen_unique_id(), TaskA.name, [i], {})
        jobs = [cjob(i) for i in xrange(20)]
        [b.put(job) for job in jobs]

        self.assertEqual(b.qsize(), 20)

        # 20 items should take at least one second to complete
        time_start = time.time()
        for i, job in enumerate(jobs):
            sys.stderr.write("i")
            self.assertEqual(b.get(), job)
        self.assertGreater(time.time() - time_start, 1.5)

    @skip_if_disabled
    def test__very_busy_queue_doesnt_block_others(self):
        b = buckets.TaskBucket(task_registry=self.registry)

        cjob = lambda i, t: MockJob(gen_unique_id(), t.name, [i], {})
        ajobs = [cjob(i, TaskA) for i in xrange(10)]
        bjobs = [cjob(i, TaskB) for i in xrange(20)]
        jobs = list(chain(*izip(bjobs, ajobs)))
        map(b.put, jobs)

        got_ajobs = 0
        for job in (b.get() for i in xrange(20)):
            if job.task_name == TaskA.name:
                got_ajobs += 1

        self.assertGreater(got_ajobs, 2)

    @skip_if_disabled
    def test_thorough__multiple_types(self):
        self.registry.register(TaskD)
        try:
            b = buckets.TaskBucket(task_registry=self.registry)

            cjob = lambda i, t: MockJob(gen_unique_id(), t.name, [i], {})

            ajobs = [cjob(i, TaskA) for i in xrange(10)]
            bjobs = [cjob(i, TaskB) for i in xrange(10)]
            cjobs = [cjob(i, TaskC) for i in xrange(10)]
            djobs = [cjob(i, TaskD) for i in xrange(10)]

            # Spread the jobs around.
            jobs = list(chain(*izip(ajobs, bjobs, cjobs, djobs)))

            [b.put(job) for job in jobs]
            for i, job in enumerate(jobs):
                sys.stderr.write("0")
                self.assertTrue(b.get(), job)
            self.assertEqual(i+1, len(jobs))
        finally:
            self.registry.unregister(TaskD)

if __name__ == "__main__":
    unittest.main()
