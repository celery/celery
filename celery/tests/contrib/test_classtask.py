from __future__ import absolute_import

from celery.contrib.classtask import ClassTask
from celery.five import with_metaclass
from celery.result import EagerResult
from celery.tests.case import AppCase
from celery.utils import uuid


class ClassTasksCase(AppCase):

    def setup(self):

        self.app.set_current()

        @with_metaclass(ClassTask)
        class IncrementCounter(object):
            count = 0
            shared = False

            def __init__(self, increment_by=1):
                self.increment_by = increment_by

            def run(self):
                IncrementCounter.count += self.increment_by or 1
                return IncrementCounter.count
        self.IncrementCounter = IncrementCounter

        @with_metaclass(ClassTask)
        class Raising(object):
            shared = False
            def run(self):
                raise KeyError('foo')
        self.Raising = Raising

        @with_metaclass(ClassTask)
        class RetryTask(object):
            iterations = 0
            shared = False
            max_retries = 3

            def __init__(self, arg1, arg2, kwarg=1, max_retries=None, care=True):
                self.rmax = self.max_retries if max_retries is None else max_retries
                self.care = care
                self.arg1 = arg1
                self.arg2 = arg2
                self.kwarg = kwarg

            def run(self):
                self.get_task().iterations += 1
                assert repr(self.request)
                retries = self.request.retries
                if self.care and retries >= self.rmax:
                    return self.arg1
                else:
                    raise self.retry(countdown=0, max_retries=self.rmax)
        self.RetryTask = RetryTask

        @with_metaclass(ClassTask)
        class RetryTaskNoargs(object):
            max_retries = 3
            iterations = 0
            shared = False

            def __init__(self):
                pass

            def run(self):
                self.get_task().iterations += 1
                if self.request.retries >= 3:
                    return 42
                else:
                    raise self.retry(countdown=0)
        self.RetryTaskNoargs = RetryTaskNoargs



class test_task_retries(ClassTasksCase):

    def test_retry(self):
        self.RetryTask.max_retries = 3
        self.RetryTask.get_task().iterations = 0
        self.RetryTask(0xFF, 0xFFFF).apply()
        self.assertEqual(self.RetryTask.get_task().iterations, 4)

        self.RetryTask.max_retries = 3
        self.RetryTask.get_task().iterations = 0
        self.RetryTask(0xFF, 0xFFFF, max_retries=10).apply()
        self.assertEqual(self.RetryTask.get_task().iterations, 11)

    def test_retry_no_args(self):
        self.RetryTaskNoargs.max_retries = 3
        self.RetryTaskNoargs.get_task().iterations = 0
        self.RetryTaskNoargs().apply(propagate=True).get()
        self.assertEqual(self.RetryTaskNoargs.get_task().iterations, 4)

    def test_max_retries_exceeded_Retry(self):
        self.RetryTask.max_retries = 2
        self.RetryTask.get_task().iterations = 0
        result = self.RetryTask(*[0xFF, 0xFFFF], care=False).apply()
        with self.assertRaises(self.RetryTask.MaxRetriesExceededError):
            result.get()
        self.assertEqual(self.RetryTask.get_task().iterations, 3)

        self.RetryTask.max_retries = 1
        self.RetryTask.get_task().iterations = 0
        result = self.RetryTask(*[0xFF, 0xFFFF], **{'care': False}).apply()
        with self.assertRaises(self.RetryTask.MaxRetriesExceededError):
            result.get()
        self.assertEqual(self.RetryTask.get_task().iterations, 2)


class test_canvas_utils(ClassTasksCase):

    def test_si(self):
        self.assertTrue(self.RetryTask().si())
        self.assertTrue(self.RetryTask().si().immutable)

    def test_chunks(self):
        self.assertTrue(self.RetryTask().chunks(range(100), 10))

    def test_map(self):
        self.assertTrue(self.RetryTask().map(range(100)))

    def test_starmap(self):
        self.assertTrue(self.RetryTask().starmap(range(100)))

    def test_on_success(self):
        self.RetryTask().on_success(1, 1, (), {})


class test_tasks(ClassTasksCase):

    def test_AsyncResult(self):
        task_id = uuid()
        result = self.RetryTask().AsyncResult(task_id)
        self.assertEqual(result.backend, self.RetryTask().backend)
        self.assertEqual(result.id, task_id)


class test_apply_task(ClassTasksCase):

    def test_apply_throw(self):
        with self.assertRaises(KeyError):
            self.Raising().apply(throw=True)

    def test_apply_with_CELERY_EAGER_PROPAGATES_EXCEPTIONS(self):
        self.app.conf.CELERY_EAGER_PROPAGATES_EXCEPTIONS = True
        with self.assertRaises(KeyError):
            self.Raising().apply()

    def test_apply(self):
        self.IncrementCounter.count = 0

        e = self.IncrementCounter(2).apply()
        self.assertIsInstance(e, EagerResult)
        self.assertEqual(e.get(), 2)

        e = self.IncrementCounter().apply()
        self.assertEqual(e.get(), 3)

        e = self.IncrementCounter(increment_by=3).apply()
        self.assertEqual(e.get(), 6)

        self.assertTrue(e.successful())
        self.assertTrue(e.ready())
        self.assertTrue(repr(e).startswith('<EagerResult:'))

        f = self.Raising().apply()
        self.assertTrue(f.ready())
        self.assertFalse(f.successful())
        self.assertTrue(f.traceback)
        with self.assertRaises(KeyError):
            f.get()
