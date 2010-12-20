import warnings

from celery import decorators
from celery.task import base

from celery.tests.compat import catch_warnings
from celery.tests.utils import unittest
from celery.tests.utils import execute_context


def add(x, y):
    return x + y


class test_decorators(unittest.TestCase):

    def assertCompatDecorator(self, decorator, type, **opts):
        warnings.resetwarnings()

        def with_catch_warnings(log):
            return decorator(**opts)(add), log[0].message

        context = catch_warnings(record=True)
        task, w = execute_context(context, with_catch_warnings)

        self.assertEqual(task(8, 8), 16)
        self.assertTrue(task.accept_magic_kwargs)
        self.assertIsInstance(task, type)
        self.assertIsInstance(w, PendingDeprecationWarning)

    def test_task(self):
        self.assertCompatDecorator(decorators.task, base.Task)

    def test_periodic_task(self):
        self.assertCompatDecorator(decorators.periodic_task,
                                   base.PeriodicTask,
                                   run_every=1)
