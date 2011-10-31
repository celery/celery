from __future__ import absolute_import
from __future__ import with_statement

import warnings

from celery.task import base

from celery.tests.compat import catch_warnings
from celery.tests.utils import unittest


def add(x, y):
    return x + y


class test_decorators(unittest.TestCase):

    def setUp(self):
        warnings.resetwarnings()

        with catch_warnings(record=True):
            from celery import decorators
            self.decorators = decorators

    def assertCompatDecorator(self, decorator, type, **opts):
        task = decorator(**opts)(add)
        self.assertEqual(task(8, 8), 16)
        self.assertTrue(task.accept_magic_kwargs)
        self.assertIsInstance(task, type)

    def test_task(self):
        self.assertCompatDecorator(self.decorators.task, base.Task)

    def test_periodic_task(self):
        self.assertCompatDecorator(self.decorators.periodic_task,
                                   base.PeriodicTask,
                                   run_every=1)
