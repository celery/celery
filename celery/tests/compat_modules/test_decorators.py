from __future__ import absolute_import

import warnings

from celery.task import base

from celery.tests.utils import Case


def add(x, y):
    return x + y


class test_decorators(Case):

    def setUp(self):
        with warnings.catch_warnings(record=True):
            from celery import decorators
            self.decorators = decorators

    def assertCompatDecorator(self, decorator, type, **opts):
        task = decorator(**opts)(add)
        self.assertEqual(task(8, 8), 16)
        self.assertTrue(task.accept_magic_kwargs)
        self.assertIsInstance(task, type)

    def test_task(self):
        self.assertCompatDecorator(self.decorators.task, base.BaseTask)

    def test_periodic_task(self):
        self.assertCompatDecorator(self.decorators.periodic_task,
                                   base.BaseTask,
                                   run_every=1)
