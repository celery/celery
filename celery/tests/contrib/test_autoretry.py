from __future__ import absolute_import

from celery.contrib.autoretry import autoretry

from celery.tests.case import AppCase


class TasksCase(AppCase):

    def setup(self):

        @autoretry(on=(ZeroDivisionError,))
        @self.app.task(shared=False)
        def autoretry_task_no_kwargs(a, b):
            self.iterations += 1
            return a/b
        self.autoretry_task_no_kwargs = autoretry_task_no_kwargs

        @autoretry(on=(ZeroDivisionError,), retry_kwargs={'max_retries': 5})
        @self.app.task(shared=False)
        def autoretry_task(a, b):
            self.iterations += 1
            return a/b
        self.autoretry_task = autoretry_task


class test_autoretry(TasksCase):

    def test_autoretry_no_kwargs(self):
        self.autoretry_task_no_kwargs.max_retries = 3
        self.autoretry_task_no_kwargs.iterations = 0
        self.autoretry_task_no_kwargs.apply((1, 0))
        self.assertEqual(self.autoretry_task_no_kwargs.iterations, 4)

    def test_autoretry(self):
        self.autoretry_task_no_kwargs.max_retries = 3
        self.autoretry_task_no_kwargs.iterations = 0
        self.autoretry_task_no_kwargs.apply((1, 0))
        self.assertEqual(self.autoretry_task_no_kwargs.iterations, 6)
