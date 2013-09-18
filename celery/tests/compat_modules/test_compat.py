from __future__ import absolute_import

from celery.tests.case import AppCase


class test_Task(AppCase):

    def test_base_task_inherits_magic_kwargs_from_app(self):
        from celery.task import Task as OldTask

        class timkX(OldTask):
            abstract = True

        app = Celery(set_as_current=False, accept_magic_kwargs=True)
        timkX.bind(app)
        # see #918
        self.assertFalse(timkX.accept_magic_kwargs)

        from celery import Task as NewTask

        class timkY(NewTask):
            abstract = True

        timkY.bind(app)
        self.assertFalse(timkY.accept_magic_kwargs)

