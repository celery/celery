from __future__ import absolute_import


import celery
from celery.app.task import BaseTask
from celery.task.base import Task

from celery.tests.utils import Case


class test_MagicModule(Case):

    def test_class_property_set_without_type(self):
        self.assertTrue(BaseTask.__dict__["app"].__get__(Task()))

    def test_class_property_set_on_class(self):
        self.assertIs(BaseTask.__dict__["app"].__set__(None, None),
                      BaseTask.__dict__["app"])

    def test_class_property_set(self):

        class X(Task):
            pass

        app = celery.Celery(set_as_current=False)
        BaseTask.__dict__["app"].__set__(X(), app)
        self.assertEqual(X.app, app)

    def test_dir(self):
        self.assertTrue(dir(celery.messaging))

    def test_direct(self):
        import sys
        prev_celery = sys.modules.pop("celery", None)
        prev_task = sys.modules.pop("celery.task", None)
        try:
            import celery
            self.assertTrue(celery.task)
        finally:
            sys.modules["celery"] = prev_celery
            sys.modules["celery.task"] = prev_task

    def test_app_attrs(self):
        self.assertEqual(celery.task.control.broadcast,
                         celery.current_app.control.broadcast)

    def test_decorators_task(self):
        @celery.decorators.task
        def _test_decorators_task():
            pass

        self.assertTrue(_test_decorators_task.accept_magic_kwargs)

    def test_decorators_periodic_task(self):
        @celery.decorators.periodic_task(run_every=3600)
        def _test_decorators_ptask():
            pass

        self.assertTrue(_test_decorators_ptask.accept_magic_kwargs)
