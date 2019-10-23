import pytest

import celery
from celery.app.task import Task as ModernTask
from celery.task.base import Task as CompatTask


@pytest.mark.usefixtures('depends_on_current_app')
class test_MagicModule:

    def test_class_property_set_without_type(self):
        assert ModernTask.__dict__['app'].__get__(CompatTask())

    def test_class_property_set_on_class(self):
        assert (ModernTask.__dict__['app'].__set__(None, None) is
                ModernTask.__dict__['app'])

    def test_class_property_set(self, app):

        class X(CompatTask):
            pass
        ModernTask.__dict__['app'].__set__(X(), app)
        assert X.app is app

    def test_dir(self):
        assert dir(celery.messaging)

    def test_direct(self):
        assert celery.task

    def test_app_attrs(self):
        assert (celery.task.control.broadcast ==
                celery.current_app.control.broadcast)

    def test_decorators_task(self):
        @celery.decorators.task
        def _test_decorators_task():
            pass

    def test_decorators_periodic_task(self):
        @celery.decorators.periodic_task(run_every=3600)
        def _test_decorators_ptask():
            pass
