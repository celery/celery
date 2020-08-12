import warnings

import pytest

from celery.task import base


def add(x, y):
    return x + y


@pytest.mark.usefixtures('depends_on_current_app')
class test_decorators:

    def test_task_alias(self):
        from celery import task
        assert task.__file__
        assert task(add)

    def setup(self):
        with warnings.catch_warnings(record=True):
            from celery import decorators
            self.decorators = decorators

    def assert_compat_decorator(self, decorator, type, **opts):
        task = decorator(**opts)(add)
        assert task(8, 8) == 16
        assert isinstance(task, type)

    def test_task(self):
        self.assert_compat_decorator(self.decorators.task, base.BaseTask)

    def test_periodic_task(self):
        self.assert_compat_decorator(
            self.decorators.periodic_task, base.BaseTask, run_every=1,
        )
