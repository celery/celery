from unittest.mock import patch

import pytest


@pytest.mark.patched_module(
    'django',
    'django.db',
    'django.db.transaction',
)
@pytest.mark.usefixtures("module")
class test_DjangoTask:
    @pytest.fixture
    def task_instance(self):
        from celery.contrib.django.task import DjangoTask
        yield DjangoTask()

    @pytest.fixture(name="on_commit")
    def on_commit(self):
        with patch(
            'django.db.transaction.on_commit',
            side_effect=lambda f: f(),
        ) as patched_on_commit:
            yield patched_on_commit

    def test_delay_on_commit(self, task_instance, on_commit):
        result = task_instance.delay_on_commit()
        assert result is None

    def test_apply_async_on_commit(self, task_instance, on_commit):
        result = task_instance.apply_async_on_commit()
        assert result is None
