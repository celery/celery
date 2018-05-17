from __future__ import absolute_import, unicode_literals

import pytest

from celery.contrib.testing.worker import TestWorkController

from .tasks import overloaded_call_task


class UnprotectedTestWorkController(TestWorkController):
    def on_before_init(self, quiet=False, **kwargs):
        # do not call _install_stack_protection
        pass


@pytest.fixture
def celery_worker_parameters():
    return {
        'WorkController': UnprotectedTestWorkController,
    }


def test_task_request_is_empty(celery_worker):
    result = overloaded_call_task.apply_async()
    assert result.get(timeout=10) is None
