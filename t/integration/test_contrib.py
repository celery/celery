from __future__ import absolute_import, unicode_literals

import pytest

from .tasks import overloaded_call_task


@pytest.mark.flaky(reruns=5, reruns_delay=2)
def test_task_has_request(celery_worker):
    result = overloaded_call_task.apply_async(task_id='test-task-id')
    assert result.get(timeout=10) == 'test-task-id'
