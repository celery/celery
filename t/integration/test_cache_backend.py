import time
from types import SimpleNamespace

import pytest

from celery import states, uuid
from celery.result import AsyncResult, GroupResult


@pytest.mark.celery(result_expires=1, result_chord_expires=60)
def test_chord_expires(app):
    if not app.conf.result_backend.startswith('cache+memcached'):
        pytest.skip('Requires memcached cache result backend.')

    backend = app.backend
    group_id, task_id = uuid(), uuid()
    header = GroupResult(group_id, [AsyncResult(task_id), AsyncResult(uuid())])
    request = SimpleNamespace(id=task_id, group=group_id, chord={})
    try:
        backend.apply_chord((group_id, header.results), None)
        backend.store_result(task_id, 1, states.SUCCESS)
        backend.on_chord_part_return(request, states.SUCCESS, 1)
        time.sleep(2)

        assert backend.get_task_meta(task_id, cache=False)['result'] == 1
        assert GroupResult.restore(group_id, backend=backend) == header
    finally:
        backend.forget(task_id)
        backend.delete_group(group_id)
        backend.delete(backend.get_key_for_chord(group_id))
