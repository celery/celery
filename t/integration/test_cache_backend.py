import time
from types import SimpleNamespace

import pytest

from celery import chord, states, uuid
from celery.app.task import Context
from celery.result import AsyncResult, GroupResult

from .conftest import TEST_BACKEND
from .tasks import identity, tsum

memcached = pytest.mark.skipif(
    not TEST_BACKEND.startswith('cache+memcached'),
    reason='Requires memcached cache result backend.')


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


@memcached
def test_chord_ignores_header_task_returning_after_completion(manager):
    backend = manager.app.backend
    errors = []

    @manager.app.task(name=f'record_chord_error.{uuid()}', shared=False)
    def record_chord_error(request, exc, traceback):
        errors.append(exc)

    result = chord([identity.si(1), identity.si(2)])(tsum.s())
    assert result.get(timeout=10) == 3
    header = result.parent
    # The broker delivers a header task again after the chord completed,
    # and that copy returns too. Its request carries the body's errbacks.
    body = tsum.s().set(task_id=result.id).on_error(record_chord_error.s())
    request = Context(id=header.results[0].id, group=header.id,
                      group_index=0, chord=body)
    backend.mark_as_done(request.id, 1, request=request)
    assert errors == []
    meta = backend.get_task_meta(result.id, cache=False)
    assert (meta['status'], meta['result']) == (states.SUCCESS, 3)


@memcached
def test_chord_counts_a_header_task_once(manager):
    backend = manager.app.backend
    group_id = uuid()
    headers = [AsyncResult(uuid()), AsyncResult(uuid())]
    body = tsum.s()
    body.freeze()
    backend.apply_chord((group_id, headers), body)
    requests = [Context(id=header.id, group=group_id, group_index=index,
                        chord=body)
                for index, header in enumerate(headers)]
    # The broker delivers header task 0 again, and both copies return.
    backend.mark_as_done(headers[0].id, 1, request=requests[0])
    backend.mark_as_done(headers[0].id, 1, request=requests[0])
    assert backend.get_task_meta(body.id, cache=False)['status'] == (
        states.PENDING)
    backend.mark_as_done(headers[1].id, 2, request=requests[1])
    assert AsyncResult(body.id).get(timeout=10) == 3
