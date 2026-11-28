"""Regression tests for chord part return on WorkerLostError after SUCCESS (#11038)."""

from unittest.mock import Mock

from billiard.einfo import ExceptionInfo

from celery import states
from celery.exceptions import WorkerLostError
from celery.worker.request import Request


def _make_worker_lost_einfo():
    try:
        raise WorkerLostError('Worker exited prematurely: signal 9 (SIGKILL)')
    except WorkerLostError:
        return ExceptionInfo(internal=True)


def test_on_failure_worker_lost_after_success_runs_chord_part_return(app):
    @app.task(shared=False)
    def header_task():
        return 42

    chord_callback = {'task': 'proj.body', 'args': (), 'kwargs': {}}
    body = ((), {}, {'callbacks': None, 'errbacks': None, 'chain': None, 'chord': chord_callback})
    headers = {
        'id': 'chord-header-1',
        'task': header_task.name,
        'group': 'chord-group-1',
        'group_index': 0,
    }
    message = Mock()
    message.headers = headers
    message.properties = {'correlation_id': 'chord-header-1', 'reply_to': ''}
    message.delivery_info = {'routing_key': 'celery'}

    req = Request(
        message,
        on_ack=Mock(),
        on_reject=Mock(),
        app=app,
        task=header_task,
        decoded=True,
        body=body,
    )
    req.task.acks_late = True
    req.task.backend = Mock()
    req.task.backend.max_retries = 3
    req.task.backend._ensure_retryable.return_value = {
        'status': states.SUCCESS,
        'result': 42,
    }

    req.on_failure(_make_worker_lost_einfo())

    req.task.backend.on_chord_part_return.assert_called_once_with(
        req._context, states.SUCCESS, 42,
    )
    assert req.acknowledged


def test_on_failure_worker_lost_after_success_without_chord_skips_chord_return(app):
    @app.task(shared=False)
    def plain_task():
        return 7

    body = ((), {}, {'callbacks': None, 'errbacks': None, 'chain': None, 'chord': None})
    headers = {'id': 'plain-task-1', 'task': plain_task.name}
    message = Mock()
    message.headers = headers
    message.properties = {'correlation_id': 'plain-task-1', 'reply_to': ''}
    message.delivery_info = {'routing_key': 'celery'}

    req = Request(
        message,
        on_ack=Mock(),
        on_reject=Mock(),
        app=app,
        task=plain_task,
        decoded=True,
        body=body,
    )
    req.task.backend = Mock()
    req.task.backend.max_retries = 3
    req.task.backend._ensure_retryable.return_value = {
        'status': states.SUCCESS,
        'result': 7,
    }

    req.on_failure(_make_worker_lost_einfo())

    req.task.backend.on_chord_part_return.assert_not_called()
