import pytest

from celery import states, uuid


@pytest.mark.celery(
    result_serializer='json',
    accept_content=['json'],
    result_compression='gzip',
)
def test_decode_responses_disables_compression(app):
    url = app.conf.result_backend
    if not url.startswith('redis'):
        pytest.skip('Requires redis result backend.')

    separator = '&' if '?' in url else '?'
    app.conf.result_backend = f'{url}{separator}decode_responses=true'
    backend = app.backend
    task_id = uuid()
    result = {'answer': 42, 'text': 'hello world'}

    try:
        backend.store_result(task_id, result, states.SUCCESS)

        assert app.AsyncResult(task_id).get(timeout=5) == result
        assert isinstance(backend.client.get(backend.get_key_for_task(task_id)), str)
        # With decode_responses enabled, the backend ignores the configured
        # compression without changing the app configuration.
        assert backend.compression is None
        assert app.conf.result_compression == 'gzip'
    finally:
        backend.forget(task_id)
