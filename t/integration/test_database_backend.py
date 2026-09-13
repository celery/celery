from unittest.mock import patch

import pytest
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

from celery.exceptions import BackendStoreError


@pytest.fixture
def db_retry_app(celery_app, tmp_path):
    """Fixture to set up a Celery app with a database backend and retry configs."""
    # Use a temporary file-based SQLite db so tables persist across sessions.
    # :memory: with SQLAlchemy NullPool drops the db when the connection closes.
    db_file = tmp_path / "test_backend.db"

    celery_app.conf.update(
        result_backend=f'db+sqlite:///{db_file}',
        database_create_tables_at_setup=True,
        result_backend_always_retry=True,
        result_backend_max_retries=2,
        # Keep sleep times low so tests run fast
        result_backend_base_sleep_between_retries_ms=1,
        result_backend_max_sleep_between_retries_ms=5,
    )

    # Initialize backend and force table creation before tests patch the session
    backend = celery_app.backend
    backend.store_result('init-task', {'status': 'initialized'}, 'SUCCESS')

    return celery_app


def test_database_backend_transient_failure_integration(db_retry_app):
    """
    Integration test simulating a transient database failure.
    The database commit fails on the first attempt but succeeds on the second.
    """
    backend = db_retry_app.backend
    task_id = 'transient-integration-task'
    expected_result = {'foo': 'bar'}

    original_commit = Session.commit
    call_count = [0]

    def transient_failing_commit(self, *args, **kwargs):
        call_count[0] += 1
        if call_count[0] == 1:
            # Simulate a network drop or DB disconnect on the first try
            raise OperationalError("simulated transient DB disconnect", params={}, orig=Exception())
        return original_commit(self, *args, **kwargs)

    with patch('sqlalchemy.orm.Session.commit', autospec=True, side_effect=transient_failing_commit):
        # This will trigger the OperationalError, get caught by _ensure_retryable, and retry successfully.
        backend.store_result(task_id, expected_result, 'SUCCESS')

    # Ensure it failed once and succeeded on the second try
    assert call_count[0] == 2

    # Verify the result was actually persisted in the SQLite DB
    meta = backend.get_task_meta(task_id)
    assert meta['status'] == 'SUCCESS'
    assert meta['result'] == expected_result


def test_database_backend_max_retries_exceeded_integration(db_retry_app):
    """
    Integration test simulating a persistent database failure
    that eventually exceeds the maximum configured retries.
    """
    backend = db_retry_app.backend
    task_id = 'persistent-integration-task'

    call_count = [0]

    def persistent_failing_commit(self, *args, **kwargs):
        call_count[0] += 1
        raise OperationalError("simulated persistent DB disconnect", params={}, orig=Exception())

    with patch('sqlalchemy.orm.Session.commit', autospec=True, side_effect=persistent_failing_commit):
        with pytest.raises(BackendStoreError):
            backend.store_result(task_id, {'result': 'fail'}, 'SUCCESS')

    # Max retries is 2, so it should attempt exactly 3 times (1 initial + 2 retries)
    assert call_count[0] == 3


def test_database_backend_get_task_meta_transient_failure(db_retry_app):
    """
    Integration test simulating a transient database failure during a read operation.
    The database query fails on the first attempt but succeeds on the second.
    """
    backend = db_retry_app.backend
    task_id = 'transient-read-task'
    expected_result = {'foo': 'bar'}

    backend.store_result(task_id, expected_result, 'SUCCESS')

    call_count = [0]
    original_query = Session.query

    def transient_failing_query(self, *args, **kwargs):
        call_count[0] += 1
        if call_count[0] == 1:
            # Simulate a transient read error on the first try
            raise OperationalError("simulated transient DB read error", params={}, orig=Exception())
        return original_query(self, *args, **kwargs)

    # Patch Session.query to simulate transient read errors
    with patch('sqlalchemy.orm.Session.query', autospec=True, side_effect=transient_failing_query):
        # This will trigger the OperationalError, get caught by _ensure_retryable, and retry successfully.
        meta = backend.get_task_meta(task_id, cache=False)

    # Ensure it failed once and succeeded on the second try
    assert call_count[0] == 2

    # Verify the final retrieved data is correct
    assert meta['status'] == 'SUCCESS'
    assert meta['result'] == expected_result
