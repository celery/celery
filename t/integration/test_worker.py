import subprocess
from time import monotonic, sleep

import pytest
from kombu import Queue

from celery import Celery
from celery.exceptions import TimeoutError
from t.integration.tasks import identity


def test_run_worker():
    with pytest.raises(subprocess.CalledProcessError) as exc_info:
        subprocess.check_output(
            ["celery", "--config", "t.integration.worker_config", "worker"],
            stderr=subprocess.STDOUT)

    called_process_error = exc_info.value
    assert called_process_error.returncode == 1, called_process_error
    output = called_process_error.output.decode('utf-8')
    assert output.find(
        "Retrying to establish a connection to the message broker after a connection "
        "loss has been disabled (app.conf.broker_connection_retry_on_startup=False). "
        "Shutting down...") != -1, output


def test_django_fixup_direct_worker(caplog, monkeypatch):
    """Test Django fixup by directly instantiating Celery worker without subprocess."""
    import logging

    import django

    # Set logging level to capture debug messages
    caplog.set_level(logging.DEBUG)

    # Configure Django settings
    monkeypatch.setenv('DJANGO_SETTINGS_MODULE', 't.integration.django_settings')
    django.setup()

    # Create Celery app with Django integration
    app = Celery('test_django_direct')
    app.config_from_object('django.conf:settings', namespace='CELERY')
    app.autodiscover_tasks()

    # Test that we can access worker configuration without recursion errors
    # This should trigger the Django fixup initialization
    worker = app.Worker(
        pool='solo',
        concurrency=1,
        loglevel='debug'
    )

    # Accessing pool_cls should not cause AttributeError
    pool_cls = worker.pool_cls
    assert pool_cls is not None

    # Verify pool_cls has __module__ attribute (should be a class, not a string)
    assert hasattr(pool_cls, '__module__'), \
        f"pool_cls should be a class with __module__, got {type(pool_cls)}: {pool_cls}"

    # Capture and check logs
    log_output = caplog.text

    # Verify no recursion-related errors in logs
    assert "RecursionError" not in log_output, f"RecursionError found in logs:\n{log_output}"
    assert "maximum recursion depth exceeded" not in log_output, \
        f"Recursion depth error found in logs:\n{log_output}"

    assert "AttributeError: 'str' object has no attribute '__module__'." not in log_output, \
        f"AttributeError found in logs:\n{log_output}"


class test_worker_queue_alias_reconnect:
    @pytest.fixture
    def celery_worker_parameters(self):
        return {'queues': ('alias', 'celery')}

    @pytest.mark.celery(task_queues=[Queue('real_name', alias='alias')])
    def test_queue_selected_by_alias_is_not_reconsumed_after_cancel_by_real_name_and_reconnect(
        self, celery_app, celery_worker
    ):
        consumer = celery_worker.consumer
        celery_app.control.cancel_consumer('real_name', reply=True, timeout=10)

        def _force_reconnect():
            raise consumer.connection_errors[0]('forced reconnect')

        # Force a reconnect so the worker rebuilds its consumers from _consume_from.
        restarts = consumer.restart_count
        consumer.hub.call_soon(_force_reconnect)
        deadline = monotonic() + 10
        while consumer.restart_count <= restarts and monotonic() < deadline:
            sleep(0.5)
        assert consumer.restart_count > restarts, 'worker did not reconnect'

        with pytest.raises(TimeoutError):
            identity.s('Hello').apply_async(queue='real_name').get(timeout=10)


class test_explicit_routing_without_default_queue:
    @pytest.fixture
    def celery_worker_parameters(self):
        return {'queues': ('non_default_queue',), 'perform_ping_check': False}

    @pytest.mark.celery(
        task_create_missing_queues=False,
        task_queues=[Queue('non_default_queue')],
    )
    def test_apply_async_succeeds_with_missing_queue_creation_disabled(self, celery_worker):
        result = identity.apply_async(('hello',), queue='non_default_queue')
        assert result.get(timeout=10) == 'hello'
