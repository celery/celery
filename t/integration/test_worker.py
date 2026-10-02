import subprocess
import sys
import time
from uuid import uuid4

import pytest

from celery import Celery
from celery.utils.sysinfo import cpu_budget

from .conftest import TEST_BROKER, flaky
from .tasks import add

TIMEOUT = 10


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


def test_django_fixup_installs_django_task_for_celery_subclass(monkeypatch):
    """A Celery subclass that leaves task_cls alone still gets DjangoTask."""
    import django

    from celery.contrib.django.task import DjangoTask

    monkeypatch.setenv('DJANGO_SETTINGS_MODULE', 't.integration.django_settings')
    django.setup()

    class MyCustomCelery(Celery):
        pass

    app = MyCustomCelery('test_django_subclass')

    assert issubclass(app.Task, DjangoTask)
    assert hasattr(app.Task, 'delay_on_commit')
    assert hasattr(app.Task, 'apply_async_on_commit')


@flaky
def test_concurrency_auto_sizes_prefork_pool(tmp_path):
    """``celery worker -c auto`` resolves concurrency and sizes the real pool."""
    hostname = f'auto-{uuid4().hex[:8]}@integration'
    log_path = tmp_path / 'worker.log'
    app = Celery(broker=TEST_BROKER)
    expected = cpu_budget().count

    with open(log_path, 'wb') as log:
        worker = subprocess.Popen(
            [sys.executable, '-m', 'celery', '-b', TEST_BROKER, 'worker',
             '-P', 'prefork', '-c', 'auto', '-n', hostname, '-l', 'INFO',
             '--without-mingle', '--without-gossip', '--without-heartbeat'],
            stdout=log, stderr=subprocess.STDOUT)
        try:
            stats = None
            deadline = time.monotonic() + 30
            while not stats and time.monotonic() < deadline:
                assert worker.poll() is None, log_path.read_text()
                stats = app.control.inspect(
                    destination=[hostname], timeout=1).stats()
            assert stats, log_path.read_text()
            assert stats[hostname]['pool']['max-concurrency'] == expected
        finally:
            worker.terminate()
            try:
                worker.wait(timeout=30)
            except subprocess.TimeoutExpired:
                worker.kill()
                worker.wait()
            app.close()

    assert (
        f"worker_concurrency='auto' resolved to {expected} (pool=prefork"
        in log_path.read_text()
    )


@flaky
def test_pidbox_reset_after_repeated_control_errors(manager):
    def assert_ping():
        ping_result = manager.inspect().ping()
        assert ping_result
        assert list(ping_result.values())[0] == {'ok': 'pong'}

    assert_ping()

    for _ in range(5):
        manager.app.control.broadcast('pidbox_reset_error', reply=False)

    assert_ping()

    result = add.delay(4, 4)
    assert result.get(timeout=TIMEOUT) == 8

    assert_ping()
