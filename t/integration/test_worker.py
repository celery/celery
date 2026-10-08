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


@pytest.fixture
def two_django_apps(monkeypatch):
    """Two Django-enabled apps in one process, torn down afterwards."""
    import weakref

    django = pytest.importorskip('django')

    from celery import _state, signals
    from celery.app import trace
    from celery.fixups.django import DjangoFixup
    from celery.utils.dispatch import Signal

    monkeypatch.setenv('DJANGO_SETTINGS_MODULE', 't.integration.django_settings')
    monkeypatch.setenv('CELERY_SKIP_CHECKS', '1')
    django.setup()

    prev_default_app = _state.default_app
    prev_current_app = getattr(_state._tls, 'current_app', None)
    apps = [
        Celery(f'test_django_multi_app{i}', set_as_current=False)
        for i in (1, 2)
    ]
    for app in apps:
        app.config_from_object('django.conf:settings', namespace='CELERY')
    fixups = [
        next(f for f in app._fixups if isinstance(f, DjangoFixup))
        for app in apps
    ]
    try:
        yield tuple(zip(apps, fixups))
    finally:
        for app, fixup in zip(apps, fixups):
            signals.import_modules.disconnect(
                fixup.on_import_modules, sender=app,
            )
            signals.worker_init.disconnect(
                dispatch_uid=fixup._worker_init_uid,
            )
            # Has to mirror the signals DjangoWorkerFixup.install()
            # connects; the check below fails if one is added there and
            # not here.
            worker_fixup = fixup.worker_fixup
            signals.beat_embedded_init.disconnect(worker_fixup.close_database)
            signals.task_prerun.disconnect(worker_fixup.on_task_prerun)
            signals.task_postrun.disconnect(worker_fixup.on_task_postrun)
            signals.worker_process_init.disconnect(
                worker_fixup.on_worker_process_init,
            )
            # Undo what building a worker for the app did to global state.
            trace.reset_worker_optimizations(app)
            app.close()
        _state.default_app = prev_default_app
        _state._tls.current_app = prev_current_app

        def owner(receiver):
            if isinstance(receiver, weakref.ReferenceType):
                receiver = receiver()
            return getattr(receiver, '__self__', None)

        owners = [*fixups, *(fixup.worker_fixup for fixup in fixups)]
        leaked = [
            name
            for name, signal in vars(signals).items()
            if isinstance(signal, Signal)
            for _, receiver in signal.receivers
            if any(owner(receiver) is o for o in owners)
        ]
        assert not leaked, f'receivers left connected to: {leaked}'


def test_django_fixup_signals_only_handled_by_own_app(
        two_django_apps, monkeypatch):
    """With two Django-enabled apps, each fixup only handles its own app.

    Unlike the unit tests, the signals come from the real loader and from
    real workers instead of being sent by hand.
    """
    from celery.fixups.django import DjangoWorkerFixup

    (app1, fixup1), (app2, fixup2) = two_django_apps

    validated, installed = [], []
    validate_models = DjangoWorkerFixup.validate_models
    install = DjangoWorkerFixup.install

    def recording_validate_models(self):
        validated.append(self.app)
        return validate_models(self)

    def recording_install(self):
        installed.append(self.app)
        return install(self)

    monkeypatch.setattr(
        DjangoWorkerFixup, 'validate_models', recording_validate_models)
    monkeypatch.setattr(DjangoWorkerFixup, 'install', recording_install)

    # import_modules for app2 must only reach app2's fixup.
    app2.loader.import_default_modules()
    assert validated == [app2]

    # worker_init for app2's worker must only reach app2's fixup.
    worker2 = app2.Worker(pool='solo', concurrency=1)
    assert installed == [app2]
    assert app1 not in validated
    assert fixup2.worker_fixup.worker is worker2

    # And app1's worker is picked up by app1's fixup, not app2's.
    worker1 = app1.Worker(pool='solo', concurrency=1)
    assert installed == [app2, app1]
    assert fixup1.worker_fixup.worker is worker1
    assert fixup2.worker_fixup.worker is worker2


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
