import pytest
from pytest_celery import CeleryBackendCluster, CeleryTestSetup

from celery.signals import after_task_publish, before_task_publish
from t.smoke.tasks import noop


@pytest.fixture
def default_worker_signals(default_worker_signals: set) -> set:
    from t.smoke import signals

    default_worker_signals.add(signals)
    yield default_worker_signals


@pytest.fixture
def celery_backend_cluster() -> CeleryBackendCluster:
    # Disable backend
    return None


class test_signals:
    @pytest.mark.parametrize(
        "log, control",
        [
            ("worker_init_handler", None),
            ("worker_process_init_handler", None),
            ("worker_ready_handler", None),
            ("worker_process_shutdown_handler", "shutdown"),
            ("worker_shutdown_handler", "shutdown"),
        ],
    )
    def test_sanity(self, celery_setup: CeleryTestSetup, log: str, control: str):
        if control:
            celery_setup.app.control.broadcast(control)
        celery_setup.worker.wait_for_log(log)


class test_before_task_publish:
    def test_sanity(self, celery_setup: CeleryTestSetup):
        @before_task_publish.connect
        def before_task_publish_handler(*args, **kwargs):
            nonlocal signal_was_called
            signal_was_called = True

        signal_was_called = False
        noop.s().apply_async(queue=celery_setup.worker.worker_queue)
        assert signal_was_called is True


class test_after_task_publish:
    def test_sanity(self, celery_setup: CeleryTestSetup):
        @after_task_publish.connect
        def after_task_publish_handler(*args, **kwargs):
            nonlocal signal_was_called
            signal_was_called = True

        signal_was_called = False
        noop.s().apply_async(queue=celery_setup.worker.worker_queue)
        assert signal_was_called is True
