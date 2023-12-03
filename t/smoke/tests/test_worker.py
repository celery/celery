import pytest
from pytest_celery import RESULT_TIMEOUT, CeleryTestSetup

from celery import Celery
from celery.canvas import chain
from t.smoke.tasks import long_running_task


@pytest.mark.parametrize(
    "restart_method",
    [
        "pool_restart",
        "docker_restart_gracefully",
        "docker_restart_force",
    ],
)
class test_worker_restart:
    @pytest.fixture
    def default_worker_app(self, default_worker_app: Celery) -> Celery:
        app = default_worker_app
        app.conf.worker_pool_restarts = True
        app.conf.task_acks_late = True
        yield app

    def test_restart_during_task_execution(
        self,
        celery_setup: CeleryTestSetup,
        restart_method: str,
    ):
        queue = celery_setup.worker.worker_queue
        sig = long_running_task.si(5, verbose=True).set(queue=queue)
        res = sig.delay()
        if restart_method == "pool_restart":
            celery_setup.app.control.pool_restart()
        elif restart_method == "docker_restart_gracefully":
            celery_setup.worker.restart()
        elif restart_method == "docker_restart_force":
            celery_setup.worker.restart(force=True)
        assert res.get(RESULT_TIMEOUT) is True

    def test_restart_between_task_execution(
        self,
        celery_setup: CeleryTestSetup,
        restart_method: str,
    ):
        queue = celery_setup.worker.worker_queue
        first = long_running_task.si(5, verbose=True).set(queue=queue)
        first_res = first.freeze()
        second = long_running_task.si(5, verbose=True).set(queue=queue)
        second_res = second.freeze()
        sig = chain(first, second)
        sig.delay()
        assert first_res.get(RESULT_TIMEOUT) is True
        if restart_method == "pool_restart":
            celery_setup.app.control.pool_restart()
        elif restart_method == "docker_restart_gracefully":
            celery_setup.worker.restart()
        elif restart_method == "docker_restart_force":
            celery_setup.worker.restart(force=True)
        assert second_res.get(RESULT_TIMEOUT) is True
