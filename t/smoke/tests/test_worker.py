import pytest
from pytest_celery import RESULT_TIMEOUT, CeleryTestSetup

from celery import Celery
from celery.canvas import chain
from t.smoke.tasks import long_running_task
from t.smoke.tests.conftest import WorkerOperations


@pytest.mark.parametrize(
    "restart_method",
    [
        WorkerOperations.RestartMethod.POOL_RESTART,
        WorkerOperations.RestartMethod.DOCKER_RESTART_GRACEFULLY,
        WorkerOperations.RestartMethod.DOCKER_RESTART_FORCE,
    ],
)
class test_worker_restart(WorkerOperations):
    @pytest.fixture
    def default_worker_app(self, default_worker_app: Celery) -> Celery:
        app = default_worker_app
        app.conf.worker_pool_restarts = True
        app.conf.task_acks_late = True
        yield app

    def test_restart_during_task_execution(
        self,
        celery_setup: CeleryTestSetup,
        restart_method: WorkerOperations.RestartMethod,
    ):
        queue = celery_setup.worker.worker_queue
        sig = long_running_task.si(5, verbose=True).set(queue=queue)
        res = sig.delay()
        self.restart(celery_setup, restart_method)
        assert res.get(RESULT_TIMEOUT) is True

    def test_restart_between_task_execution(
        self,
        celery_setup: CeleryTestSetup,
        restart_method: WorkerOperations.RestartMethod,
    ):
        queue = celery_setup.worker.worker_queue
        first = long_running_task.si(5, verbose=True).set(queue=queue)
        first_res = first.freeze()
        second = long_running_task.si(5, verbose=True).set(queue=queue)
        second_res = second.freeze()
        sig = chain(first, second)
        sig.delay()
        assert first_res.get(RESULT_TIMEOUT) is True
        self.restart(celery_setup, restart_method)
        assert second_res.get(RESULT_TIMEOUT) is True
