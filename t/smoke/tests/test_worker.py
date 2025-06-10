from time import sleep

import pytest
from pytest_celery import CeleryTestSetup, CeleryTestWorker, RabbitMQTestBroker

import celery
from celery import Celery
from celery.canvas import chain, group
from t.smoke.conftest import SuiteOperations, WorkerKill, WorkerRestart
from t.smoke.tasks import long_running_task

RESULT_TIMEOUT = 30


def assert_container_exited(worker: CeleryTestWorker, attempts: int = RESULT_TIMEOUT):
    """It might take a few moments for the container to exit after the worker is killed."""
    while attempts:
        worker.container.reload()
        if worker.container.status == "exited":
            break
        attempts -= 1
        sleep(1)

    worker.container.reload()
    assert worker.container.status == "exited"


@pytest.mark.parametrize("method", list(WorkerRestart.Method))
class test_worker_restart(SuiteOperations):
    @pytest.fixture
    def default_worker_app(self, default_worker_app: Celery) -> Celery:
        app = default_worker_app
        app.conf.worker_pool_restarts = True
        app.conf.task_acks_late = True
        return app

    def test_restart_during_task_execution(
        self,
        celery_setup: CeleryTestSetup,
        method: WorkerRestart.Method,
    ):
        queue = celery_setup.worker.worker_queue
        sig = long_running_task.si(5, verbose=True).set(queue=queue)
        res = sig.delay()
        self.restart_worker(celery_setup.worker, method)
        assert res.get(RESULT_TIMEOUT) is True

    def test_restart_between_task_execution(
        self,
        celery_setup: CeleryTestSetup,
        method: WorkerRestart.Method,
    ):
        # We use freeze() to control the order of execution for the restart operation
        queue = celery_setup.worker.worker_queue
        first = long_running_task.si(5, verbose=True).set(queue=queue)
        first_res = first.freeze()
        second = long_running_task.si(5, verbose=True).set(queue=queue)
        second_res = second.freeze()
        sig = chain(first, second)
        sig.delay()
        assert first_res.get(RESULT_TIMEOUT) is True
        self.restart_worker(celery_setup.worker, method)
        assert second_res.get(RESULT_TIMEOUT) is True


class test_worker_shutdown(SuiteOperations):
    @pytest.fixture
    def default_worker_app(self, default_worker_app: Celery) -> Celery:
        app = default_worker_app
        app.conf.task_acks_late = True
        return app

    def test_warm_shutdown(self, celery_setup: CeleryTestSetup):
        queue = celery_setup.worker.worker_queue
        worker = celery_setup.worker
        sig = long_running_task.si(5, verbose=True).set(queue=queue)
        res = sig.delay()

        worker.assert_log_exists("Starting long running task")
        self.kill_worker(worker, WorkerKill.Method.SIGTERM)
        worker.assert_log_exists("worker: Warm shutdown (MainProcess)")

        assert_container_exited(worker)
        assert res.get(RESULT_TIMEOUT)

    def test_multiple_warm_shutdown_does_nothing(self, celery_setup: CeleryTestSetup):
        queue = celery_setup.worker.worker_queue
        worker = celery_setup.worker
        sig = long_running_task.si(5, verbose=True).set(queue=queue)
        res = sig.delay()

        worker.assert_log_exists("Starting long running task")
        for _ in range(3):
            self.kill_worker(worker, WorkerKill.Method.SIGTERM)

        assert_container_exited(worker)
        assert res.get(RESULT_TIMEOUT)

    def test_cold_shutdown(self, celery_setup: CeleryTestSetup):
        queue = celery_setup.worker.worker_queue
        worker = celery_setup.worker
        sig = long_running_task.si(5, verbose=True).set(queue=queue)
        res = sig.delay()

        worker.assert_log_exists("Starting long running task")
        self.kill_worker(worker, WorkerKill.Method.SIGQUIT)
        worker.assert_log_exists("worker: Cold shutdown (MainProcess)")
        worker.assert_log_does_not_exist(f"long_running_task[{res.id}] succeeded", timeout=10)

        assert_container_exited(worker)

        with pytest.raises(celery.exceptions.TimeoutError):
            res.get(timeout=5)

    def test_hard_shutdown_from_warm(self, celery_setup: CeleryTestSetup):
        queue = celery_setup.worker.worker_queue
        worker = celery_setup.worker
        sig = long_running_task.si(420, verbose=True).set(queue=queue)
        sig.delay()

        worker.assert_log_exists("Starting long running task")
        self.kill_worker(worker, WorkerKill.Method.SIGTERM)
        self.kill_worker(worker, WorkerKill.Method.SIGQUIT)
        self.kill_worker(worker, WorkerKill.Method.SIGQUIT)

        worker.assert_log_exists("worker: Warm shutdown (MainProcess)")
        worker.assert_log_exists("worker: Cold shutdown (MainProcess)")

        assert_container_exited(worker)

    def test_hard_shutdown_from_cold(self, celery_setup: CeleryTestSetup):
        queue = celery_setup.worker.worker_queue
        worker = celery_setup.worker
        sig = long_running_task.si(420, verbose=True).set(queue=queue)
        sig.delay()

        worker.assert_log_exists("Starting long running task")
        self.kill_worker(worker, WorkerKill.Method.SIGQUIT)
        self.kill_worker(worker, WorkerKill.Method.SIGQUIT)

        worker.assert_log_exists("worker: Cold shutdown (MainProcess)")

        assert_container_exited(worker)

    class test_REMAP_SIGTERM(SuiteOperations):
        @pytest.fixture
        def default_worker_env(self, default_worker_env: dict) -> dict:
            default_worker_env.update({"REMAP_SIGTERM": "SIGQUIT"})
            return default_worker_env

        def test_cold_shutdown(self, celery_setup: CeleryTestSetup):
            queue = celery_setup.worker.worker_queue
            worker = celery_setup.worker
            sig = long_running_task.si(5, verbose=True).set(queue=queue)
            res = sig.delay()

            worker.assert_log_exists("Starting long running task")
            self.kill_worker(worker, WorkerKill.Method.SIGTERM)
            worker.assert_log_exists("worker: Cold shutdown (MainProcess)")
            worker.assert_log_does_not_exist(f"long_running_task[{res.id}] succeeded", timeout=10)

            assert_container_exited(worker)

        def test_hard_shutdown_from_cold(self, celery_setup: CeleryTestSetup):
            queue = celery_setup.worker.worker_queue
            worker = celery_setup.worker
            sig = long_running_task.si(420, verbose=True).set(queue=queue)
            sig.delay()

            worker.assert_log_exists("Starting long running task")
            self.kill_worker(worker, WorkerKill.Method.SIGTERM)
            self.kill_worker(worker, WorkerKill.Method.SIGTERM)

            worker.assert_log_exists("worker: Cold shutdown (MainProcess)")

            assert_container_exited(worker)

    class test_worker_soft_shutdown_timeout(SuiteOperations):
        @pytest.fixture
        def default_worker_app(self, default_worker_app: Celery) -> Celery:
            app = default_worker_app
            app.conf.worker_soft_shutdown_timeout = 10
            return app

        def test_soft_shutdown(self, celery_setup: CeleryTestSetup):
            app = celery_setup.app
            queue = celery_setup.worker.worker_queue
            worker = celery_setup.worker
            sig = long_running_task.si(5, verbose=True).set(queue=queue)
            res = sig.delay()

            worker.assert_log_exists("Starting long running task")
            self.kill_worker(worker, WorkerKill.Method.SIGQUIT)
            worker.assert_log_exists(
                f"Initiating Soft Shutdown, terminating in {app.conf.worker_soft_shutdown_timeout} seconds",
                timeout=5,
            )
            worker.assert_log_exists("worker: Cold shutdown (MainProcess)")

            assert_container_exited(worker)
            assert res.get(RESULT_TIMEOUT)

        def test_hard_shutdown_from_soft(self, celery_setup: CeleryTestSetup):
            queue = celery_setup.worker.worker_queue
            worker = celery_setup.worker
            sig = long_running_task.si(420, verbose=True).set(queue=queue)
            sig.delay()

            worker.assert_log_exists("Starting long running task")
            self.kill_worker(worker, WorkerKill.Method.SIGQUIT)
            self.kill_worker(worker, WorkerKill.Method.SIGQUIT)
            worker.assert_log_exists("Waiting gracefully for cold shutdown to complete...")
            worker.assert_log_exists("worker: Cold shutdown (MainProcess)")
            self.kill_worker(worker, WorkerKill.Method.SIGQUIT)

            assert_container_exited(worker)

        class test_REMAP_SIGTERM(SuiteOperations):
            @pytest.fixture
            def default_worker_env(self, default_worker_env: dict) -> dict:
                default_worker_env.update({"REMAP_SIGTERM": "SIGQUIT"})
                return default_worker_env

            def test_soft_shutdown(self, celery_setup: CeleryTestSetup):
                app = celery_setup.app
                queue = celery_setup.worker.worker_queue
                worker = celery_setup.worker
                sig = long_running_task.si(5, verbose=True).set(queue=queue)
                res = sig.delay()

                worker.assert_log_exists("Starting long running task")
                self.kill_worker(worker, WorkerKill.Method.SIGTERM)
                worker.assert_log_exists(
                    f"Initiating Soft Shutdown, terminating in {app.conf.worker_soft_shutdown_timeout} seconds"
                )
                worker.assert_log_exists("worker: Cold shutdown (MainProcess)")

                assert_container_exited(worker)
                assert res.get(RESULT_TIMEOUT)

            def test_hard_shutdown_from_soft(self, celery_setup: CeleryTestSetup):
                queue = celery_setup.worker.worker_queue
                worker = celery_setup.worker
                sig = long_running_task.si(420, verbose=True).set(queue=queue)
                sig.delay()

                worker.assert_log_exists("Starting long running task")
                self.kill_worker(worker, WorkerKill.Method.SIGTERM)
                self.kill_worker(worker, WorkerKill.Method.SIGTERM)
                worker.assert_log_exists("Waiting gracefully for cold shutdown to complete...")
                worker.assert_log_exists("worker: Cold shutdown (MainProcess)", timeout=5)
                self.kill_worker(worker, WorkerKill.Method.SIGTERM)

                assert_container_exited(worker)

        class test_reset_visibility_timeout(SuiteOperations):
            @pytest.fixture
            def default_worker_app(self, default_worker_app: Celery) -> Celery:
                app = default_worker_app
                app.conf.prefetch_multiplier = 2
                app.conf.worker_concurrency = 10
                app.conf.visibility_timeout = 3600  # 1 hour
                app.conf.broker_transport_options = {
                    "visibility_timeout": app.conf.visibility_timeout,
                    "polling_interval": 1,
                }
                app.conf.result_backend_transport_options = {
                    "visibility_timeout": app.conf.visibility_timeout,
                    "polling_interval": 1,
                }
                return app

            def test_soft_shutdown_reset_visibility_timeout(self, celery_setup: CeleryTestSetup):
                if isinstance(celery_setup.broker, RabbitMQTestBroker):
                    pytest.skip("RabbitMQ does not support visibility timeout")

                app = celery_setup.app
                queue = celery_setup.worker.worker_queue
                worker = celery_setup.worker
                sig = long_running_task.si(15, verbose=True).set(queue=queue)
                res = sig.delay()

                worker.assert_log_exists("Starting long running task")
                self.kill_worker(worker, WorkerKill.Method.SIGQUIT)
                worker.assert_log_exists(
                    f"Initiating Soft Shutdown, terminating in {app.conf.worker_soft_shutdown_timeout} seconds"
                )
                worker.assert_log_exists("worker: Cold shutdown (MainProcess)")
                worker.assert_log_exists("Restoring 1 unacknowledged message(s)")
                assert_container_exited(worker)
                worker.restart()
                assert res.get(RESULT_TIMEOUT)

            def test_soft_shutdown_reset_visibility_timeout_group_one_finish(self, celery_setup: CeleryTestSetup):
                if isinstance(celery_setup.broker, RabbitMQTestBroker):
                    pytest.skip("RabbitMQ does not support visibility timeout")

                app = celery_setup.app
                queue = celery_setup.worker.worker_queue
                worker = celery_setup.worker
                short_task = long_running_task.si(3, verbose=True).set(queue=queue)
                short_task_res = short_task.freeze()
                long_task = long_running_task.si(15, verbose=True).set(queue=queue)
                long_task_res = long_task.freeze()
                sig = group(short_task, long_task)
                sig.delay()

                worker.assert_log_exists(f"long_running_task[{short_task_res.id}] received")
                worker.assert_log_exists(f"long_running_task[{long_task_res.id}] received")
                self.kill_worker(worker, WorkerKill.Method.SIGQUIT)
                worker.assert_log_exists(
                    f"Initiating Soft Shutdown, terminating in {app.conf.worker_soft_shutdown_timeout} seconds"
                )
                worker.assert_log_exists("worker: Cold shutdown (MainProcess)")
                worker.assert_log_exists("Restoring 1 unacknowledged message(s)")
                assert_container_exited(worker)
                assert short_task_res.get(RESULT_TIMEOUT)

            def test_soft_shutdown_reset_visibility_timeout_group_none_finish(self, celery_setup: CeleryTestSetup):
                if isinstance(celery_setup.broker, RabbitMQTestBroker):
                    pytest.skip("RabbitMQ does not support visibility timeout")

                app = celery_setup.app
                queue = celery_setup.worker.worker_queue
                worker = celery_setup.worker
                short_task = long_running_task.si(15, verbose=True).set(queue=queue)
                short_task_res = short_task.freeze()
                long_task = long_running_task.si(15, verbose=True).set(queue=queue)
                long_task_res = long_task.freeze()
                sig = group(short_task, long_task)
                res = sig.delay()

                worker.assert_log_exists(f"long_running_task[{short_task_res.id}] received")
                worker.assert_log_exists(f"long_running_task[{long_task_res.id}] received")
                self.kill_worker(worker, WorkerKill.Method.SIGQUIT)
                worker.assert_log_exists(
                    f"Initiating Soft Shutdown, terminating in {app.conf.worker_soft_shutdown_timeout} seconds"
                )
                worker.assert_log_exists("worker: Cold shutdown (MainProcess)")
                worker.assert_log_exists("Restoring 2 unacknowledged message(s)")
                assert_container_exited(worker)
                worker.restart()
                assert res.get(RESULT_TIMEOUT) == [True, True]
                assert short_task_res.get(RESULT_TIMEOUT)
                assert long_task_res.get(RESULT_TIMEOUT)

            class test_REMAP_SIGTERM(SuiteOperations):
                @pytest.fixture
                def default_worker_env(self, default_worker_env: dict) -> dict:
                    default_worker_env.update({"REMAP_SIGTERM": "SIGQUIT"})
                    return default_worker_env

                def test_soft_shutdown_reset_visibility_timeout(self, celery_setup: CeleryTestSetup):
                    if isinstance(celery_setup.broker, RabbitMQTestBroker):
                        pytest.skip("RabbitMQ does not support visibility timeout")

                    app = celery_setup.app
                    queue = celery_setup.worker.worker_queue
                    worker = celery_setup.worker
                    sig = long_running_task.si(15, verbose=True).set(queue=queue)
                    res = sig.delay()

                    worker.assert_log_exists("Starting long running task")
                    self.kill_worker(worker, WorkerKill.Method.SIGTERM)
                    worker.assert_log_exists(
                        f"Initiating Soft Shutdown, terminating in {app.conf.worker_soft_shutdown_timeout} seconds"
                    )
                    worker.assert_log_exists("worker: Cold shutdown (MainProcess)")
                    worker.assert_log_exists("Restoring 1 unacknowledged message(s)")
                    assert_container_exited(worker)
                    worker.restart()
                    assert res.get(RESULT_TIMEOUT)

                def test_soft_shutdown_reset_visibility_timeout_group_one_finish(
                    self,
                    celery_setup: CeleryTestSetup,
                ):
                    if isinstance(celery_setup.broker, RabbitMQTestBroker):
                        pytest.skip("RabbitMQ does not support visibility timeout")

                    app = celery_setup.app
                    queue = celery_setup.worker.worker_queue
                    worker = celery_setup.worker
                    short_task = long_running_task.si(3, verbose=True).set(queue=queue)
                    short_task_res = short_task.freeze()
                    long_task = long_running_task.si(15, verbose=True).set(queue=queue)
                    long_task_res = long_task.freeze()
                    sig = group(short_task, long_task)
                    sig.delay()

                    worker.assert_log_exists(f"long_running_task[{short_task_res.id}] received")
                    worker.assert_log_exists(f"long_running_task[{long_task_res.id}] received")
                    self.kill_worker(worker, WorkerKill.Method.SIGTERM)
                    worker.assert_log_exists(
                        f"Initiating Soft Shutdown, terminating in {app.conf.worker_soft_shutdown_timeout} seconds"
                    )
                    worker.assert_log_exists("worker: Cold shutdown (MainProcess)")
                    worker.assert_log_exists("Restoring 1 unacknowledged message(s)")
                    assert_container_exited(worker)
                    assert short_task_res.get(RESULT_TIMEOUT)

            class test_worker_enable_soft_shutdown_on_idle(SuiteOperations):
                @pytest.fixture
                def default_worker_app(self, default_worker_app: Celery) -> Celery:
                    app = default_worker_app
                    app.conf.worker_enable_soft_shutdown_on_idle = True
                    return app

                def test_soft_shutdown(self, celery_setup: CeleryTestSetup):
                    app = celery_setup.app
                    worker = celery_setup.worker

                    self.kill_worker(worker, WorkerKill.Method.SIGQUIT)
                    worker.assert_log_exists(
                        f"Initiating Soft Shutdown, terminating in {app.conf.worker_soft_shutdown_timeout} seconds",
                    )
                    worker.assert_log_exists("worker: Cold shutdown (MainProcess)")

                    assert_container_exited(worker)

                def test_soft_shutdown_eta(self, celery_setup: CeleryTestSetup):
                    if isinstance(celery_setup.broker, RabbitMQTestBroker):
                        pytest.skip("RabbitMQ does not support visibility timeout")

                    app = celery_setup.app
                    queue = celery_setup.worker.worker_queue
                    worker = celery_setup.worker
                    sig = long_running_task.si(5, verbose=True).set(queue=queue)
                    res = sig.apply_async(countdown=app.conf.worker_soft_shutdown_timeout + 5)

                    worker.assert_log_exists(f"long_running_task[{res.id}] received")
                    self.kill_worker(worker, WorkerKill.Method.SIGQUIT)
                    worker.assert_log_exists(
                        f"Initiating Soft Shutdown, terminating in {app.conf.worker_soft_shutdown_timeout} seconds"
                    )
                    worker.assert_log_exists("worker: Cold shutdown (MainProcess)")
                    worker.assert_log_exists("Restoring 1 unacknowledged message(s)")
                    assert_container_exited(worker)
                    worker.restart()
                    assert res.get(RESULT_TIMEOUT)
