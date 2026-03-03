from pytest_celery import CeleryTestSetup


class test_control:
    def test_sanity(self, celery_setup: CeleryTestSetup):
        r = celery_setup.app.control.ping()
        assert all(
            [
                all([res["ok"] == "pong" for _, res in response.items()])
                for response in r
            ]
        )

    def test_shutdown_exit_with_zero(self, celery_setup: CeleryTestSetup):
        celery_setup.app.control.shutdown(destination=[celery_setup.worker.hostname()])
        while celery_setup.worker.container.status != "exited":
            celery_setup.worker.container.reload()
        assert celery_setup.worker.container.attrs["State"]["ExitCode"] == 0
