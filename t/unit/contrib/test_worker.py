import pytest

# this import adds a @shared_task, which uses connect_on_app_finalize
# to install the celery.ping task that the test lib uses
import celery.contrib.testing.tasks
from celery import Celery
from celery.contrib.testing.worker import start_worker


class test_worker:
    def setup_method(self):
        self.app = Celery('celerytest', backend='cache+memory://', broker='memory://',)

        @self.app.task
        def add(x, y):
            return x + y

        self.add = add

        @self.app.task
        def error_task():
            raise NotImplementedError()

        self.error_task = error_task

        self.app.config_from_object({
            'worker_hijack_root_logger': False,
        })

        # to avoid changing the root logger level to ERROR,
        # we have we have to set both app.log.loglevel start_worker arg to 0
        # (see celery.app.log.setup_logging_subsystem)
        self.app.log.loglevel = 0

    def test_start_worker(self):
        with start_worker(app=self.app, loglevel=0):
            result = self.add.s(1, 2).apply_async()
            val = result.get(timeout=5)
        assert val == 3

    def test_start_worker_with_exception(self):
        """Make sure that start_worker does not hang on exception"""

        with pytest.raises(NotImplementedError):
            with start_worker(app=self.app, loglevel=0):
                result = self.error_task.apply_async()
                result.get(timeout=5)
