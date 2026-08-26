from unittest.mock import Mock, patch

import pytest

# this import adds a @shared_task, which uses connect_on_app_finalize
# to install the celery.ping task that the test lib uses
import celery.contrib.testing.tasks  # noqa
from celery import Celery
from celery.contrib.testing.worker import TestWorkController, start_worker


class test_worker:
    def setup_method(self):
        self.app = Celery('celerytest', backend='cache+memory://', broker='memory://', )

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
        # we have to set both app.log.loglevel start_worker arg to 0
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

    def test_start_worker_with_hostname_config(self):
        """Make sure a custom hostname can be supplied to the TestWorkController"""
        test_hostname = 'test_name@test_host'
        with start_worker(app=self.app, loglevel=0, hostname=test_hostname) as w:

            assert isinstance(w, TestWorkController)
            assert w.hostname == test_hostname

            result = self.add.s(1, 2).apply_async()
            val = result.get(timeout=5)
        assert val == 3


class test_TestWorkController:

    @patch('celery.contrib.testing.worker.worker.WorkController.__init__')
    def test_init_with_string_pool_cls_prefork(self, mock_super_init):
        mock_super_init.return_value = None
        controller = object.__new__(TestWorkController)
        controller._on_started = None
        controller.pool_cls = 'prefork'
        with patch('celery.contrib.testing.worker.logging.handlers.QueueListener') as mock_listener:
            with patch('billiard.Queue') as mock_queue:
                with patch.dict('sys.modules', {'tblib': None, 'tblib.pickling_support': None}):
                    mock_queue.return_value = Mock()
                    controller.__init__(app=Mock())
            mock_listener.assert_called_once()
        assert controller.logger_queue is not None

    @patch('celery.contrib.testing.worker.worker.WorkController.__init__')
    def test_init_with_string_pool_cls_solo(self, mock_super_init):
        mock_super_init.return_value = None
        controller = object.__new__(TestWorkController)
        controller._on_started = None
        controller.pool_cls = 'solo'
        controller.__init__(app=Mock())
        assert controller.logger_queue is None

    @patch('celery.contrib.testing.worker.worker.WorkController.__init__')
    def test_init_with_string_pool_cls_gevent(self, mock_super_init):
        mock_super_init.return_value = None
        controller = object.__new__(TestWorkController)
        controller._on_started = None
        controller.pool_cls = 'gevent'
        controller.__init__(app=Mock())
        assert controller.logger_queue is None
