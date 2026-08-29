import os
from unittest.mock import Mock, patch

import pytest
from click.testing import CliRunner

from celery.app.log import Logging
from celery.bin.celery import celery
from celery.worker.consumer.tasks import Tasks


@pytest.fixture(scope='session')
def use_celery_app_trap():
    return False


@pytest.fixture
def mock_app():
    app = Mock()
    app.conf = Mock()
    app.conf.worker_disable_prefetch = False
    app.conf.worker_detect_quorum_queues = False
    return app


@pytest.fixture
def mock_consumer(mock_app):
    consumer = Mock()
    consumer.app = mock_app
    consumer.pool = Mock()
    consumer.pool.num_processes = 4
    consumer.controller = Mock()
    consumer.controller.max_concurrency = None
    consumer.initial_prefetch_count = 16
    consumer.task_consumer = Mock()
    consumer.task_consumer.channel = Mock()
    consumer.task_consumer.channel.qos = Mock()
    original_can_consume = Mock(return_value=True)
    consumer.task_consumer.channel.qos.can_consume = original_can_consume
    consumer.connection = Mock()
    consumer.connection.transport = Mock()
    consumer.connection.transport.driver_type = 'redis'  # Default to Redis for existing tests
    consumer.connection.qos_semantics_matches_spec = True
    consumer.update_strategies = Mock()
    consumer.on_decode_error = Mock()
    consumer.app.amqp = Mock()
    consumer.app.amqp.TaskConsumer = Mock(return_value=consumer.task_consumer)
    consumer.app.amqp.queues = {}  # Empty dict for quorum queue detection
    return consumer


def test_cli(isolated_cli_runner: CliRunner):
    Logging._setup = True  # To avoid hitting the logging sanity checks
    res = isolated_cli_runner.invoke(
        celery,
        ["-A", "t.unit.bin.proj.app", "worker", "--pool", "solo"],
        catch_exceptions=False
    )
    assert res.exit_code == 1, (res, res.stdout)


def test_cli_skip_checks(isolated_cli_runner: CliRunner):
    Logging._setup = True  # To avoid hitting the logging sanity checks
    with patch.dict(os.environ, clear=True):
        res = isolated_cli_runner.invoke(
            celery,
            ["-A", "t.unit.bin.proj.app", "--skip-checks", "worker", "--pool", "solo"],
            catch_exceptions=False,
        )
        assert res.exit_code == 1, (res, res.stdout)
        assert os.environ["CELERY_SKIP_CHECKS"] == "true", "should set CELERY_SKIP_CHECKS"


def test_cli_disable_prefetch_flag(isolated_cli_runner: CliRunner):
    Logging._setup = True
    with patch('celery.bin.worker.worker.callback') as worker_callback_mock:
        res = isolated_cli_runner.invoke(
            celery,
            ["-A", "t.unit.bin.proj.app", "worker", "--pool", "solo", "--disable-prefetch"],
            catch_exceptions=False,
        )
        assert res.exit_code == 0
        _, kwargs = worker_callback_mock.call_args
        assert kwargs['disable_prefetch'] is True


def test_disable_prefetch_affects_qos_behavior(mock_app, mock_consumer):
    mock_app.conf.worker_disable_prefetch = True
    original_can_consume = mock_consumer.task_consumer.channel.qos.can_consume
    with patch('celery.worker.state.reserved_requests', []):
        tasks_instance = Tasks(mock_consumer)
        tasks_instance.start(mock_consumer)
        assert mock_consumer.task_consumer.channel.qos.can_consume != original_can_consume
        modified_can_consume = mock_consumer.task_consumer.channel.qos.can_consume
        with patch('celery.worker.state.reserved_requests', list(range(4))):
            assert not modified_can_consume()
        with patch('celery.worker.state.reserved_requests', list(range(2))):
            original_can_consume.return_value = True
            assert modified_can_consume()
            original_can_consume.return_value = False
            assert not modified_can_consume()


def test_disable_prefetch_none_preserves_behavior(mock_app, mock_consumer):
    mock_app.conf.worker_disable_prefetch = False
    kwargs_with_none = {'disable_prefetch': None}
    if 'disable_prefetch' in kwargs_with_none and kwargs_with_none['disable_prefetch'] is not None:
        mock_app.conf.worker_disable_prefetch = kwargs_with_none.pop('disable_prefetch')
    assert mock_app.conf.worker_disable_prefetch is False
    assert 'disable_prefetch' in kwargs_with_none
    original_can_consume = mock_consumer.task_consumer.channel.qos.can_consume
    tasks_instance = Tasks(mock_consumer)
    tasks_instance.start(mock_consumer)
    assert mock_consumer.task_consumer.channel.qos.can_consume == original_can_consume


def test_disable_prefetch_ignored_for_non_redis_brokers(mock_app, mock_consumer):
    """Test that disable_prefetch is ignored for non-Redis brokers."""
    mock_app.conf.worker_disable_prefetch = True
    mock_consumer.connection.transport.driver_type = 'amqp'  # RabbitMQ
    original_can_consume = mock_consumer.task_consumer.channel.qos.can_consume

    tasks_instance = Tasks(mock_consumer)
    tasks_instance.start(mock_consumer)

    # Should not modify can_consume method for non-Redis brokers
    assert mock_consumer.task_consumer.channel.qos.can_consume == original_can_consume
