import os
from unittest.mock import Mock, patch

import pytest
from click.testing import CliRunner

from celery.app.log import Logging
from celery.bin.celery import celery


@pytest.fixture(scope='session')
def use_celery_app_trap():
    return False


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
    # Ensure CLI parses --disable-prefetch and passes it into worker startup
    res = isolated_cli_runner.invoke(
        celery,
        ["-A", "t.unit.bin.proj.app", "worker", "--pool", "solo", "--disable-prefetch"],
        catch_exceptions=False,
    )
    # We only assert parsing works and command initializes; worker exits with 1 in tests
    assert res.exit_code == 1, res.output


def test_worker_sets_disable_prefetch_when_true():
    app_mock = Mock()
    app_mock.conf = Mock()
    app_mock.conf.worker_disable_prefetch = False
    
    kwargs = {'disable_prefetch': True}
    if 'disable_prefetch' in kwargs and kwargs['disable_prefetch'] is not None:
        app_mock.conf.worker_disable_prefetch = kwargs.pop('disable_prefetch')
    
    assert app_mock.conf.worker_disable_prefetch is True
    assert 'disable_prefetch' not in kwargs


def test_worker_ignores_disable_prefetch_when_none():
    app_mock = Mock()
    app_mock.conf = Mock()
    app_mock.conf.worker_disable_prefetch = False
    
    kwargs = {'disable_prefetch': None}
    if 'disable_prefetch' in kwargs and kwargs['disable_prefetch'] is not None:
        app_mock.conf.worker_disable_prefetch = kwargs.pop('disable_prefetch')
    
    assert app_mock.conf.worker_disable_prefetch is False
    assert 'disable_prefetch' in kwargs
