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
        ["--config", "t.integration.test_worker_config", "worker"],
        catch_exceptions=False
    )
    assert res.exit_code == 1, res
