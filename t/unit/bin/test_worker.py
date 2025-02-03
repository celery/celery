import os
from unittest.mock import patch

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
