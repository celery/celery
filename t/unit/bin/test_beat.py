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
        ["-A", "t.unit.bin.proj.app", "beat", "-S", "t.unit.bin.proj.scheduler.mScheduler"],
        catch_exceptions=True
    )
    assert res.exit_code == 1, (res, res.stdout)
    assert res.stdout.startswith("celery beat")
    assert "Configuration ->" in res.stdout


def test_cli_quiet(isolated_cli_runner: CliRunner):
    Logging._setup = True  # To avoid hitting the logging sanity checks
    res = isolated_cli_runner.invoke(
        celery,
        ["-A", "t.unit.bin.proj.app", "--quiet", "beat", "-S", "t.unit.bin.proj.scheduler.mScheduler"],
        catch_exceptions=True
    )
    assert res.exit_code == 1, (res, res.stdout)
    assert not res.stdout.startswith("celery beat")
    assert "Configuration -> " not in res.stdout
