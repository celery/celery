import pytest
from click.testing import CliRunner


@pytest.fixture(scope='session')
def use_celery_app_trap():
    return False


@pytest.fixture
def isolated_cli_runner():
    """Return a Click CliRunner with mix_stderr=True (default) so that
    both stdout and stderr appear in ``res.output``, matching ClickException
    formatting which writes to stderr."""
    return CliRunner()


@pytest.fixture
def cli_runner():
    """Plain CliRunner fixture used by tests that don't need env isolation."""
    return CliRunner()
