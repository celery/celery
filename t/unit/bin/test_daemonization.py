from __future__ import annotations

from unittest.mock import patch

import pytest
from click.testing import CliRunner

from celery.bin.celery import celery

from .proj import daemon_config as config


@pytest.mark.usefixtures('depends_on_current_app')
@pytest.mark.parametrize("daemon", ["worker", "beat", "events"])
def test_daemon_options_from_config(daemon: str, cli_runner: CliRunner):

    with patch(f"celery.bin.{daemon}.{daemon}.callback") as mock:
        cli_runner.invoke(celery, f"-A t.unit.bin.proj.daemon {daemon}")

    mock.assert_called_once()
    for param in "logfile", "pidfile", "uid", "gid", "umask", "executable":
        assert mock.call_args.kwargs[param] == getattr(config, f"{daemon}_{param}")
