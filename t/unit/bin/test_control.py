import os
import re
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from celery.bin.celery import celery
from celery.platforms import EX_UNAVAILABLE

_GLOBAL_OPTIONS = ['-A', 't.unit.bin.proj.app_with_custom_cmds', '--broker', 'memory://']
_INSPECT_OPTIONS = ['--timeout', '0']  # Avoid waiting for the zero workers to reply


@pytest.fixture(autouse=True)
def clean_os_environ():
    # Celery modifies os.environ when given the CLI option --broker memory://
    # This interferes with other tests, so we need to reset os.environ
    with patch.dict(os.environ, clear=True):
        yield


@pytest.mark.parametrize(
    ('celery_cmd', 'custom_cmd'),
    [
        ('inspect', ('custom_inspect_cmd', '123')),
        ('control', ('custom_control_cmd', '123', '456')),
    ],
)
def test_custom_remote_command(celery_cmd, custom_cmd, isolated_cli_runner: CliRunner):
    res = isolated_cli_runner.invoke(
        celery,
        [*_GLOBAL_OPTIONS, celery_cmd, *_INSPECT_OPTIONS, *custom_cmd],
        catch_exceptions=False,
    )
    assert res.exit_code == EX_UNAVAILABLE, (res, res.stdout)
    assert res.stdout.strip() == 'Error: No nodes replied within time constraint'


@pytest.mark.parametrize(
    ('celery_cmd', 'remote_cmd'),
    [
        # Test nonexistent commands
        ('inspect', 'this_command_does_not_exist'),
        ('control', 'this_command_does_not_exist'),
        # Test commands that exist, but are of the wrong type
        ('inspect', 'custom_control_cmd'),
        ('control', 'custom_inspect_cmd'),
    ],
)
def test_unrecognized_remote_command(celery_cmd, remote_cmd, isolated_cli_runner: CliRunner):
    res = isolated_cli_runner.invoke(
        celery,
        [*_GLOBAL_OPTIONS, celery_cmd, *_INSPECT_OPTIONS, remote_cmd],
        catch_exceptions=False,
    )
    assert res.exit_code == 2, (res, res.stdout)
    assert f'Error: Command {remote_cmd} not recognized. Available {celery_cmd} commands: ' in res.stdout


_expected_inspect_regex = (
    '\n  custom_inspect_cmd x\\s+Ask the workers to reply with x\\.\n'
)
_expected_control_regex = (
    '\n  custom_control_cmd a b\\s+Ask the workers to reply with a and b\\.\n'
)


@pytest.mark.parametrize(
    ('celery_cmd', 'expected_regex'),
    [
        ('inspect', re.compile(_expected_inspect_regex, re.MULTILINE)),
        ('control', re.compile(_expected_control_regex, re.MULTILINE)),
    ],
)
def test_listing_remote_commands(celery_cmd, expected_regex, isolated_cli_runner: CliRunner):
    res = isolated_cli_runner.invoke(
        celery,
        [*_GLOBAL_OPTIONS, celery_cmd, '--list'],
    )
    assert res.exit_code == 0, (res, res.stdout)
    assert expected_regex.search(res.stdout)
