import os
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from celery.bin.celery import celery


@pytest.fixture(autouse=True)
def clean_os_environ():
    # Celery modifies os.environ when given the CLI option --broker memory://
    with patch.dict(os.environ, clear=True):
        yield


@pytest.fixture
def settings_file(tmp_path):
    path = tmp_path / 'celeryconfig.py'
    path.write_bytes(
        b'BROKER_URL = "memory://"\r\n'
        b'CELERY_ALWAYS_EAGER = True\r\n'
        b'# \xc3\xa9t\xc3\xa9\n'
    )
    return path


def _upgrade(*args):
    return CliRunner().invoke(
        celery,
        ['-A', 't.unit.bin.proj.app', '--broker', 'memory://', 'upgrade', 'settings', *args],
        catch_exceptions=False,
    )


def test_upgrade_settings_renames_keys_and_writes_backup(settings_file):
    original = settings_file.read_bytes()

    res = _upgrade(str(settings_file))

    assert res.exit_code == 0, res.output
    assert 'Changes to your setting have been made!' in res.output
    # Keys are renamed; line endings and non-ASCII text are left untouched.
    assert settings_file.read_bytes() == (
        b'broker_url = "memory://"\r\n'
        b'task_always_eager = True\r\n'
        b'# \xc3\xa9t\xc3\xa9\n'
    )
    backup = settings_file.with_name(settings_file.name + '.orig')
    assert backup.read_bytes() == original


def test_upgrade_settings_django(settings_file):
    res = _upgrade(str(settings_file), '--django', '--no-backup')

    assert res.exit_code == 0, res.output
    assert settings_file.read_bytes() == (
        b'CELERY_BROKER_URL = "memory://"\r\n'
        b'CELERY_TASK_ALWAYS_EAGER = True\r\n'
        b'# \xc3\xa9t\xc3\xa9\n'
    )
    assert not settings_file.with_name(settings_file.name + '.orig').exists()


def test_upgrade_settings_without_changes(tmp_path):
    path = tmp_path / 'celeryconfig.py'
    path.write_bytes(b'broker_url = "memory://"\r\n')

    res = _upgrade(str(path))

    assert res.exit_code == 0, res.output
    assert 'Does not seem to require any changes' in res.output
    assert path.read_bytes() == b'broker_url = "memory://"\r\n'
    assert not path.with_name(path.name + '.orig').exists()
