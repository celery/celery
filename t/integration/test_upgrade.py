"""Run ``celery upgrade settings`` as a real command on a real settings file."""

import subprocess
import sys

import pytest

SETTINGS = (
    b'BROKER_URL = "memory://"\r\n'
    b'CELERY_ALWAYS_EAGER = True\r\n'
    b'# \xc3\xa9t\xc3\xa9\n'
)


def upgrade_settings(*args):
    # Deprecation warnings raised by the upgrade command itself are turned into errors.
    return subprocess.run(
        [sys.executable, '-W', 'error::DeprecationWarning:celery.bin.upgrade',
         '-m', 'celery', 'upgrade', 'settings', *args],
        capture_output=True, text=True, check=False,
    )


@pytest.mark.parametrize('options, expected, backup', [
    ((), b'broker_url = "memory://"\r\ntask_always_eager = True\r\n# \xc3\xa9t\xc3\xa9\n', True),
    (('--django', '--no-backup'),
     b'CELERY_BROKER_URL = "memory://"\r\nCELERY_TASK_ALWAYS_EAGER = True\r\n# \xc3\xa9t\xc3\xa9\n', False),
])
def test_upgrade_settings_command(tmp_path, options, expected, backup):
    path = tmp_path / 'celeryconfig.py'
    path.write_bytes(SETTINGS)

    result = upgrade_settings(str(path), *options)

    assert result.returncode == 0, result.stderr
    assert 'DeprecationWarning' not in result.stderr
    assert 'Changes to your setting have been made!' in result.stdout
    assert path.read_bytes() == expected
    orig = path.with_name(path.name + '.orig')
    assert orig.exists() is backup
    if backup:
        assert orig.read_bytes() == SETTINGS
