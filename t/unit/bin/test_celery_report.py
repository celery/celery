import json

import pytest
from click.testing import CliRunner

from celery import Celery
from celery._state import set_default_app
from celery.bin.celery import celery


def test_report_json_output():
    app = Celery('test')
    set_default_app(app)
    runner = CliRunner()
    result = runner.invoke(celery, ['report', '--json'])
    assert result.exit_code == 0
    # Check if output is valid JSON
    data = json.loads(result.output)
    assert isinstance(data, dict)
    # Check for common keys
    assert 'details' in data or any('->' in k for k in data)
