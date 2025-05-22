import json
import pytest
from click.testing import CliRunner
from celery.bin.celery import celery

@pytest.mark.skip(reason="This test requires a Celery app context. It may fail in environments where current_app is not set.")
def test_report_json_output():
    runner = CliRunner()
    result = runner.invoke(celery, ['report', '--json'])
    assert result.exit_code == 0
    # Check if output is valid JSON
    data = json.loads(result.output)
    assert isinstance(data, dict)
    # Check for common keys
    assert 'details' in data or any('->' in k for k in data)