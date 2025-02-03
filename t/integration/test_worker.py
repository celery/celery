import subprocess

import pytest


def test_run_worker():
    with pytest.raises(subprocess.CalledProcessError) as exc_info:
        subprocess.check_output(
            ["celery", "--config", "t.integration.test_worker_config", "worker"],
            stderr=subprocess.STDOUT)

    called_process_error = exc_info.value
    assert called_process_error.returncode == 1, called_process_error
    output = called_process_error.output.decode('utf-8')
    assert output.find(
        "Retrying to establish a connection to the message broker after a connection "
        "loss has been disabled (app.conf.broker_connection_retry_on_startup=False). "
        "Shutting down...") != -1, output
