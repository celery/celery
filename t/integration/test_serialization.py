import os
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor

disabled_error_message = "Refusing to deserialize disabled content of type "


class test_config_serialization:
    def test_accept(self, celery_app):
        app = celery_app
        # Redefine env to use in subprocess
        # broker_url and result backend are different for each integration test backend
        passenv = {
            **os.environ,
            "CELERY_BROKER_URL": app.conf.broker_url,
            "CELERY_RESULT_BACKEND": app.conf.result_backend,
        }
        with ThreadPoolExecutor(max_workers=2) as executor:
            f1 = executor.submit(get_worker_error_messages, "w1", passenv)
            f2 = executor.submit(get_worker_error_messages, "w2", passenv)
            time.sleep(3)
            log1 = f1.result()
            log2 = f2.result()

        for log in [log1, log2]:
            assert log.find(disabled_error_message) == -1, log


def get_worker_error_messages(name, env):
    """run a worker and return its stderr

    :param name: the name of the worker
    :param env: the environment to run the worker in

    worker must be running in other process because of avoiding conflict."""
    worker = subprocess.Popen(
        [
            "celery",
            "--config",
            "t.integration.test_serialization_config",
            "worker",
            "-c",
            "2",
            "-n",
            f"{name}@%%h",
        ],
        stderr=subprocess.PIPE,
        stdout=subprocess.PIPE,
        env=env,
    )
    worker.terminate()
    err = worker.stderr.read().decode("utf-8")
    return err
