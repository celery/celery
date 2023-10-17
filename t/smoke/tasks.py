from time import sleep

import celery.utils
from celery import shared_task
from t.integration.tasks import *  # noqa


@shared_task
def noop(*args, **kwargs) -> None:
    return celery.utils.noop(*args, **kwargs)


@shared_task
def long_running_task(seconds: float = 1) -> None:
    sleep(seconds)
