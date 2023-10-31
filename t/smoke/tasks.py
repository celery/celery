from time import sleep

import celery.utils
from celery import Task, shared_task, signature
from celery.canvas import Signature
from t.integration.tasks import *  # noqa
from t.integration.tasks import replaced_with_me


@shared_task
def noop(*args, **kwargs) -> None:
    return celery.utils.noop(*args, **kwargs)


@shared_task
def long_running_task(seconds: float = 1) -> bool:
    sleep(seconds)
    return True


@shared_task(bind=True)
def replace_with_task(self: Task, replace_with: Signature = None):
    if replace_with is None:
        replace_with = replaced_with_me.s()
    self.replace(signature(replace_with))
