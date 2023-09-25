import celery.utils
from celery import shared_task
from t.integration.tasks import *  # noqa


@shared_task
def noop(*args, **kwargs) -> None:
    return celery.utils.noop(*args, **kwargs)
