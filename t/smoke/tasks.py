from celery import shared_task
from celery.utils import noop as celery_noop
from t.integration.tasks import *  # noqa


@shared_task
def noop(*args, **kwargs) -> None:
    return celery_noop(*args, **kwargs)
