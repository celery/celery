from celery.utils import get_cls_by_name

ALIASES = {
    "processes": "celery.concurrency.processes.TaskPool",
    "eventlet": "celery.concurrency.evlet.TaskPool",
    "gevent": "celery.concurrency.evg.TaskPool",
    "threads": "celery.concurrency.threads.TaskPool",
    "solo": "celery.concurrency.solo.TaskPool",
}


def get_implementation(cls):
    return get_cls_by_name(cls, ALIASES)
