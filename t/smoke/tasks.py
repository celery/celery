from __future__ import annotations

from sys import getsizeof
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
def long_running_task(
    seconds: float = 1,
    verbose: bool = False,
    allocate: int | None = None,
    exhaust_memory: bool = False,
) -> bool:
    from celery import current_task
    from celery.utils.log import get_task_logger

    logger = get_task_logger(current_task.name)

    logger.info("Starting long running task")

    if allocate:
        # Attempt to allocate megabytes in memory
        _ = [0] * (allocate * 10**6 // getsizeof(int()))

    if exhaust_memory:
        mem = []
        while True:
            mem.append(' ' * 10**6)  # 1 MB of spaces

    for i in range(0, int(seconds)):
        sleep(1)
        if verbose:
            logger.info(f"Sleeping: {i}")

    logger.info("Finished long running task")

    return True


@shared_task(bind=True)
def replace_with_task(self: Task, replace_with: Signature = None):
    if replace_with is None:
        replace_with = replaced_with_me.s()
    return self.replace(signature(replace_with))
