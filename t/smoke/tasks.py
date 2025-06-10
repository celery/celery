"""Smoke tests tasks."""

from __future__ import annotations

import os
import sys
from signal import SIGKILL
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
def long_running_task(seconds: float = 1, verbose: bool = False) -> bool:
    from celery import current_task
    from celery.utils.log import get_task_logger

    logger = get_task_logger(current_task.name)

    logger.info("Starting long running task")

    for i in range(0, int(seconds)):
        sleep(1)
        if verbose:
            logger.info(f"Sleeping: {i}")

    logger.info("Finished long running task")

    return True


@shared_task(soft_time_limit=3, time_limit=5)
def soft_time_limit_lower_than_time_limit():
    sleep(4)


@shared_task(soft_time_limit=5, time_limit=3)
def soft_time_limit_must_exceed_time_limit():
    pass


@shared_task(bind=True)
def replace_with_task(self: Task, replace_with: Signature = None):
    if replace_with is None:
        replace_with = replaced_with_me.s()
    return self.replace(signature(replace_with))


@shared_task
def self_termination_sigkill():
    """Forceful termination."""
    os.kill(os.getpid(), SIGKILL)


@shared_task
def self_termination_system_exit():
    """Triggers a system exit to simulate a critical stop of the Celery worker."""
    sys.exit(1)


@shared_task(time_limit=2)
def self_termination_delay_timeout():
    """Delays the execution to simulate a task timeout."""
    sleep(4)


@shared_task
def self_termination_exhaust_memory():
    """Continuously allocates memory to simulate memory exhaustion."""
    mem = []
    while True:
        mem.append(" " * 10**6)
