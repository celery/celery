"""Tasks for the subinterpreter pool smoke tests.

The subinterpreter pool imports the task modules in every subinterpreter,
and extension modules such as pydantic-core (imported by
t.integration.tasks), gevent or psutil (imported by pytest-celery's tasks)
cannot be loaded in one, so this module only depends on Celery.
"""

from __future__ import annotations

from time import sleep

from celery import shared_task


@shared_task
def add(x: int, y: int) -> int:
    return x + y


@shared_task
def fail() -> None:
    raise ValueError("Task expected to fail")


@shared_task
def current_interpreter_id() -> int:
    from concurrent import interpreters

    return interpreters.get_current().id


@shared_task
def long_running_task(seconds: float = 1) -> bool:
    from celery import current_task
    from celery.utils.log import get_task_logger

    logger = get_task_logger(current_task.name)

    logger.info("Starting long running task")
    sleep(seconds)
    logger.info("Finished long running task")

    return True
