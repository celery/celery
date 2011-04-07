import warnings
from celery.schedules import schedule, crontab_parser, crontab

__all__ = ["schedule", "crontab_parser", "crontab"]

warnings.warn(DeprecationWarning(
    "celery.task.schedules is deprecated and renamed to celery.schedules"))
