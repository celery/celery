from __future__ import absolute_import

import warnings
from ..schedules import schedule, crontab_parser, crontab
from ..exceptions import CDeprecationWarning

__all__ = ["schedule", "crontab_parser", "crontab"]

warnings.warn(CDeprecationWarning(
    "celery.task.schedules is deprecated and renamed to celery.schedules"))
