# -*- coding: utf-8 -*-
from __future__ import absolute_import

import warnings
from celery.schedules import schedule, crontab_parser, crontab  # noqa
from celery.exceptions import CDeprecationWarning

warnings.warn(CDeprecationWarning(
    "celery.task.schedules is deprecated and renamed to celery.schedules"))
