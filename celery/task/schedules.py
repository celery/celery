# -*- coding: utf-8 -*-
from __future__ import absolute_import

import warnings
from ..schedules import schedule, crontab_parser, crontab  # noqa
from ..exceptions import CDeprecationWarning

warnings.warn(CDeprecationWarning(
    "celery.task.schedules is deprecated and renamed to celery.schedules"))
