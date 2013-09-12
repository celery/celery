"""This module has moved to celery.app.trace."""
from __future__ import absolute_import

import sys

from celery.utils import warn_deprecated

warn_deprecated('celery.task.trace', removal='3.2',
                alternative='Please use celery.app.trace instead.')

from celery.app import trace
sys.modules[__name__] = trace
