"""This module has moved to celery.app.trace."""
from __future__ import absolute_import

import sys

from celery.app import trace
sys.modules[__name__] = trace
