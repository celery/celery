"""Distributed Task Queue"""
from celery.distmeta import __version__, __author__, __contact__
from celery.distmeta import __homepage__, __docformat__
from celery.distmeta import VERSION, is_stable_release, version_with_meta

from celery.decorators import task
from celery.task.base import Task, PeriodicTask
from celery.execute import apply_async, apply
