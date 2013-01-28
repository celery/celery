# -*- coding: utf-8 -*-
"""
    celery.signals
    ~~~~~~~~~~~~~~

    This module defines the signals (Observer pattern) sent by
    both workers and clients.

    Functions can be connected to these signals, and connected
    functions are called whenever a signal is called.

    See :ref:`signals` for more information.

"""
from __future__ import absolute_import
from .utils.dispatch import Signal

task_sent = Signal(providing_args=[
    'task_id', 'task', 'args', 'kwargs', 'eta', 'taskset'])
task_prerun = Signal(providing_args=['task_id', 'task', 'args', 'kwargs'])
task_postrun = Signal(providing_args=[
    'task_id', 'task', 'args', 'kwargs', 'retval'])
task_success = Signal(providing_args=['result'])
task_retry = Signal(providing_args=[
    'request', 'reason', 'einfo',
])
task_failure = Signal(providing_args=[
    'task_id', 'exception', 'args', 'kwargs', 'traceback', 'einfo'])
task_revoked = Signal(providing_args=['terminated', 'signum', 'expired'])
celeryd_init = Signal(providing_args=['instance', 'conf'])
celeryd_after_setup = Signal(providing_args=['instance', 'conf'])
worker_init = Signal(providing_args=[])
worker_process_init = Signal(providing_args=[])
worker_ready = Signal(providing_args=[])
worker_shutdown = Signal(providing_args=[])
setup_logging = Signal(providing_args=[
    'loglevel', 'logfile', 'format', 'colorize'])
after_setup_logger = Signal(providing_args=[
    'logger', 'loglevel', 'logfile', 'format', 'colorize'])
after_setup_task_logger = Signal(providing_args=[
    'logger', 'loglevel', 'logfile', 'format', 'colorize'])
beat_init = Signal(providing_args=[])
beat_embedded_init = Signal(providing_args=[])
eventlet_pool_started = Signal(providing_args=[])
eventlet_pool_preshutdown = Signal(providing_args=[])
eventlet_pool_postshutdown = Signal(providing_args=[])
eventlet_pool_apply = Signal(providing_args=['target', 'args', 'kwargs'])
