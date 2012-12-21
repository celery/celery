# -*- coding: utf-8 -*-
"""
    celery.apps.beat
    ~~~~~~~~~~~~~~~~

    This module is the 'program-version' of :mod:`celery.beat`.

    It does everything necessary to run that module
    as an actual application, like installing signal handlers
    and so on.

"""
from __future__ import absolute_import

import socket
import sys

from celery import VERSION_BANNER, platforms, beat
from celery.app import app_or_default
from celery.app.abstract import configurated, from_config
from celery.utils.imports import qualname
from celery.utils.log import LOG_LEVELS, get_logger
from celery.utils.timeutils import humanize_seconds

STARTUP_INFO_FMT = """
Configuration ->
    . broker -> %(conninfo)s
    . loader -> %(loader)s
    . scheduler -> %(scheduler)s
%(scheduler_info)s
    . logfile -> %(logfile)s@%(loglevel)s
    . maxinterval -> %(hmax_interval)s (%(max_interval)ss)
""".strip()

logger = get_logger('celery.beat')


class Beat(configurated):
    Service = beat.Service

    app = None
    loglevel = from_config('log_level')
    logfile = from_config('log_file')
    schedule = from_config('schedule_filename')
    scheduler_cls = from_config('scheduler')
    redirect_stdouts = from_config()
    redirect_stdouts_level = from_config()

    def __init__(self, max_interval=None, app=None,
                 socket_timeout=30, pidfile=None, no_color=None, **kwargs):
        """Starts the celerybeat task scheduler."""
        self.app = app = app_or_default(app or self.app)
        self.setup_defaults(kwargs, namespace='celerybeat')

        self.max_interval = max_interval
        self.socket_timeout = socket_timeout
        self.no_color = no_color
        self.colored = app.log.colored(
            self.logfile,
            enabled=not no_color if no_color is not None else no_color,
        )
        self.pidfile = pidfile

        if not isinstance(self.loglevel, int):
            self.loglevel = LOG_LEVELS[self.loglevel.upper()]

    def run(self):
        print(str(self.colored.cyan(
            'celerybeat v%s is starting.' % VERSION_BANNER)))
        self.init_loader()
        self.set_process_title()
        self.start_scheduler()

    def setup_logging(self, colorize=None):
        if colorize is None and self.no_color is not None:
            colorize = not self.no_color
        self.app.log.setup(self.loglevel, self.logfile,
                           self.redirect_stdouts, self.redirect_stdouts_level,
                           colorize=colorize)

    def start_scheduler(self):
        c = self.colored
        if self.pidfile:
            platforms.create_pidlock(self.pidfile)
        beat = self.Service(app=self.app,
                            max_interval=self.max_interval,
                            scheduler_cls=self.scheduler_cls,
                            schedule_filename=self.schedule)

        print(str(c.blue('__    ', c.magenta('-'),
                  c.blue('    ... __   '), c.magenta('-'),
                  c.blue('        _\n'),
                  c.reset(self.startup_info(beat)))))
        self.setup_logging()
        if self.socket_timeout:
            logger.debug('Setting default socket timeout to %r',
                         self.socket_timeout)
            socket.setdefaulttimeout(self.socket_timeout)
        try:
            self.install_sync_handler(beat)
            beat.start()
        except Exception, exc:
            logger.critical('celerybeat raised exception %s: %r',
                            exc.__class__, exc,
                            exc_info=True)

    def init_loader(self):
        # Run the worker init handler.
        # (Usually imports task modules and such.)
        self.app.loader.init_worker()
        self.app.finalize()

    def startup_info(self, beat):
        scheduler = beat.get_scheduler(lazy=True)
        return STARTUP_INFO_FMT % {
            'conninfo': self.app.connection().as_uri(),
            'logfile': self.logfile or '[stderr]',
            'loglevel': LOG_LEVELS[self.loglevel],
            'loader': qualname(self.app.loader),
            'scheduler': qualname(scheduler),
            'scheduler_info': scheduler.info,
            'hmax_interval': humanize_seconds(beat.max_interval),
            'max_interval': beat.max_interval,
        }

    def set_process_title(self):
        arg_start = 'manage' in sys.argv[0] and 2 or 1
        platforms.set_process_title(
            'celerybeat', info=' '.join(sys.argv[arg_start:]),
        )

    def install_sync_handler(self, beat):
        """Install a `SIGTERM` + `SIGINT` handler that saves
        the celerybeat schedule."""

        def _sync(signum, frame):
            beat.sync()
            raise SystemExit()

        platforms.signals.update(SIGTERM=_sync, SIGINT=_sync)
