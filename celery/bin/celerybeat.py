# -*- coding: utf-8 -*-
"""celerybeat

.. program:: celerybeat

.. cmdoption:: -s, --schedule

    Path to the schedule database. Defaults to `celerybeat-schedule`.
    The extension ".db" will be appended to the filename.

.. cmdoption:: -S, --scheduler

    Scheduler class to use. Default is celery.beat.PersistentScheduler

.. cmdoption:: -f, --logfile

    Path to log file. If no logfile is specified, `stderr` is used.

.. cmdoption:: -l, --loglevel

    Logging level, choose between `DEBUG`, `INFO`, `WARNING`,
    `ERROR`, `CRITICAL`, or `FATAL`.

"""
from __future__ import with_statement
from __future__ import absolute_import

if __name__ == "__main__" and __package__ is None:
    __package__ = "celery.bin.celerybeat"

import os

from functools import partial

from ..platforms import detached

from .base import Command, Option, daemon_options


class BeatCommand(Command):
    enable_config_from_cmdline = True
    supports_args = False
    preload_options = (Command.preload_options
                     + daemon_options(default_pidfile="celerybeat.pid"))

    def run(self, detach=False, logfile=None, pidfile=None, uid=None,
            gid=None, umask=None, working_directory=None, **kwargs):
        workdir = working_directory
        kwargs.pop("app", None)
        beat = partial(self.app.Beat,
                       logfile=logfile, pidfile=pidfile, **kwargs)

        if detach:
            with detached(logfile, pidfile, uid, gid, umask, workdir):
                return beat().run()
        else:
            return beat().run()

    def prepare_preload_options(self, options):
        workdir = options.get("working_directory")
        if workdir:
            os.chdir(workdir)

    def get_options(self):
        conf = self.app.conf

        return (
            Option('--detach',
                default=False, action="store_true", dest="detach",
                help="Detach and run in the background."),
            Option('-s', '--schedule',
                default=conf.CELERYBEAT_SCHEDULE_FILENAME,
                action="store", dest="schedule",
                help="Path to the schedule database. The extension "
                    "'.db' will be appended to the filename. Default: %s" % (
                            conf.CELERYBEAT_SCHEDULE_FILENAME, )),
            Option('--max-interval',
                default=None, type="float", dest="max_interval",
                help="Max. seconds to sleep between schedule iterations."),
            Option('-S', '--scheduler',
                default=None,
                action="store", dest="scheduler_cls",
                help="Scheduler class. Default is "
                     "celery.beat:PersistentScheduler"),
            Option('-l', '--loglevel',
                default=conf.CELERYBEAT_LOG_LEVEL,
                action="store", dest="loglevel",
                help="Loglevel. One of DEBUG/INFO/WARNING/ERROR/CRITICAL."))


def main():
    beat = BeatCommand()
    beat.execute_from_commandline()

if __name__ == "__main__":      # pragma: no cover
    main()
