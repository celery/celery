# -*- coding: utf-8 -*-
"""celerybeat

.. program:: celerybeat

.. seealso::

    See :ref:`preload-options` and :ref:`daemon-options`.

.. cmdoption:: --detach

    Detach and run in the background as a daemon.

.. cmdoption:: -s, --schedule

    Path to the schedule database. Defaults to `celerybeat-schedule`.
    The extension ".db" may be appended to the filename.
    Default is %(default)s.

.. cmdoption:: -S, --scheduler

    Scheduler class to use.
    Default is :class:`celery.beat.PersistentScheduler`.

.. cmdoption:: max-interval

    Max seconds to sleep between schedule iterations.

.. cmdoption:: -f, --logfile

    Path to log file. If no logfile is specified, `stderr` is used.

.. cmdoption:: -l, --loglevel

    Logging level, choose between `DEBUG`, `INFO`, `WARNING`,
    `ERROR`, `CRITICAL`, or `FATAL`.

"""
from __future__ import with_statement
from __future__ import absolute_import

import os

from functools import partial

from celery.platforms import detached

from celery.bin.base import Command, Option, daemon_options


class BeatCommand(Command):
    doc = __doc__
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
        c = self.app.conf

        return (
            Option('--detach', action="store_true"),
            Option('-s', '--schedule', default=c.CELERYBEAT_SCHEDULE_FILENAME),
            Option('--max-interval', type="float"),
            Option('-S', '--scheduler', dest="scheduler_cls"),
            Option('-l', '--loglevel', default=c.CELERYBEAT_LOG_LEVEL),
        )


def main():
    beat = BeatCommand()
    beat.execute_from_commandline()

if __name__ == "__main__":      # pragma: no cover
    main()
