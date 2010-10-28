#!/usr/bin/env python
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
from celery.bin.base import Command, Option


class BeatCommand(Command):

    def run(self, *args, **kwargs):
        kwargs.pop("app", None)
        return self.app.Beat(**kwargs).run()

    def get_options(self):
        conf = self.app.conf

        return (
            Option('-s', '--schedule',
                default=conf.CELERYBEAT_SCHEDULE_FILENAME,
                action="store", dest="schedule",
                help="Path to the schedule database. The extension "
                    "'.db' will be appended to the filename. Default: %s" % (
                            conf.CELERYBEAT_SCHEDULE_FILENAME, )),
            Option('--max-interval',
                default=3600.0, type="float", dest="max_interval",
                help="Max. seconds to sleep between schedule iterations."),
            Option('-S', '--scheduler',
                default=None,
                action="store", dest="scheduler_cls",
                help="Scheduler class. Default is "
                     "celery.beat.PersistentScheduler"),
            Option('-f', '--logfile', default=conf.CELERYBEAT_LOG_FILE,
                action="store", dest="logfile",
                help="Path to log file."),
            Option('-l', '--loglevel',
                default=conf.CELERYBEAT_LOG_LEVEL,
                action="store", dest="loglevel",
                help="Loglevel. One of DEBUG/INFO/WARNING/ERROR/CRITICAL."),
        )


def main():
    beat = BeatCommand()
    beat.execute_from_commandline()

if __name__ == "__main__":      # pragma: no cover
    main()
