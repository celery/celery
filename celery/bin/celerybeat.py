#!/usr/bin/env python
"""celerybeat

.. program:: celerybeat

.. cmdoption:: -s, --schedule

    Path to the schedule database. Defaults to ``celerybeat-schedule``.
    The extension ".db" will be appended to the filename.

.. cmdoption:: -S, --scheduler

    Scheduler class to use. Default is celery.beat.PersistentScheduler

.. cmdoption:: -f, --logfile

    Path to log file. If no logfile is specified, ``stderr`` is used.

.. cmdoption:: -l, --loglevel

    Logging level, choose between ``DEBUG``, ``INFO``, ``WARNING``,
    ``ERROR``, ``CRITICAL``, or ``FATAL``.

"""
import sys
import traceback

from optparse import OptionParser, make_option as Option

import celery
from celery import beat
from celery import conf
from celery import platform
from celery.log import emergency_error
from celery.utils import info

STARTUP_INFO_FMT = """
Configuration ->
    . broker -> %(conninfo)s
    . schedule -> %(schedule)s
    . logfile -> %(logfile)s@%(loglevel)s
""".strip()

OPTION_LIST = (
    Option('-s', '--schedule',
        default=conf.CELERYBEAT_SCHEDULE_FILENAME,
        action="store", dest="schedule",
        help="Path to the schedule database. The extension "
             "'.db' will be appended to the filename. Default: %s" % (
                    conf.CELERYBEAT_SCHEDULE_FILENAME, )),
    Option('--max-interval',
        default=3600, type="int", dest="max_interval",
        help="Maximum time to sleep between re-reading the schedule."),
    Option('-S', '--scheduler',
        default=None,
        action="store", dest="scheduler_cls",
        help="Scheduler class. Default is celery.beat.PersistentScheduler"),
    Option('-f', '--logfile', default=conf.CELERYBEAT_LOG_FILE,
        action="store", dest="logfile",
        help="Path to log file."),
    Option('-l', '--loglevel',
        default=conf.CELERYBEAT_LOG_LEVEL,
        action="store", dest="loglevel",
        help="Loglevel. One of DEBUG/INFO/WARNING/ERROR/CRITICAL."),
)


class Beat(object):
    Service = beat.Service

    def __init__(self, loglevel=None, logfile=None, schedule=None,
            max_interval=None, scheduler_cls=None, defaults=conf, **kwargs):
        """Starts the celerybeat task scheduler."""

        self.loglevel = loglevel or defaults.CELERYBEAT_LOG_LEVEL
        self.logfile = logfile or defaults.CELERYBEAT_LOG_FILE
        self.schedule = schedule or defaults.CELERYBEAT_SCHEDULE_FILENAME
        self.scheduler_cls = scheduler_cls
        self.max_interval = max_interval

        if not isinstance(self.loglevel, int):
            self.loglevel = conf.LOG_LEVELS[self.loglevel.upper()]

    def run(self):
        print("celerybeat %s is starting." % celery.__version__)
        self.init_loader()
        print(self.startup_info())
        self.set_process_title()
        print("celerybeat has started.")
        self.start_scheduler()

    def start_scheduler(self):
        from celery.log import setup_logger
        logger = setup_logger(self.loglevel, self.logfile, name="celery.beat")
        beat = self.Service(logger=logger,
                            max_interval=self.max_interval,
                            scheduler_cls=self.scheduler_cls,
                            schedule_filename=self.schedule)

        try:
            self.install_sync_handler(beat)
            beat.start()
        except Exception, exc:
            emergency_error(self.logfile,
                    "celerybeat raised exception %s: %s\n%s" % (
                            exc.__class__, exc, traceback.format_exc()))

    def init_loader(self):
        # Run the worker init handler.
        # (Usually imports task modules and such.)
        from celery.loaders import current_loader
        current_loader().init_worker()

    def startup_info(self):
        return STARTUP_INFO_FMT % {
            "conninfo": info.format_broker_info(),
            "logfile": self.logfile or "@stderr",
            "loglevel": conf.LOG_LEVELS[self.loglevel],
            "schedule": self.schedule,
        }

    def set_process_title(self):
        arg_start = "manage" in sys.argv[0] and 2 or 1
        platform.set_process_title("celerybeat",
                               info=" ".join(sys.argv[arg_start:]))

    def install_sync_handler(self, beat):
        """Install a ``SIGTERM`` + ``SIGINT`` handler that saves
        the celerybeat schedule."""

        def _sync(signum, frame):
            beat.sync()
            raise SystemExit()

        platform.install_signal_handler("SIGTERM", _sync)
        platform.install_signal_handler("SIGINT", _sync)


def parse_options(arguments):
    """Parse the available options to ``celeryd``."""
    parser = OptionParser(option_list=OPTION_LIST)
    options, values = parser.parse_args(arguments)
    return options


def run_celerybeat(**options):
    Beat(**options).run()


def main():
    options = parse_options(sys.argv[1:])
    run_celerybeat(**vars(options))

if __name__ == "__main__":
    main()
