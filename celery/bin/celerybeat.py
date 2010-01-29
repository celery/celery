#!/usr/bin/env python
"""celerybeat

.. program:: celerybeat

.. cmdoption:: -s, --schedule

    Path to the schedule database. Defaults to celerybeat-schedule.
    The extension ".db" will be appended to the filename.

.. cmdoption:: -f, --logfile

    Path to log file. If no logfile is specified, ``stderr`` is used.

.. cmdoption:: -l, --loglevel

    Logging level, choose between ``DEBUG``, ``INFO``, ``WARNING``,
    ``ERROR``, ``CRITICAL``, or ``FATAL``.

"""
import sys
import optparse
import traceback

import celery
from celery import conf
from celery import platform
from celery.log import emergency_error
from celery.beat import ClockService
from celery.utils import info

STARTUP_INFO_FMT = """
Configuration ->
    . broker -> %(conninfo)s
    . schedule -> %(schedule)s
    . logfile -> %(logfile)s@%(loglevel)s
""".strip()

OPTION_LIST = (
    optparse.make_option('-s', '--schedule',
            default=conf.CELERYBEAT_SCHEDULE_FILENAME,
            action="store", dest="schedule",
            help="Path to the schedule database. The extension \
                    '.db' will be appended to the filename. Default: %s" % (
                    conf.CELERYBEAT_SCHEDULE_FILENAME)),
    optparse.make_option('-f', '--logfile', default=conf.CELERYBEAT_LOG_FILE,
            action="store", dest="logfile",
            help="Path to log file."),
    optparse.make_option('-l', '--loglevel',
            default=conf.CELERYBEAT_LOG_LEVEL,
            action="store", dest="loglevel",
            help="Choose between DEBUG/INFO/WARNING/ERROR/CRITICAL/FATAL."),
)


def run_clockservice(loglevel=conf.CELERYBEAT_LOG_LEVEL,
        logfile=conf.CELERYBEAT_LOG_FILE,
        schedule=conf.CELERYBEAT_SCHEDULE_FILENAME, **kwargs):
    """Starts the celerybeat clock server."""

    print("celerybeat %s is starting." % celery.__version__)

    # Setup logging
    if not isinstance(loglevel, int):
        loglevel = conf.LOG_LEVELS[loglevel.upper()]

    # Run the worker init handler.
    # (Usually imports task modules and such.)
    from celery.loaders import current_loader
    current_loader().on_worker_init()


    # Dump configuration to screen so we have some basic information
    # when users sends e-mails.

    print(STARTUP_INFO_FMT % {
            "conninfo": info.format_broker_info(),
            "logfile": logfile or "@stderr",
            "loglevel": conf.LOG_LEVELS[loglevel],
            "schedule": schedule,
    })

    print("celerybeat has started.")
    arg_start = "manage" in sys.argv[0] and 2 or 1
    platform.set_process_title("celerybeat",
                               info=" ".join(sys.argv[arg_start:]))

    def _run_clock():
        from celery.log import setup_logger
        logger = setup_logger(loglevel, logfile)
        clockservice = ClockService(logger=logger, schedule_filename=schedule)

        try:
            clockservice.start()
        except Exception, e:
            emergency_error(logfile,
                    "celerybeat raised exception %s: %s\n%s" % (
                            e.__class__, e, traceback.format_exc()))

    _run_clock()


def parse_options(arguments):
    """Parse the available options to ``celeryd``."""
    parser = optparse.OptionParser(option_list=OPTION_LIST)
    options, values = parser.parse_args(arguments)
    return options


def main():
    options = parse_options(sys.argv[1:])
    run_clockservice(**vars(options))

if __name__ == "__main__":
    main()
