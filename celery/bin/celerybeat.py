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

.. cmdoption:: -p, --pidfile

    Path to pidfile.

.. cmdoption:: -d, --detach, --daemon

    Run in the background as a daemon.

.. cmdoption:: -u, --uid

    User-id to run ``celerybeat`` as when in daemon mode.

.. cmdoption:: -g, --gid

    Group-id to run ``celerybeat`` as when in daemon mode.

.. cmdoption:: --umask

    umask of the process when in daemon mode.

.. cmdoption:: --workdir

    Directory to change to when in daemon mode.

.. cmdoption:: --chroot

    Change root directory to this path when in daemon mode.

"""
import os
import sys
import traceback
import optparse

from celery import conf
from celery import platform
from celery import __version__
from celery.log import emergency_error
from celery.beat import ClockService
from celery.utils import noop
from celery.loaders import current_loader, settings
from celery.messaging import get_connection_info

STARTUP_INFO_FMT = """
Configuration ->
    . broker -> %(conninfo)s
    . exchange -> %(exchange)s (%(exchange_type)s)
    . consumer -> queue:%(consumer_queue)s binding:%(consumer_rkey)s
    . schedule -> %(schedule)s
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
    optparse.make_option('-p', '--pidfile',
            default=conf.CELERYBEAT_PID_FILE,
            action="store", dest="pidfile",
            help="Path to pidfile."),
    optparse.make_option('-d', '--detach', '--daemon', default=False,
            action="store_true", dest="detach",
            help="Run in the background as a daemon."),
    optparse.make_option('-u', '--uid', default=None,
            action="store", dest="uid",
            help="User-id to run celerybeat as when in daemon mode."),
    optparse.make_option('-g', '--gid', default=None,
            action="store", dest="gid",
            help="Group-id to run celerybeat as when in daemon mode."),
    optparse.make_option('--umask', default=0,
            action="store", type="int", dest="umask",
            help="umask of the process when in daemon mode."),
    optparse.make_option('--workdir', default=None,
            action="store", dest="working_directory",
            help="Directory to change to when in daemon mode."),
    optparse.make_option('--chroot', default=None,
            action="store", dest="chroot",
            help="Change root directory to this path when in daemon mode."),
    )


def run_clockservice(detach=False, loglevel=conf.CELERYBEAT_LOG_LEVEL,
        logfile=conf.CELERYBEAT_LOG_FILE, pidfile=conf.CELERYBEAT_PID_FILE,
        umask=0, uid=None, gid=None, working_directory=None, chroot=None,
        schedule=conf.CELERYBEAT_SCHEDULE_FILENAME, **kwargs):
    """Starts the celerybeat clock server."""

    print("celerybeat %s is starting." % __version__)

    # Setup logging
    if not isinstance(loglevel, int):
        loglevel = conf.LOG_LEVELS[loglevel.upper()]
    if not detach:
        logfile = None # log to stderr when not running in the background.

    # Dump configuration to screen so we have some basic information
    # when users sends e-mails.

    print(STARTUP_INFO_FMT % {
            "conninfo": get_connection_info(),
            "exchange": conf.AMQP_EXCHANGE,
            "exchange_type": conf.AMQP_EXCHANGE_TYPE,
            "consumer_queue": conf.AMQP_CONSUMER_QUEUE,
            "consumer_rkey": conf.AMQP_CONSUMER_ROUTING_KEY,
            "publisher_rkey": conf.AMQP_PUBLISHER_ROUTING_KEY,
            "loglevel": loglevel,
            "pidfile": pidfile,
            "schedule": schedule,
    })

    print("celerybeat has started.")
    arg_start = "manage" in sys.argv[0] and 2 or 1
    platform.set_process_title("celerybeat",
                               info=" ".join(sys.argv[arg_start:]))
    from celery.log import setup_logger, redirect_stdouts_to_logger
    on_stop = noop
    if detach:
        context, on_stop = platform.create_daemon_context(logfile, pidfile,
                                        chroot_directory=chroot,
                                        working_directory=working_directory,
                                        umask=umask)
        context.open()
        logger = setup_logger(loglevel, logfile)
        redirect_stdouts_to_logger(logger, loglevel)
        platform.set_effective_user(uid, gid)

    # Run the worker init handler.
    # (Usually imports task modules and such.)
    current_loader.on_worker_init()

    def _run_clock():
        logger = setup_logger(loglevel, logfile)
        clockservice = ClockService(logger=logger, is_detached=detach,
                                    schedule_filename=schedule)

        try:
            clockservice.start()
        except Exception, e:
            emergency_error(logfile,
                    "celerybeat raised exception %s: %s\n%s" % (
                            e.__class__, e, traceback.format_exc()))

    try:
        _run_clock()
    except:
        on_stop()
        raise


def parse_options(arguments):
    """Parse the available options to ``celeryd``."""
    parser = optparse.OptionParser(option_list=OPTION_LIST)
    options, values = parser.parse_args(arguments)
    return options


if __name__ == "__main__":
    options = parse_options(sys.argv[1:])
    run_clockservice(**vars(options))
