#!/usr/bin/env python
"""celerymon

.. program:: celerymon

.. cmdoption:: -P, --port

    Port the webserver should listen to. Default: ``8989``.

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

    User-id to run ``celerymon`` as when in daemon mode.

.. cmdoption:: -g, --gid

    Group-id to run ``celerymon`` as when in daemon mode.

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
from celery.monitoring import MonitorService
from celery.loaders import settings
from celery.messaging import get_connection_info

STARTUP_INFO_FMT = """
Configuration ->
    . broker -> %(conninfo)s
    . exchange -> %(exchange)s (%(exchange_type)s)
    . consumer -> queue:%(consumer_queue)s binding:%(consumer_rkey)s
    . webserver -> http://localhost:%(http_port)s
""".strip()

OPTION_LIST = (
    optparse.make_option('-f', '--logfile', default=conf.CELERYMON_LOG_FILE,
            action="store", dest="logfile",
            help="Path to log file."),
    optparse.make_option('-l', '--loglevel',
            default=conf.CELERYMON_LOG_LEVEL,
            action="store", dest="loglevel",
            help="Choose between DEBUG/INFO/WARNING/ERROR/CRITICAL/FATAL."),
    optparse.make_option('-P', '--port',
            action="store", type="int", dest="http_port", default=8989,
            help="Port the webserver should listen to."),
    optparse.make_option('-p', '--pidfile',
            default=conf.CELERYMON_PID_FILE,
            action="store", dest="pidfile",
            help="Path to pidfile."),
    optparse.make_option('-d', '--detach', '--daemon', default=False,
            action="store_true", dest="detach",
            help="Run in the background as a daemon."),
    optparse.make_option('-u', '--uid', default=None,
            action="store", dest="uid",
            help="User-id to run celerymon as when in daemon mode."),
    optparse.make_option('-g', '--gid', default=None,
            action="store", dest="gid",
            help="Group-id to run celerymon as when in daemon mode."),
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


def run_monitor(detach=False, loglevel=conf.CELERYMON_LOG_LEVEL,
        logfile=conf.CELERYMON_LOG_FILE, pidfile=conf.CELERYMON_PID_FILE,
        umask=0, uid=None, gid=None, working_directory=None, chroot=None,
        http_port=8989, **kwargs):
    """Starts the celery monitor."""

    print("celerymon %s is starting." % __version__)

    # Setup logging
    if not isinstance(loglevel, int):
        loglevel = conf.LOG_LEVELS[loglevel.upper()]
    if not detach:
        logfile = None # log to stderr when not running in the background.

    # Dump configuration to screen so we have some basic information
    # when users sends e-mails.
    print(STARTUP_INFO_FMT % {
            "http_port": http_port,
            "conninfo": get_connection_info(),
            "exchange": conf.AMQP_EXCHANGE,
            "exchange_type": conf.AMQP_EXCHANGE_TYPE,
            "consumer_queue": conf.AMQP_CONSUMER_QUEUE,
            "consumer_rkey": conf.AMQP_CONSUMER_ROUTING_KEY,
            "publisher_rkey": conf.AMQP_PUBLISHER_ROUTING_KEY,
            "loglevel": loglevel,
            "pidfile": pidfile,
    })

    from celery.log import setup_logger, redirect_stdouts_to_logger
    print("celerymon has started.")
    if detach:
        context = platform.create_daemon_context(logfile, pidfile,
                                        chroot_directory=chroot,
                                        working_directory=working_directory,
                                        umask=umask,
                                        uid=uid,
                                        gid=gid)
        context.open()
        logger = setup_logger(loglevel, logfile)
        redirect_stdouts_to_logger(logger, loglevel)

    def _run_clock():
        logger = setup_logger(loglevel, logfile)
        monitor = MonitorService(logger=logger,
                                 is_detached=detach,
                                 http_port=http_port)

        try:
            monitor.start()
        except Exception, e:
            emergency_error(logfile,
                    "celerymon raised exception %s: %s\n%s" % (
                            e.__class__, e, traceback.format_exc()))

    try:
        _run_clock()
    except:
        if detach:
            context.close()
        raise


def parse_options(arguments):
    """Parse the available options to ``celerymon``."""
    parser = optparse.OptionParser(option_list=OPTION_LIST)
    options, values = parser.parse_args(arguments)
    return options


if __name__ == "__main__":
    options = parse_options(sys.argv[1:])
    run_monitor(**vars(options))
