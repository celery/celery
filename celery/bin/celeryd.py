#!/usr/bin/env python
"""celeryd

.. program:: celeryd

.. cmdoption:: -c, --concurrency

    Number of child processes processing the queue. The default
    is the number of CPUs available on your system.

.. cmdoption:: -f, --logfile

    Path to log file. If no logfile is specified, ``stderr`` is used.

.. cmdoption:: -l, --loglevel

    Logging level, choose between ``DEBUG``, ``INFO``, ``WARNING``,
    ``ERROR``, ``CRITICAL``, or ``FATAL``.

.. cmdoption:: -p, --pidfile

    Path to pidfile.

.. cmdoption:: -s, --statistics

    Turn on reporting of statistics (remember to flush the statistics message
    queue from time to time).

.. cmdoption:: -d, --detach, --daemon

    Run in the background as a daemon.

.. cmdoption:: -S, --supervised

    Restart the worker server if it dies.

.. cmdoption:: --discard

    Discard all waiting tasks before the daemon is started.
    **WARNING**: This is unrecoverable, and the tasks will be
    deleted from the messaging server.

.. cmdoption:: -u, --uid

    User-id to run ``celeryd`` as when in daemon mode.

.. cmdoption:: -g, --gid

    Group-id to run ``celeryd`` as when in daemon mode.

.. cmdoption:: --umask

    umask of the process when in daemon mode.

.. cmdoption:: --workdir

    Directory to change to when in daemon mode.

.. cmdoption:: --chroot

    Change root directory to this path when in daemon mode.

"""
import os
import sys
import warnings
from carrot.connection import DjangoBrokerConnection
from celery.loaders import current_loader
from celery.loaders import settings
from celery import __version__
from celery.supervisor import OFASupervisor
from celery.log import emergency_error
from celery.conf import LOG_LEVELS, DAEMON_LOG_FILE, DAEMON_LOG_LEVEL
from celery.conf import DAEMON_CONCURRENCY, DAEMON_PID_FILE
from celery import conf
from celery.task import discard_all
from celery.worker import WorkController
from celery import platform
import multiprocessing
import traceback
import optparse

USE_STATISTICS = getattr(settings, "CELERY_STATISTICS", False)
# Make sure the setting exists.
settings.CELERY_STATISTICS = USE_STATISTICS

STARTUP_INFO_FMT = """
Configuration ->
    * Broker -> %(carrot_backend)s://%(vhost)s@%(host)s:%(port)s
    * Exchange -> %(exchange)s (%(exchange_type)s)
    * Consumer -> Queue:%(consumer_queue)s Routing:%(consumer_rkey)s
    * Concurrency -> %(concurrency)s
    * Statistics -> %(statistics)s
""".strip()

OPTION_LIST = (
    optparse.make_option('-c', '--concurrency', default=DAEMON_CONCURRENCY,
            action="store", dest="concurrency", type="int",
            help="Number of child processes processing the queue."),
    optparse.make_option('--discard', default=False,
            action="store_true", dest="discard",
            help="Discard all waiting tasks before the server is started. "
                 "WARNING: This is unrecoverable, and the tasks will be "
                 "deleted from the messaging server."),
    optparse.make_option('-s', '--statistics', default=USE_STATISTICS,
            action="store_true", dest="statistics",
            help="Collect statistics."),
    optparse.make_option('-f', '--logfile', default=DAEMON_LOG_FILE,
            action="store", dest="logfile",
            help="Path to log file."),
    optparse.make_option('-l', '--loglevel', default=DAEMON_LOG_LEVEL,
            action="store", dest="loglevel",
            help="Choose between DEBUG/INFO/WARNING/ERROR/CRITICAL/FATAL."),
    optparse.make_option('-p', '--pidfile', default=DAEMON_PID_FILE,
            action="store", dest="pidfile",
            help="Path to pidfile."),
    optparse.make_option('-d', '--detach', '--daemon', default=False,
            action="store_true", dest="detach",
            help="Run in the background as a daemon."),
    optparse.make_option('-S', '--supervised', default=False,
            action="store_true", dest="supervised",
            help="Restart the worker server if it dies."),
    optparse.make_option('-u', '--uid', default=None,
            action="store", dest="uid",
            help="User-id to run celeryd as when in daemon mode."),
    optparse.make_option('-g', '--gid', default=None,
            action="store", dest="gid",
            help="Group-id to run celeryd as when in daemon mode."),
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


def run_worker(concurrency=DAEMON_CONCURRENCY, detach=False,
        loglevel=DAEMON_LOG_LEVEL, logfile=DAEMON_LOG_FILE, discard=False,
        pidfile=DAEMON_PID_FILE, umask=0, uid=None, gid=None,
        supervised=False, working_directory=None, chroot=None,
        statistics=None, **kwargs):
    """Starts the celery worker server."""

    if detach:
        warnings.warn("""

WARNING: celery's support for --detach is severely broken!

Please use start-stop-daemon, supervisord or similar
daemonization services instead.

For more information and instructions, please read
http://ask.github.com/celery/cookbook/daemonizing.html

""", UserWarning)

    print("Celery %s is starting." % __version__)

    # set SIGCLD back to the default SIG_DFL (before python-daemon overrode
    # it) lets the parent wait() for the terminated child process and stops
    # the 'OSError: [Errno 10] No child processes' problem.
    platform.reset_signal("SIGCLD")

    if statistics is not None:
        settings.CELERY_STATISTICS = statistics

    if not concurrency:
        concurrency = multiprocessing.cpu_count()

    if conf.CELERY_BACKEND == "database" \
            and settings.DATABASE_ENGINE == "sqlite3" and \
            concurrency > 1:
        warnings.warn("The sqlite3 database engine doesn't support "
                "concurrency. We'll be using a single process only.",
                UserWarning)
        concurrency = 1

    # Setup logging
    if not isinstance(loglevel, int):
        loglevel = LOG_LEVELS[loglevel.upper()]
    if not detach:
        logfile = None # log to stderr when not running in the background.

    if discard:
        discarded_count = discard_all()
        what = discarded_count > 1 and "messages" or "message"
        print("discard: Erased %d %s from the queue.\n" % (
                discarded_count, what))

    # Dump configuration to screen so we have some basic information
    # when users sends e-mails.
    broker_connection = DjangoBrokerConnection()
    carrot_backend = broker_connection.backend_cls
    if carrot_backend and not isinstance(carrot_backend, str):
        carrot_backend = carrot_backend.__name__

    print(STARTUP_INFO_FMT % {
            "carrot_backend": carrot_backend or "amqp",
            "vhost": broker_connection.virtual_host or "(default)",
            "host": broker_connection.hostname or "(default)",
            "port": broker_connection.port or "(port)",
            "exchange": conf.AMQP_EXCHANGE,
            "exchange_type": conf.AMQP_EXCHANGE_TYPE,
            "consumer_queue": conf.AMQP_CONSUMER_QUEUE,
            "consumer_rkey": conf.AMQP_CONSUMER_ROUTING_KEY,
            "publisher_rkey": conf.AMQP_PUBLISHER_ROUTING_KEY,
            "concurrency": concurrency,
            "loglevel": loglevel,
            "pidfile": pidfile,
            "statistics": settings.CELERY_STATISTICS and "ON" or "OFF",
    })
    del(broker_connection)

    print("Celery has started.")
    if detach:
        from celery.log import setup_logger, redirect_stdouts_to_logger
        context = platform.create_daemon_context(logfile, pidfile,
                                        chroot_directory=chroot,
                                        working_directory=working_directory,
                                        umask=umask,
                                        uid=uid,
                                        gid=gid)
        context.open()
        logger = setup_logger(loglevel, logfile)
        redirect_stdouts_to_logger(logger, loglevel)

    # Run the worker init handler.
    # (Usually imports task modules and such.)
    current_loader.on_worker_init()

    def run_worker():
        worker = WorkController(concurrency=concurrency,
                                loglevel=loglevel,
                                logfile=logfile,
                                is_detached=detach)

        # Install signal handler that restarts celeryd on SIGHUP,
        # (only on POSIX systems)
        install_worker_restart_handler(worker)

        try:
            worker.start()
        except Exception, e:
            emergency_error(logfile, "celeryd raised exception %s: %s\n%s" % (
                            e.__class__, e, traceback.format_exc()))

    try:
        if supervised:
            OFASupervisor(target=run_worker).start()
        else:
            run_worker()
    except:
        if detach:
            context.close()
        raise


def install_worker_restart_handler(worker):

    def restart_worker_sig_handler(signum, frame):
        """Signal handler restarting the current python program."""
        worker.logger.info("Restarting celeryd (%s)" % (
            " ".join(sys.argv)))
        if worker.is_detached:
            pid = os.fork()
            if pid:
                worker.stop()
                sys.exit(0)
        else:
            worker.stop()
        os.execv(sys.executable, [sys.executable] + sys.argv)

    platform.install_signal_handler("SIGHUP", restart_worker_sig_handler)


def parse_options(arguments):
    """Parse the available options to ``celeryd``."""
    parser = optparse.OptionParser(option_list=OPTION_LIST)
    options, values = parser.parse_args(arguments)
    return options


def main():
    options = parse_options(sys.argv[1:])
    run_worker(**vars(options))

if __name__ == "__main__":
    main()
