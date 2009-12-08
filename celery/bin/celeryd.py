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

.. cmdoption:: -B, --beat

    Also run the ``celerybeat`` periodic task scheduler. Please note that
    there must only be one instance of this service.

.. cmdoption:: -E, --events

    Send events that can be captured by monitors like ``celerymon``.

.. cmdoption:: -d, --detach, --daemon

    Run in the background as a daemon.

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
import multiprocessing
import traceback
import optparse

from carrot.connection import DjangoBrokerConnection

from celery import conf
from celery import platform
from celery import __version__
from celery.log import emergency_error
from celery.task import discard_all
from celery.worker import WorkController
from celery.loaders import current_loader, settings
from celery.loaders import settings
from celery.messaging import get_connection_info

STARTUP_INFO_FMT = """
Configuration ->
    . broker -> %(conninfo)s
    . exchange -> %(exchange)s (%(exchange_type)s)
    . consumer -> queue:%(consumer_queue)s binding:%(consumer_rkey)s
    . concurrency -> %(concurrency)s
    . events -> %(events)s
    . beat -> %(celerybeat)s
""".strip()

OPTION_LIST = (
    optparse.make_option('-c', '--concurrency',
            default=conf.DAEMON_CONCURRENCY,
            action="store", dest="concurrency", type="int",
            help="Number of child processes processing the queue."),
    optparse.make_option('--discard', default=False,
            action="store_true", dest="discard",
            help="Discard all waiting tasks before the server is started. "
                 "WARNING: This is unrecoverable, and the tasks will be "
                 "deleted from the messaging server."),
    optparse.make_option('-f', '--logfile', default=conf.DAEMON_LOG_FILE,
            action="store", dest="logfile",
            help="Path to log file."),
    optparse.make_option('-l', '--loglevel', default=conf.DAEMON_LOG_LEVEL,
            action="store", dest="loglevel",
            help="Choose between DEBUG/INFO/WARNING/ERROR/CRITICAL/FATAL."),
    optparse.make_option('-p', '--pidfile', default=conf.DAEMON_PID_FILE,
            action="store", dest="pidfile",
            help="Path to pidfile."),
    optparse.make_option('-B', '--beat', default=False,
            action="store_true", dest="run_clockservice",
            help="Also run the celerybeat periodic task scheduler. \
                  Please note that only one instance must be running."),
    optparse.make_option('-E', '--events', default=conf.CELERY_SEND_EVENTS,
            action="store_true", dest="events",
            help="Send events so celery can be monitored by e.g. celerymon."),
    optparse.make_option('-d', '--detach', '--daemon', default=False,
            action="store_true", dest="detach",
            help="Run in the background as a daemon."),
    optparse.make_option('-u', '--uid', default=None,
            action="store", dest="uid", type="int",
            help="User-id to run celeryd as when in daemon mode."),
    optparse.make_option('-g', '--gid', default=None,
            action="store", dest="gid", type="int",
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


def run_worker(concurrency=conf.DAEMON_CONCURRENCY, detach=False,
        loglevel=conf.DAEMON_LOG_LEVEL, logfile=conf.DAEMON_LOG_FILE,
        discard=False, pidfile=conf.DAEMON_PID_FILE, umask=0,
        uid=None, gid=None, working_directory=None,
        chroot=None, run_clockservice=False, events=False, **kwargs):
    """Starts the celery worker server."""

    print("Celery %s is starting." % __version__)

    if not concurrency:
        concurrency = multiprocessing.cpu_count()

    if conf.CELERY_BACKEND == "database" \
            and settings.DATABASE_ENGINE == "sqlite3" and \
            concurrency > 1:
        import warnings
        warnings.warn("The sqlite3 database engine doesn't support "
                "concurrency. We'll be using a single process only.",
                UserWarning)
        concurrency = 1

    # Setup logging
    if not isinstance(loglevel, int):
        loglevel = conf.LOG_LEVELS[loglevel.upper()]
    if not detach:
        logfile = None # log to stderr when not running in the background.

    if discard:
        discarded_count = discard_all()
        what = discarded_count > 1 and "messages" or "message"
        print("discard: Erased %d %s from the queue.\n" % (
                discarded_count, what))

    # Dump configuration to screen so we have some basic information
    # when users sends e-mails.

    print(STARTUP_INFO_FMT % {
            "conninfo": get_connection_info(),
            "exchange": conf.AMQP_EXCHANGE,
            "exchange_type": conf.AMQP_EXCHANGE_TYPE,
            "consumer_queue": conf.AMQP_CONSUMER_QUEUE,
            "consumer_rkey": conf.AMQP_CONSUMER_ROUTING_KEY,
            "publisher_rkey": conf.AMQP_PUBLISHER_ROUTING_KEY,
            "concurrency": concurrency,
            "loglevel": loglevel,
            "pidfile": pidfile,
            "celerybeat": run_clockservice and "ON" or "OFF",
            "events": events and "ON" or "OFF",
    })

    print("Celery has started.")
    set_process_status("Running...")
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


    def run_worker():
        worker = WorkController(concurrency=concurrency,
                                loglevel=loglevel,
                                logfile=logfile,
                                embed_clockservice=run_clockservice,
                                send_events=events,
                                is_detached=detach)
        # Run the worker init handler.
        # (Usually imports task modules and such.)
        from celery import signals
        current_loader.on_worker_init()
        signals.worker_init.send(sender=worker)

        # Install signal handler that restarts celeryd on SIGHUP,
        # (only on POSIX systems)
        install_worker_restart_handler(worker)

        try:
            worker.start()
        except Exception, e:
            emergency_error(logfile, "celeryd raised exception %s: %s\n%s" % (
                            e.__class__, e, traceback.format_exc()))

    try:
        run_worker()
    except:
        set_process_status("Exiting...")
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


def set_process_status(info):
    arg_start = "manage" in sys.argv[0] and 2 or 1
    if sys.argv[arg_start:]:
        info = "%s (%s)" % (info, " ".join(sys.argv[arg_start:]))
    platform.set_mp_process_title("celeryd", info=info)


if __name__ == "__main__":
    options = parse_options(sys.argv[1:])
    run_worker(**vars(options))
