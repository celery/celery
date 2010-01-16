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

.. cmdoption:: -B, --beat

    Also run the ``celerybeat`` periodic task scheduler. Please note that
    there must only be one instance of this service.

.. cmdoption:: -E, --events

    Send events that can be captured by monitors like ``celerymon``.

.. cmdoption:: --discard

    Discard all waiting tasks before the daemon is started.
    **WARNING**: This is unrecoverable, and the tasks will be
    deleted from the messaging server.

"""
import os
import sys
import logging
import optparse
import traceback
import multiprocessing

from celery import conf
from celery import platform
from celery import __version__
from celery.log import emergency_error
from celery.task import discard_all
from celery.utils import noop
from celery.utils import info
from celery.worker import WorkController

STARTUP_INFO_FMT = """
Configuration ->
    . broker -> %(conninfo)s
    . queues ->
%(queues)s
    . concurrency -> %(concurrency)s
    . loader -> %(loader)s
    . logfile -> %(logfile)s@%(loglevel)s
    . events -> %(events)s
    . beat -> %(celerybeat)s
%(tasks)s
""".strip()

TASK_LIST_FMT = """    . tasks ->\n%s"""

OPTION_LIST = (
    optparse.make_option('-c', '--concurrency',
            default=conf.CELERYD_CONCURRENCY,
            action="store", dest="concurrency", type="int",
            help="Number of child processes processing the queue."),
    optparse.make_option('--discard', default=False,
            action="store_true", dest="discard",
            help="Discard all waiting tasks before the server is started. "
                 "WARNING: This is unrecoverable, and the tasks will be "
                 "deleted from the messaging server."),
    optparse.make_option('-f', '--logfile', default=conf.CELERYD_LOG_FILE,
            action="store", dest="logfile",
            help="Path to log file."),
    optparse.make_option('-l', '--loglevel', default=conf.CELERYD_LOG_LEVEL,
            action="store", dest="loglevel",
            help="Choose between DEBUG/INFO/WARNING/ERROR/CRITICAL/FATAL."),
    optparse.make_option('-B', '--beat', default=False,
            action="store_true", dest="run_clockservice",
            help="Also run the celerybeat periodic task scheduler. \
                  Please note that only one instance must be running."),
    optparse.make_option('-E', '--events', default=conf.SEND_EVENTS,
            action="store_true", dest="events",
            help="Send events so celery can be monitored by e.g. celerymon."),
)


def run_worker(concurrency=conf.CELERYD_CONCURRENCY,
        loglevel=conf.CELERYD_LOG_LEVEL, logfile=conf.CELERYD_LOG_FILE,
        discard=False, run_clockservice=False, events=False, **kwargs):
    """Starts the celery worker server."""

    print("Celery %s is starting." % __version__)

    from celery.loaders import current_loader, load_settings
    loader = current_loader()
    settings = load_settings()

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

    if discard:
        discarded_count = discard_all()
        what = discarded_count > 1 and "messages" or "message"
        print("discard: Erased %d %s from the queue.\n" % (
                discarded_count, what))

    # Run the worker init handler.
    # (Usually imports task modules and such.)
    loader.on_worker_init()

    # Dump configuration to screen so we have some basic information
    # when users sends e-mails.

    tasklist = ""
    if loglevel <= logging.INFO:
        from celery.registry import tasks
        tasklist = tasks.keys()
        if not loglevel <= logging.DEBUG:
            tasklist = filter(lambda s: not s.startswith("celery."), tasklist)
        tasklist = TASK_LIST_FMT % "\n".join("        . %s" % task
                                                for task in sorted(tasklist))

    print(STARTUP_INFO_FMT % {
            "conninfo": info.format_broker_info(),
            "queues": info.format_routing_table(indent=8),
            "concurrency": concurrency,
            "loglevel": conf.LOG_LEVELS[loglevel],
            "logfile": logfile or "[stderr]",
            "celerybeat": run_clockservice and "ON" or "OFF",
            "events": events and "ON" or "OFF",
            "tasks": tasklist,
            "loader": loader.__class__.__module__,
    })

    print("Celery has started.")
    set_process_status("Running...")

    def run_worker():
        worker = WorkController(concurrency=concurrency,
                                loglevel=loglevel,
                                logfile=logfile,
                                embed_clockservice=run_clockservice,
                                send_events=events)

        # Install signal handler so SIGHUP restarts the worker.
        install_worker_restart_handler(worker)

        from celery import signals
        signals.worker_init.send(sender=worker)

        try:
            worker.start()
        except Exception, e:
            emergency_error(logfile, "celeryd raised exception %s: %s\n%s" % (
                            e.__class__, e, traceback.format_exc()))

    try:
        run_worker()
    except:
        set_process_status("Exiting...")
        raise


def install_worker_restart_handler(worker):

    def restart_worker_sig_handler(signum, frame):
        """Signal handler restarting the current python program."""
        worker.logger.warn("Restarting celeryd (%s)" % (
            " ".join(sys.argv)))
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


def main():
    options = parse_options(sys.argv[1:])
    run_worker(**vars(options))

if __name__ == "__main__":
    main()
