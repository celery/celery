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

.. cmdoption:: -n, --hostname

    Set custom hostname.

.. cmdoption:: -B, --beat

    Also run the ``celerybeat`` periodic task scheduler. Please note that
    there must only be one instance of this service.

.. cmdoption:: -s, --schedule

    Path to the schedule database if running with the ``-B`` option.
    Defaults to ``celerybeat-schedule``. The extension ".db" will be
    appended to the filename.

.. cmdoption:: -E, --events

    Send events that can be captured by monitors like ``celerymon``.

.. cmdoption:: --discard

    Discard all waiting tasks before the daemon is started.
    **WARNING**: This is unrecoverable, and the tasks will be
    deleted from the messaging server.

"""
import os
import sys
import socket
import logging
import optparse
import warnings
import traceback
import multiprocessing

import celery
from celery import conf
from celery import signals
from celery import platform
from celery.log import emergency_error
from celery.task import discard_all
from celery.utils import info
from celery.utils import get_full_cls_name
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
    optparse.make_option('-n', '--hostname', default=None,
            action="store", dest="hostname",
            help="Set custom host name. E.g. 'foo.example.com'."),
    optparse.make_option('-B', '--beat', default=False,
            action="store_true", dest="run_clockservice",
            help="Also run the celerybeat periodic task scheduler. \
                  Please note that only one instance must be running."),
    optparse.make_option('-s', '--schedule',
            default=conf.CELERYBEAT_SCHEDULE_FILENAME,
            action="store", dest="schedule",
            help="Path to the schedule database if running with the -B \
                    option. The extension '.db' will be appended to the \
                    filename. Default: %s" % (
                    conf.CELERYBEAT_SCHEDULE_FILENAME)),
    optparse.make_option('-E', '--events', default=conf.SEND_EVENTS,
            action="store_true", dest="events",
            help="Send events so celery can be monitored by e.g. celerymon."),
    optparse.make_option('--time-limit',
            default=conf.CELERYD_TASK_TIME_LIMIT,
            action="store", type="int", dest="task_time_limit",
            help="Enables a hard time limit (in seconds) for task run times."),
    optparse.make_option('--soft-time-limit',
            default=conf.CELERYD_TASK_SOFT_TIME_LIMIT,
            action="store", type="int", dest="task_soft_time_limit",
            help="Enables a soft time limit (in seconds) for task run times.")
)


class Worker(object):

    def __init__(self, concurrency=conf.CELERYD_CONCURRENCY,
            loglevel=conf.CELERYD_LOG_LEVEL, logfile=conf.CELERYD_LOG_FILE,
            hostname=None, discard=False, run_clockservice=False,
            schedule=conf.CELERYBEAT_SCHEDULE_FILENAME,
            task_time_limit=conf.CELERYD_TASK_TIME_LIMIT,
            task_soft_time_limit=conf.CELERYD_TASK_SOFT_TIME_LIMIT,
            events=False, **kwargs):
        self.concurrency = concurrency or multiprocessing.cpu_count()
        self.loglevel = loglevel
        self.logfile = logfile
        self.hostname = hostname or socket.gethostname()
        self.discard = discard
        self.run_clockservice = run_clockservice
        self.schedule = schedule
        self.events = events
        self.task_time_limit = task_time_limit
        self.task_soft_time_limit = task_soft_time_limit
        if not isinstance(self.loglevel, int):
            self.loglevel = conf.LOG_LEVELS[self.loglevel.upper()]

    def run(self):
        print("celery@%s v%s is starting." % (self.hostname,
                                              celery.__version__))

        self.init_loader()

        if conf.RESULT_BACKEND == "database" \
                and self.settings.DATABASE_ENGINE == "sqlite3" and \
                self.concurrency > 1:
            warnings.warn("The sqlite3 database engine doesn't handle "
                          "concurrency well. Will use a single process only.",
                          UserWarning)
            self.concurrency = 1

        if getattr(self.settings, "DEBUG", False):
            warnings.warn("Using settings.DEBUG leads to a memory leak, "
                    "never use this setting in a production environment!")

        if self.discard:
            self.purge_messages()
        self.worker_init()

        # Dump configuration to screen so we have some basic information
        # for when users sends bug reports.
        print(self.startup_info())
        set_process_status("Running...")

        self.run_worker()

    def on_listener_ready(self, listener):
        signals.worker_ready.send(sender=listener)
        print("celery@%s has started." % self.hostname)

    def init_loader(self):
        from celery.loaders import current_loader, load_settings
        self.loader = current_loader()
        self.settings = load_settings()

    def purge_messages(self):
        discarded_count = discard_all()
        what = discarded_count > 1 and "messages" or "message"
        print("discard: Erased %d %s from the queue.\n" % (
            discarded_count, what))

    def worker_init(self):
        # Run the worker init handler.
        # (Usually imports task modules and such.)
        self.loader.init_worker()

    def tasklist(self, include_builtins=True):
        from celery.registry import tasks
        tasklist = tasks.keys()
        if not include_builtins:
            tasklist = filter(lambda s: not s.startswith("celery."),
                              tasklist)
        return TASK_LIST_FMT % "\n".join("\t. %s" % task
                                            for task in sorted(tasklist))

    def startup_info(self):
        tasklist = ""
        if self.loglevel <= logging.INFO:
            include_builtins = self.loglevel <= logging.DEBUG
            tasklist = self.tasklist(include_builtins=include_builtins)

        return STARTUP_INFO_FMT % {
            "conninfo": info.format_broker_info(),
            "queues": info.format_routing_table(indent=8),
            "concurrency": self.concurrency,
            "loglevel": conf.LOG_LEVELS[self.loglevel],
            "logfile": self.logfile or "[stderr]",
            "celerybeat": self.run_clockservice and "ON" or "OFF",
            "events": self.events and "ON" or "OFF",
            "tasks": tasklist,
            "loader": get_full_cls_name(self.loader.__class__),
        }

    def run_worker(self):
        worker = WorkController(concurrency=self.concurrency,
                                loglevel=self.loglevel,
                                logfile=self.logfile,
                                hostname=self.hostname,
                                ready_callback=self.on_listener_ready,
                                embed_clockservice=self.run_clockservice,
                                schedule_filename=self.schedule,
                                send_events=self.events,
                                task_time_limit=self.task_time_limit,
                                task_soft_time_limit=self.task_soft_time_limit)

        # Install signal handler so SIGHUP restarts the worker.
        install_worker_restart_handler(worker)
        install_worker_term_handler(worker)
        install_worker_int_handler(worker)

        signals.worker_init.send(sender=worker)
        try:
            worker.start()
        except Exception, exc:
            emergency_error(self.logfile,
                    "celeryd raised exception %s: %s\n%s" % (
                        exc.__class__, exc, traceback.format_exc()))


def install_worker_int_handler(worker):

    def _stop(signum, frame):
        process_name = multiprocessing.current_process().name
        if process_name == "MainProcess":
            worker.logger.warn("celeryd: Cold shutdown (%s)" % (
                                    process_name))
            worker.terminate()
        raise SystemExit()

    platform.install_signal_handler("SIGINT", _stop)


def install_worker_term_handler(worker):

    def _stop(signum, frame):
        process_name = multiprocessing.current_process().name
        if process_name == "MainProcess":
            worker.logger.warn("celeryd: Warm shutdown (%s)" % (
                process_name))
            worker.stop()
        raise SystemExit()

    platform.install_signal_handler("SIGTERM", _stop)


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


def run_worker(**options):
    return Worker(**options).run()


def main():
    options = parse_options(sys.argv[1:])
    return run_worker(**vars(options))

if __name__ == "__main__":
    main()
