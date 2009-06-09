#!/usr/bin/env python
"""celeryd

.. program:: celeryd

.. cmdoption:: -c, --concurrency

    Number of child processes processing the queue.

.. cmdoption:: -f, --logfile

    Path to log file. If no logfile is specified, ``stderr`` is used.

.. cmdoption:: -l, --loglevel

    Logging level, choose between ``DEBUG``, ``INFO``, ``WARNING``,
    ``ERROR``, ``CRITICAL``, or ``FATAL``.

.. cmdoption:: -p, --pidfile

    Path to pidfile.

.. cmdoption:: -w, --wakeup-after

    If the queue is empty, this is the time *in seconds* the
    daemon sleeps until it wakes up to check if there's any
    new messages on the queue.

.. cmdoption:: -d, --daemon

    Run in the background as a daemon.

.. cmdoption:: --discard

    Discard all waiting tasks before the daemon is started.
    **WARNING**: This is unrecoverable, and the tasks will be
    deleted from the messaging server.

"""
import os
import sys
sys.path.append(os.getcwd())
django_project_dir = os.environ.get("DJANGO_PROJECT_DIR")
if django_project_dir:
    sys.path.append(django_project_dir)

from django.conf import settings
from celery.platform import PIDFile, daemonize, remove_pidfile
from celery.log import emergency_error
from celery.conf import LOG_LEVELS, DAEMON_LOG_FILE, DAEMON_LOG_LEVEL
from celery.conf import DAEMON_CONCURRENCY, DAEMON_PID_FILE
from celery.conf import QUEUE_WAKEUP_AFTER
from celery import discovery
from celery.task import discard_all
from celery.worker import WorkController
import traceback
import optparse
import atexit


def main(concurrency=DAEMON_CONCURRENCY, daemon=False,
        loglevel=DAEMON_LOG_LEVEL, logfile=DAEMON_LOG_FILE, discard=False,
        pidfile=DAEMON_PID_FILE, queue_wakeup_after=QUEUE_WAKEUP_AFTER):
    """Run the celery daemon."""
    if settings.DATABASE_ENGINE == "sqlite3" and concurrency > 1:
        import warnings
        warnings.warn("The sqlite3 database engine doesn't support "
                "concurrency. We'll be using a single process only.",
                UserWarning)
        concurrency = 1

    if discard:
        discarded_count = discard_all()
        what = "message"
        if discarded_count > 1:
            what = "messages"
        sys.stderr.write("Discard: Erased %d %s from the queue.\n" % (
            discarded_count, what))
    if daemon:
        sys.stderr.write("Launching celeryd in the background...\n")
        pidfile_handler = PIDFile(pidfile)
        pidfile_handler.check()
        daemonize(pidfile=pidfile_handler)
        atexit.register(remove_pidfile, pidfile)
    else:
        logfile = None # log to stderr when not running as daemon.

    discovery.autodiscover()
    celeryd = WorkController(concurrency=concurrency,
                               loglevel=loglevel,
                               logfile=logfile,
                               queue_wakeup_after=queue_wakeup_after,
                               is_detached=daemon)
    try:
        celeryd.run()
    except Exception, e:
        emergency_error(logfile, "celeryd raised exception %s: %s\n%s" % (
                            e.__class__, e, traceback.format_exc()))


OPTION_LIST = (
    optparse.make_option('-c', '--concurrency', default=DAEMON_CONCURRENCY,
            action="store", dest="concurrency", type="int",
            help="Number of child processes processing the queue."),
    optparse.make_option('--discard', default=False,
            action="store_true", dest="discard",
            help="Discard all waiting tasks before the daemon is started. "
                 "WARNING: This is unrecoverable, and the tasks will be "
                 "deleted from the messaging server."),
    optparse.make_option('-f', '--logfile', default=DAEMON_LOG_FILE,
            action="store", dest="logfile",
            help="Path to log file."),
    optparse.make_option('-l', '--loglevel', default=DAEMON_LOG_LEVEL,
            action="store", dest="loglevel",
            help="Choose between DEBUG/INFO/WARNING/ERROR/CRITICAL/FATAL."),
    optparse.make_option('-p', '--pidfile', default=DAEMON_PID_FILE,
            action="store", dest="pidfile",
            help="Path to pidfile."),
    optparse.make_option('-w', '--wakeup-after', default=QUEUE_WAKEUP_AFTER,
            action="store", type="float", dest="queue_wakeup_after",
            help="If the queue is empty, this is the time *in seconds* the "
                 "daemon sleeps until it wakes up to check if there's any "
                 "new messages on the queue."),
    optparse.make_option('-d', '--daemon', default=False,
            action="store_true", dest="daemon",
            help="Run in the background as a daemon."),
)


def parse_options(arguments):
    """Option parsers for the available options to ``celeryd``."""
    parser = optparse.OptionParser(option_list=OPTION_LIST)
    options, values = parser.parse_args(arguments)
    if not isinstance(options.loglevel, int):
        options.loglevel = LOG_LEVELS[options.loglevel.upper()]
    return options

if __name__ == "__main__":
    options = parse_options(sys.argv[1:])
    main(concurrency=options.concurrency,
         daemon=options.daemon,
         logfile=options.logfile,
         loglevel=options.loglevel,
         pidfile=options.pidfile,
         discard=options.discard,
         queue_wakeup_after=options.queue_wakeup_after)
