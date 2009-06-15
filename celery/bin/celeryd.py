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
sys.path.append(os.getcwd())
django_project_dir = os.environ.get("DJANGO_PROJECT_DIR")
if django_project_dir:
    sys.path.append(django_project_dir)

from django.conf import settings
from celery.log import emergency_error
from celery.conf import LOG_LEVELS, DAEMON_LOG_FILE, DAEMON_LOG_LEVEL
from celery.conf import DAEMON_CONCURRENCY, DAEMON_PID_FILE
from celery.log import setup_logger
from celery.messaging import TaskConsumer
from celery import conf
from celery import discovery
from celery.task import discard_all
from celery.worker import WorkController
import multiprocessing
import traceback
import optparse
import atexit
from daemon import DaemonContext
from daemon.pidlockfile import PIDLockFile
import errno

STARTUP_INFO_FMT = """
    * Celery loading with the following configuration
        * Broker -> amqp://%(vhost)s@%(host)s:%(port)s
        * Exchange -> %(exchange)s (%(exchange_type)s)
        * Consumer -> Queue:%(consumer_queue)s Routing:%(consumer_rkey)s
        * Concurrency:%(concurrency)s
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


def acquire_pidlock(pidfile):
    """Get the :class:`daemon.pidlockfile.PIDLockFile` handler for
    ``pidfile``.

    If the ``pidfile`` already exists, but the process is not running the
    ``pidfile`` will be removed, a ``"stale pidfile"`` message is emitted
    and execution continues as normally. However, if the process is still
    running the program will exit complaning that the program is already
    running in the background somewhere.

    """
    pidlock = PIDLockFile(pidfile)
    if not pidlock.is_locked():
        return pidlock
    pid = pidlock.read_pid()
    try:
        os.kill(pid, 0)
    except os.error, exc:
        if exc.errno == errno.ESRCH:
            sys.stderr.write("Stale pidfile exists. Removing it.\n")
            pidlock.release()
            return PIDLockFile(pidfile)
    else:
        raise SystemExit(
                "ERROR: Pidfile (%s) already exists.\n"
                "Seems celeryd is already running? (PID: %d)" % (
                    pidfile, pid))
    return pidlock


def run_worker(concurrency=DAEMON_CONCURRENCY, detach=False,
        loglevel=DAEMON_LOG_LEVEL, logfile=DAEMON_LOG_FILE, discard=False,
        pidfile=DAEMON_PID_FILE, umask=0, uid=None, gid=None,
        working_directory=None, chroot=None, **kwargs):
    """Starts the celery worker server."""

    if not concurrency:
        concurrency = multiprocessing.cpu_count()
    if settings.DATABASE_ENGINE == "sqlite3" and concurrency > 1:
        import warnings
        warnings.warn("The sqlite3 database engine doesn't support "
                "concurrency. We'll be using a single process only.",
                UserWarning)
        concurrency = 1

    # Setup logging
    if not isinstance(loglevel, int):
        loglevel = LOG_LEVELS[loglevel.upper()]
    if not detach:
        logfile = None # log to stderr when not running in the background.
    logger = setup_logger(logfile=logfile, loglevel=loglevel)

    def say(msg):
        """Log the message using loglevel ``INFO`` if running in the
        background, else just print the message to ``stdout``."""
        if detach:
            return logger.info(msg)
        print(msg)

    if discard:
        discarded_count = discard_all()
        what = discarded_count > 1 and "messages" or "message"
        say("discard: Erased %d %s from the queue.\n" % (
                discarded_count, what))

    # Dump configuration to screen so we have some basic information
    # when users sends e-mails.
    say(STARTUP_INFO_FMT % {
            "vhost": settings.AMQP_VHOST,
            "host": settings.AMQP_SERVER,
            "port": settings.AMQP_PORT,
            "exchange": conf.AMQP_EXCHANGE,
            "exchange_type": conf.AMQP_EXCHANGE_TYPE,
            "consumer_queue": conf.AMQP_CONSUMER_QUEUE,
            "consumer_rkey": conf.AMQP_CONSUMER_ROUTING_KEY,
            "publisher_rkey": conf.AMQP_PUBLISHER_ROUTING_KEY,
            "concurrency": concurrency,
            "loglevel": loglevel,
            "pidfile": pidfile,
    })

    if detach:
        # Since without stderr any errors will be silently suppressed,
        # we need to know that we have access to the logfile
        if logfile:
            open(logfile, "a").close()
        pidlock = acquire_pidlock(pidfile)
        if not umask:
            umask = 0
        uid = uid and int(uid) or os.geteuid()
        gid = gid and int(gid) or os.getegid()
        working_directory = working_directory or os.getcwd()
        print("* Launching celeryd in the background...")
        context = DaemonContext(chroot_directory=chroot,
                                working_directory=working_directory,
                                umask=umask,
                                pidfile=pidlock,
                                uid=uid,
                                gid=gid)
        context.open()

    discovery.autodiscover()
    worker = WorkController(concurrency=concurrency,
                            loglevel=loglevel,
                            logfile=logfile,
                            is_detached=detach)
    try:
        worker.run()
    except Exception, e:
        emergency_error(logfile, "celeryd raised exception %s: %s\n%s" % (
                            e.__class__, e, traceback.format_exc()))
    except:
        if context:
            context.close()
        raise


def parse_options(arguments):
    """Parse the available options to ``celeryd``."""
    parser = optparse.OptionParser(option_list=OPTION_LIST)
    options, values = parser.parse_args(arguments)
    return options


if __name__ == "__main__":
    options = parse_options(sys.argv[1:])
    run_worker(**options)
