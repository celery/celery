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
from celery.conf import QUEUE_WAKEUP_AFTER
from celery import discovery
from celery.task import discard_all
from celery.worker import WorkController
import traceback
import optparse
import atexit
from daemon import DaemonContext
from daemon.pidlockfile import PIDLockFile


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
            return
    else:
        raise SystemExit(
                "ERROR: Pidfile (%s) already exists.\n"
                "Seems celeryd is already running? (PID: %d)" % (
                    pidfile, pid))
    return pidlock        


def run_worker(concurrency=DAEMON_CONCURRENCY, daemon=False,
        loglevel=DAEMON_LOG_LEVEL, logfile=DAEMON_LOG_FILE, discard=False,
        pidfile=DAEMON_PID_FILE, queue_wakeup_after=QUEUE_WAKEUP_AFTER,
        umask=0, uid=None, gid=None, working_directory=None, chroot=None,
        **kwargs):
    """Run the celery daemon."""
    if settings.DATABASE_ENGINE == "sqlite3" and concurrency > 1:
        import warnings
        warnings.warn("The sqlite3 database engine doesn't support "
                "concurrency. We'll be using a single process only.",
                UserWarning)
        concurrency = 1
    
    if not isinstance(loglevel, int):
        loglevel = LOG_LEVELS[loglevel.upper()]

    if discard:
        discarded_count = discard_all()
        what = "message"
        if discarded_count > 1:
            what = "messages"
        sys.stderr.write("Discard: Erased %d %s from the queue.\n" % (
            discarded_count, what))
    if daemon:
        # Since without stderr any errors will be silently suppressed,
        # we need to know that we have access to the logfile
        pidlock = acquire_pidlock(pidfile)
        if not umask:
            umask = 0
        if logfile:
            open(logfile, "a").close()
        uid = uid and int(uid) or os.geteuid()
        gid = gid and int(gid) or os.getegid()
        working_directory = working_directory or os.getcwd()
        sys.stderr.write("Launching celeryd in the background...\n")
        context = DaemonContext(chroot_directory=chroot,
                                working_directory=working_directory,
                                umask=umask,
                                pidfile=pidlock,
                                uid=uid,
                                gid=gid)
        context.open()
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
    except:
        context.close()
        raise


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
    optparse.make_option('-d', '--detach', '--daemon', default=False,
            action="store_true", dest="daemon",
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


def parse_options(arguments):
    """Parse the available options to ``celeryd``."""
    parser = optparse.OptionParser(option_list=OPTION_LIST)
    options, values = parser.parse_args(arguments)
    return options


if __name__ == "__main__":
    options = parse_options(sys.argv[1:])
    run_worker(**options)
