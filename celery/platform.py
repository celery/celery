import os
import sys
import pwd
import grp
import signal
from contextlib import contextmanager
try:
    from setproctitle import setproctitle as _setproctitle
except ImportError:
    _setproctitle = None


CAN_DETACH = True
try:
    import resource
except ImportError:
    CAN_DETACH = False

from celery.utils import noop


def maybe_remove_file(path, ignore_perm_denied=False):
    """Try to unlink file, but don't care if it doesn't exist.

    :param path: The path of the file to remove.
    :keyword ignore_perm_denied: Ignore permission denied errors.

    """
    try:
        os.unlink(path)
    except OSError, exc:
        if exc.errno == errno.ENOENT:
            return
        if exc.errno == errno.EACCES and ignore_perm_denied:
            return
        raise


@contextmanager
def pidlockfile(pidfile):
    from daemon import pidlockfile
    import lockfile

    try:
        pidlockfile.write_pid_to_pidfile(pidfile)
    except OSError, exc:
        raise lockfile.LockFailed(str(exc))

    yield

    maybe_remove_file(pidfile, ignore_perm_denied=True)


def acquire_pidlock(pidfile):
    """Get the :class:`daemon.pidlockfile.PIDLockFile` handler for
    ``pidfile``.

    If the ``pidfile`` already exists, but the process is not running the
    ``pidfile`` will be removed, a ``"stale pidfile"`` message is emitted
    and execution continues as normally. However, if the process is still
    running the program will exit complaning that the program is already
    running in the background somewhere.

    """
    from daemon import pidlockfile
    from lockfile import LinkFileLock, LockFailed
    import errno

    class SafeRemovePIDLockFile(pidlockfile.PIDLockFile):

        def __init__(self, path, threaded=True):
            self.abspath = os.path.abspath(path)
            super(SafeRemovePIDLockFile, self).__init__(path, threaded)

        def is_locked(self):
            return os.path.exists(self.abspath)

        def i_am_locking(self):
            return self.is_locked()

        def acquire(self, timeout=None):
            try:
                pidlockfile.write_pid_to_pidfile(self.abspath)
            except OSError, exc:
                raise LockFailed(str(exc))

        def release(self):
            maybe_remove_file(self.abspath, ignore_perm_denied=True)

        def break_lock(self):
            self.release()

        def is_stale(self):
            pid = pidlock.read_pid()
            try:
                os.kill(pid, 0)
            except os.error, exc:
                if exc.errno == errno.ESRCH:
                    sys.stderr.write("Stale pidfile exists. Removing it.\n")
                    self.release()
                    return True
            except TypeError, exc:
                sys.stderr.write("Broken pidfile found. Removing it.\n")
                self.release()
                return True
            return False

    pidlock = SafeRemovePIDLockFile(pidfile)
    if pidlock.is_locked() and not pidlock.is_stale():
        raise SystemExit(
                "ERROR: Pidfile (%s) already exists.\n"
                "Seems celeryd is already running? (PID: %d)" % (
                    pidfile, pid))
    return pidlock


def create_daemon_context(logfile=None, pidfile=None, **options):
    if not CAN_DETACH:
        raise RuntimeError(
                "This operating system doesn't support detach.")

    import daemon
    daemon.change_process_owner = noop # We handle our own user change.

    # set SIGCLD back to the default SIG_DFL (before python-daemon overrode
    # it) lets the parent wait() for the terminated child process and stops
    # the 'OSError: [Errno 10] No child processes' problem.
    reset_signal("SIGCLD")

    # Since without stderr any errors will be silently suppressed,
    # we need to know that we have access to the logfile
    if logfile:
        open(logfile, "a").close()

    options["pidfile"] = pidfile and acquire_pidlock(pidfile)

    defaults = {"umask": lambda: 0,
                "chroot_directory": lambda: None,
                "working_directory": lambda: os.getcwd()}

    for opt_name, opt_default_gen in defaults.items():
        if opt_name not in options or options[opt_name] is None:
            options[opt_name] = opt_default_gen()

    context = daemon.DaemonContext(**options)

    return context, context.close


def reset_signal(signal_name):
    """Reset signal to the default signal handler.

    Does nothing if the platform doesn't support signals,
    or the specified signal in particular.

    """
    if hasattr(signal, signal_name):
        signal.signal(getattr(signal, signal_name), signal.SIG_DFL)


def install_signal_handler(signal_name, handler):
    """Install a handler.

    Does nothing if the current platform doesn't support signals,
    or the specified signal in particular.

    """
    if not hasattr(signal, signal_name):
        return

    signum = getattr(signal, signal_name)
    signal.signal(signum, handler)


def set_process_title(progname, info=None):
    """Set the ps name for the currently running process
    if :mod`setproctitle` is installed."""
    if _setproctitle:
        proctitle = "[%s]" % progname
        proctitle = info and "%s %s" % (proctitle, info) or proctitle
        _setproctitle(proctitle)


def set_mp_process_title(progname, info=None):
    """Set the ps name using the multiprocessing process name.

    Only works if :mod:`setproctitle` is installed.

    """
    from multiprocessing.process import current_process
    return set_process_title("%s.%s" % (progname, current_process().name),
                             info=info)


def parse_uid(uid):
    """Parse user uid.

    uid can be an integer (uid) or a string (username), if it's a username
    the uid is taken from the system password system.

    """
    try:
        return int(uid)
    except ValueError:
        return pwd.getpwnam(uid).pw_uid


def parse_gid(gid):
    """Parse group gid.

    gid can be an integer (gid) or a string (group name), if it's a name
    the gid is taken from the system password system.

    """
    try:
        return int(gid)
    except ValueError:
        return grp.getgrnam(gid).gr_gid


def setegid(gid):
    """Set effective group id."""
    gid = parse_uid(gid)
    if gid != os.getgid():
        os.setegid(gid)


def seteuid(uid):
    """Set effective user id."""
    uid = parse_uid(uid)
    if uid != os.getuid():
        os.seteuid(uid)


def set_effective_user(uid=None, gid=None):
    """Change privileges to a new user/group.

    If uid and gid is set the effective user/group is set.

    If only uid is set, the effective user is set, and the group is set
    to the users primary group.

    If only gid is set, the effective group is set.

    """
    # gid/uid can be int or username/groupname.
    uid = uid and parse_uid(uid)
    gid = gid and parse_gid(gid)

    if uid:
        # If gid isn't defined, get the primary gid of the user.
        setegid(gid or pwd.getpwuid(uid).pw_gid)
        seteuid(uid)
    else:
        gid and setegid(gid)
