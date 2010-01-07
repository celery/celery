import os
import sys
import pwd
import grp
import errno
import atexit
import signal
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

DAEMON_UMASK = 0
DAEMON_WORKDIR = "/"
DAEMON_MAXFD = 1024
DAEMON_REDIRECT_TO = getattr(os, "devnull", "/dev/null")


def create_pidlock(pidfile):
    """Create a PIDFile to be used with python-daemon.

    If the pidfile already exists the program exits with an error message,
    however if the process it refers to is not running anymore, the pidfile
    is just deleted.

    """
    from daemon import pidlockfile
    from lockfile import LockFailed

    class PIDFile(object):

        def __init__(self, path):
            self.path = os.path.abspath(path)

        def __enter__(self):
            try:
                pidlockfile.write_pid_to_pidfile(self.path)
            except OSError, exc:
                raise LockFailed(str(exc))
            return self

        def __exit__(self, *_exc):
            self.release()

        def is_locked(self):
            return os.path.exists(self.path)

        def release(self):
            try:
                os.unlink(self.path)
            except OSError, exc:
                if exc.errno in (errno.ENOENT, errno.EACCES):
                    return
                raise

        def read_pid(self):
            return pidlockfile.read_pid_from_pidfile(self.path)

        def is_stale(self):
            pid = self.read_pid()
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

    pidlock = PIDFile(pidfile)
    if pidlock.is_locked() and not pidlock.is_stale():
        raise SystemExit(
                "ERROR: Pidfile (%s) already exists.\n"
                "Seems we're already running? (PID: %d)" % (
                    pidfile, pidlock.read_pid()))
    return pidlock


class DaemonContext(object):
    _is_open = False

    def __init__(self, pidfile=None, chroot_directory=None, 
            working_directory=DAEMON_WORKDIR, umask=DAEMON_UMASK, **kwargs):
        self.pidfile = pidfile
        self.chroot_directory = chroot_directory
        self.working_directory = working_directory
        self.umask = umask

    def detach(self):
        if os.fork() == 0: # first child
            os.setsid() # create new session.
            if os.fork() > 0: # second child
                os._exit(0)
        else:
            os._exit(0)

    def open(self):
        from daemon import daemon
        if self._is_open:
            return

        self.detach()

        if self.pidfile is not None:
                self.pidfile.__enter__()

        if self.chroot_directory is not None:
            daemon.change_root_directory(self.chroot_directory)
        os.chdir(self.working_directory)
        os.umask(self.umask)

        daemon.close_all_open_files()

        os.open(DAEMON_REDIRECT_TO, os.O_RDWR)
        os.dup2(0, 1)
        os.dup2(0, 2)

        self._is_open = True
        atexit.register(self.close)

    def close(self):
        if not self._is_open:
            return
        if self.pidfile is not None:
            self.pidfile.__exit__()
        self._is_open = False


def create_daemon_context(logfile=None, pidfile=None, **options):
    if not CAN_DETACH:
        raise RuntimeError(
                "This operating system doesn't support detach.")

    # set SIGCLD back to the default SIG_DFL (before python-daemon overrode
    # it) lets the parent wait() for the terminated child process and stops
    # the 'OSError: [Errno 10] No child processes' problem.
    reset_signal("SIGCLD")

    # Since without stderr any errors will be silently suppressed,
    # we need to know that we have access to the logfile
    if logfile:
        open(logfile, "a").close()

    options["pidfile"] = pidfile and create_pidlock(pidfile)

    defaults = {"umask": lambda: 0,
                "chroot_directory": lambda: None,
                "working_directory": lambda: os.getcwd()}

    for opt_name, opt_default_gen in defaults.items():
        if opt_name not in options or options[opt_name] is None:
            options[opt_name] = opt_default_gen()

    context = DaemonContext(**options)

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
