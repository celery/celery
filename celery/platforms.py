from __future__ import absolute_import

import os
import sys
import errno
import signal as _signal

from celery.local import try_import

_setproctitle = try_import("setproctitle")
resource = try_import("resource")
pwd = try_import("pwd")
grp = try_import("grp")

DAEMON_UMASK = 0
DAEMON_WORKDIR = "/"
DAEMON_REDIRECT_TO = getattr(os, "devnull", "/dev/null")


class LockFailed(Exception):
    pass


def get_fdmax(default=None):
    fdmax = resource.getrlimit(resource.RLIMIT_NOFILE)[1]
    if fdmax == resource.RLIM_INFINITY:
        return default
    return fdmax


class PIDFile(object):

    def __init__(self, path):
        self.path = os.path.abspath(path)

    def write_pid(self):
        open_flags = (os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        open_mode = (((os.R_OK | os.W_OK) << 6) |
                        ((os.R_OK) << 3) |
                        ((os.R_OK)))
        pidfile_fd = os.open(self.path, open_flags, open_mode)
        pidfile = os.fdopen(pidfile_fd, "w")
        pid = os.getpid()
        pidfile.write("%d\n" % (pid, ))
        pidfile.close()

    def acquire(self):
        try:
            self.write_pid()
        except OSError, exc:
            raise LockFailed(str(exc))
        return self
    __enter__ = acquire

    def is_locked(self):
        return os.path.exists(self.path)

    def release(self, *args):
        self.remove()
    __exit__ = release

    def read_pid(self):
        try:
            fh = open(self.path, "r")
        except IOError, exc:
            if exc.errno == errno.ENOENT:
                return
            raise

        line = fh.readline().strip()
        fh.close()

        try:
            return int(line)
        except ValueError:
            raise ValueError("PID file %r contents invalid." % self.path)

    def remove(self):
        try:
            os.unlink(self.path)
        except OSError, exc:
            if exc.errno in (errno.ENOENT, errno.EACCES):
                return
            raise

    def remove_if_stale(self):
        try:
            pid = self.read_pid()
        except ValueError, exc:
            sys.stderr.write("Broken pidfile found. Removing it.\n")
            self.remove()
            return True
        if not pid:
            self.remove()
            return True

        try:
            os.kill(pid, 0)
        except os.error, exc:
            if exc.errno == errno.ESRCH:
                sys.stderr.write("Stale pidfile exists. Removing it.\n")
                self.remove()
                return True
        return False


def create_pidlock(pidfile):
    """Create and verify pidfile.

    If the pidfile already exists the program exits with an error message,
    however if the process it refers to is not running anymore, the pidfile
    is just deleted.

    """

    pidlock = PIDFile(pidfile)
    if pidlock.is_locked() and not pidlock.remove_if_stale():
        raise SystemExit(
                "ERROR: Pidfile (%s) already exists.\n"
                "Seems we're already running? (PID: %s)" % (
                    pidfile, pidlock.read_pid()))
    return pidlock


class DaemonContext(object):
    _is_open = False

    def __init__(self, pidfile=None, workdir=DAEMON_WORKDIR,
            umask=DAEMON_UMASK, **kwargs):
        self.workdir = workdir
        self.umask = umask

    def open(self):
        if not self._is_open:
            self._detach()

            os.chdir(self.workdir)
            os.umask(self.umask)

            for fd in reversed(range(get_fdmax(default=2048))):
                try:
                    os.close(fd)
                except OSError, exc:
                    if exc.errno != errno.EBADF:
                        raise

            os.open(DAEMON_REDIRECT_TO, os.O_RDWR)
            os.dup2(0, 1)
            os.dup2(0, 2)

            self._is_open = True
    __enter__ = open

    def close(self, *args):
        if self._is_open:
            self._is_open = False
    __exit__ = close

    def _detach(self):
        if os.fork() == 0:      # first child
            os.setsid()         # create new session
            if os.fork() > 0:   # second child
                os._exit(0)
        else:
            os._exit(0)
        return self


def detached(logfile=None, pidfile=None, uid=None, gid=None, umask=0,
             workdir=None, **opts):
    if not resource:
        raise RuntimeError("This platform does not support detach.")
    workdir = os.getcwd() if workdir is None else workdir

    signals.reset("SIGCLD")  # Make sure SIGCLD is using the default handler.
    set_effective_user(uid=uid, gid=gid)

    # Since without stderr any errors will be silently suppressed,
    # we need to know that we have access to the logfile.
    logfile and open(logfile, "a").close()
    # Doesn't actually create the pidfile, but makes sure it's not stale.
    pidfile and create_pidlock(pidfile)

    return DaemonContext(umask=umask, workdir=workdir)


def parse_uid(uid):
    """Parse user id.

    uid can be an interger (uid) or a string (username), if a username
    the uid is taken from the password file.

    """
    try:
        return int(uid)
    except ValueError:
        if pwd:
            try:
                return pwd.getpwnam(uid).pw_uid
            except KeyError:
                raise KeyError("User does not exist: %r" % (uid, ))
        raise


def parse_gid(gid):
    """Parse group id.

    gid can be an integer (gid) or a string (group name), if a group name
    the gid is taken from the password file.

    """
    try:
        return int(gid)
    except ValueError:
        if grp:
            try:
                return grp.getgrnam(gid).gr_gid
            except KeyError:
                raise KeyError("Group does not exist: %r" % (gid, ))
        raise


def setegid(gid):
    """Set effective group id."""
    gid = parse_gid(gid)
    if gid != os.getgid():
        os.setegid(gid)


def seteuid(uid):
    """Set effective user id."""
    uid = parse_uid(uid)
    if uid != os.getuid():
        os.seteuid(uid)


def set_effective_user(uid=None, gid=None):
    """Change process privileges to new user/group.

    If uid and gid is set the effective user/group is set.

    If only uid is set, the effective uer is set, and the group is
    set to the users primary group.

    If only gid is set, the effective group is set.

    """
    uid = uid and parse_uid(uid)
    gid = gid and parse_gid(gid)

    if uid:
        # If gid isn't defined, get the primary gid of the uer.
        if not gid and pwd:
            gid = pwd.getpwuid(uid).pw_gid
        setegid(gid)
        seteuid(uid)
    else:
        gid and setegid(gid)


class Signals(object):
    ignored = _signal.SIG_IGN
    default = _signal.SIG_DFL

    def supported(self, signal_name):
        """Returns true value if ``signal_name`` exists on this platform."""
        try:
            return self.signum(signal_name)
        except AttributeError:
            pass

    def signum(self, signal_name):
        """Get signal number from signal name."""
        if isinstance(signal_name, int):
            return signal_name
        if not isinstance(signal_name, basestring) \
                or not signal_name.isupper():
            raise TypeError("signal name must be uppercase string.")
        if not signal_name.startswith("SIG"):
            signal_name = "SIG" + signal_name
        return getattr(_signal, signal_name)

    def reset(self, *signal_names):
        """Reset signals to the default signal handler.

        Does nothing if the platform doesn't support signals,
        or the specified signal in particular.

        """
        self.update((sig, self.default) for sig in signal_names)

    def ignore(self, *signal_names):
        """Ignore signal using :const:`SIG_IGN`.

        Does nothing if the platform doesn't support signals,
        or the specified signal in particular.

        """
        self.update((sig, self.ignored) for sig in signal_names)

    def __getitem__(self, signal_name):
        return _signal.getsignal(self.signum(signal_name))

    def __setitem__(self, signal_name, handler):
        """Install signal handler.

        Does nothing if the current platform doesn't support signals,
        or the specified signal in particular.

        """
        try:
            _signal.signal(self.signum(signal_name), handler)
        except (AttributeError, ValueError):
            pass

    def update(self, _d_=None, **sigmap):
        """Set signal handlers from a mapping."""
        for signal_name, handler in dict(_d_ or {}, **sigmap).iteritems():
            self[signal_name] = handler


signals = Signals()
get_signal = signals.signum                   # compat
install_signal_handler = signals.__setitem__  # compat
reset_signal = signals.reset                  # compat
ignore_signal = signals.ignore                # compat


def strargv(argv):
    arg_start = 2 if "manage" in argv[0] else 1
    if len(argv) > arg_start:
        return " ".join(argv[arg_start:])
    return ""


def set_process_title(progname, info=None):
    """Set the ps name for the currently running process.

    Only works if :mod:`setproctitle` is installed.

    """
    proctitle = "[%s]" % progname
    proctitle = "%s %s" % (proctitle, info) if info else proctitle
    if _setproctitle:
        _setproctitle.setproctitle(proctitle)
    return proctitle


def set_mp_process_title(progname, info=None, hostname=None):
    """Set the ps name using the multiprocessing process name.

    Only works if :mod:`setproctitle` is installed.

    """
    if hostname:
        progname = "%s@%s" % (progname, hostname.split(".")[0])
    try:
        from multiprocessing.process import current_process
    except ImportError:
        return set_process_title(progname, info=info)
    else:
        return set_process_title("%s:%s" % (progname,
                                            current_process().name), info=info)
