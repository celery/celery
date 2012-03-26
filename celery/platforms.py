# -*- coding: utf-8 -*-
"""
    celery.platforms
    ~~~~~~~~~~~~~~~~

    Utilities dealing with platform specifics: signals, daemonization,
    users, groups, and so on.

    :copyright: (c) 2009 - 2012 by Ask Solem.
    :license: BSD, see LICENSE for more details.

"""
from __future__ import absolute_import
from __future__ import with_statement

import errno
import os
import platform as _platform
import shlex
import signal as _signal
import sys

from .local import try_import

from kombu.utils.limits import TokenBucket

_setproctitle = try_import("setproctitle")
resource = try_import("resource")
pwd = try_import("pwd")
grp = try_import("grp")

EX_OK = getattr(os, "EX_OK", 0)
EX_FAILURE = 1
EX_UNAVAILABLE = getattr(os, "EX_UNAVAILABLE", 69)
EX_USAGE = getattr(os, "EX_USAGE", 64)

try:
    from multiprocessing.process import current_process
except ImportError:
    current_process = None  # noqa

SYSTEM = _platform.system()
IS_OSX = SYSTEM == "Darwin"
IS_WINDOWS = SYSTEM == "Windows"

DAEMON_UMASK = 0
DAEMON_WORKDIR = "/"
DAEMON_REDIRECT_TO = getattr(os, "devnull", "/dev/null")

_setps_bucket = TokenBucket(0.5)  # 30/m, every 2 seconds


def pyimplementation():
    if hasattr(_platform, "python_implementation"):
        return _platform.python_implementation()
    elif sys.platform.startswith("java"):
        return "Jython " + sys.platform
    elif hasattr(sys, "pypy_version_info"):
        v = ".".join(map(str, sys.pypy_version_info[:3]))
        if sys.pypy_version_info[3:]:
            v += "-" + "".join(map(str, sys.pypy_version_info[3:]))
        return "PyPy " + v
    else:
        return "CPython"


class LockFailed(Exception):
    """Raised if a pidlock can't be acquired."""
    pass


def get_fdmax(default=None):
    """Returns the maximum number of open file descriptors
    on this system.

    :keyword default: Value returned if there's no file
                      descriptor limit.

    """
    fdmax = resource.getrlimit(resource.RLIMIT_NOFILE)[1]
    if fdmax == resource.RLIM_INFINITY:
        return default
    return fdmax


class PIDFile(object):
    """PID lock file.

    This is the type returned by :func:`create_pidlock`.

    **Should not be used directly, use the :func:`create_pidlock`
    context instead**

    """

    #: Path to the pid lock file.
    path = None

    def __init__(self, path):
        self.path = os.path.abspath(path)

    def acquire(self):
        """Acquire lock."""
        try:
            self.write_pid()
        except OSError, exc:
            raise LockFailed, LockFailed(str(exc)), sys.exc_info()[2]
        return self
    __enter__ = acquire

    def is_locked(self):
        """Returns true if the pid lock exists."""
        return os.path.exists(self.path)

    def release(self, *args):
        """Release lock."""
        self.remove()
    __exit__ = release

    def read_pid(self):
        """Reads and returns the current pid."""
        try:
            fh = open(self.path, "r")
        except IOError, exc:
            if exc.errno == errno.ENOENT:
                return
            raise

        try:
            line = fh.readline()
            if line.strip() == line:  # must contain '\n'
                raise ValueError(
                    "Partially written or invalid pidfile %r" % (self.path))
        finally:
            fh.close()

        try:
            return int(line.strip())
        except ValueError:
            raise ValueError("PID file %r contents invalid." % self.path)

    def remove(self):
        """Removes the lock."""
        try:
            os.unlink(self.path)
        except OSError, exc:
            if exc.errno in (errno.ENOENT, errno.EACCES):
                return
            raise

    def remove_if_stale(self):
        """Removes the lock if the process is not running.
        (does not respond to signals)."""
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

    def write_pid(self):
        pid = os.getpid()
        content = "%d\n" % (pid, )

        open_flags = (os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        open_mode = (((os.R_OK | os.W_OK) << 6) |
                        ((os.R_OK) << 3) |
                        ((os.R_OK)))
        pidfile_fd = os.open(self.path, open_flags, open_mode)
        pidfile = os.fdopen(pidfile_fd, "w")
        try:
            pidfile.write(content)
            # flush and sync so that the re-read below works.
            pidfile.flush()
            try:
                os.fsync(pidfile_fd)
            except AttributeError:
                pass
        finally:
            pidfile.close()

        with open(self.path) as fh:
            if fh.read() != content:
                raise LockFailed(
                    "Inconsistency: Pidfile content doesn't match at re-read")


def create_pidlock(pidfile):
    """Create and verify pid file.

    If the pid file already exists the program exits with an error message,
    however if the process it refers to is not running anymore, the pid file
    is deleted and the program continues.

    The caller is responsible for releasing the lock before the program
    exits.

    :returns: :class:`PIDFile`.

    **Example**:

    .. code-block:: python

        import atexit
        pidlock = create_pidlock("/var/run/app.pid").acquire()
        atexit.register(pidlock.release)

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
    workdir = DAEMON_WORKDIR
    umask = DAEMON_UMASK

    def __init__(self, pidfile=None, workdir=None, umask=None,
            fake=False, **kwargs):
        self.workdir = workdir or self.workdir
        self.umask = self.umask if umask is None else umask
        self.fake = fake

    def open(self):
        if not self._is_open:
            if not self.fake:
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
             workdir=None, fake=False, **opts):
    """Detach the current process in the background (daemonize).

    :keyword logfile: Optional log file.  The ability to write to this file
       will be verified before the process is detached.
    :keyword pidfile: Optional pid file.  The pid file will not be created,
      as this is the responsibility of the child.  But the process will
      exit if the pid lock exists and the pid written is still running.
    :keyword uid: Optional user id or user name to change
      effective privileges to.
    :keyword gid: Optional group id or group name to change effective
      privileges to.
    :keyword umask: Optional umask that will be effective in the child process.
    :keyword workdir: Optional new working directory.
    :keyword fake: Don't actually detach, intented for debugging purposes.
    :keyword \*\*opts: Ignored.

    **Example**:

    .. code-block:: python

        import atexit
        from celery.platforms import detached, create_pidlock

        with detached(logfile="/var/log/app.log", pidfile="/var/run/app.pid",
                      uid="nobody"):
            # Now in detached child process with effective user set to nobody,
            # and we know that our logfile can be written to, and that
            # the pidfile is not locked.
            pidlock = create_pidlock("/var/run/app.pid").acquire()
            atexit.register(pidlock.release)

            # Run the program
            program.run(logfile="/var/log/app.log")

    """

    if not resource:
        raise RuntimeError("This platform does not support detach.")
    workdir = os.getcwd() if workdir is None else workdir

    signals.reset("SIGCLD")  # Make sure SIGCLD is using the default handler.
    if not os.geteuid():
        # no point trying to setuid unless we're root.
        maybe_drop_privileges(uid=uid, gid=gid)

    # Since without stderr any errors will be silently suppressed,
    # we need to know that we have access to the logfile.
    logfile and open(logfile, "a").close()
    # Doesn't actually create the pidfile, but makes sure it's not stale.
    pidfile and create_pidlock(pidfile)

    return DaemonContext(umask=umask, workdir=workdir, fake=fake)


def parse_uid(uid):
    """Parse user id.

    uid can be an integer (uid) or a string (user name), if a user name
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


def _setgroups_hack(groups):
    """:fun:`setgroups` may have a platform-dependent limit,
    and it is not always possible to know in advance what this limit
    is, so we use this ugly hack stolen from glibc."""
    groups = groups[:]

    while 1:
        try:
            return os.setgroups(groups)
        except ValueError:   # error from Python's check.
            if len(groups) <= 1:
                raise
            groups[:] = groups[:-1]
        except OSError, exc:  # error from the OS.
            if exc.errno != errno.EINVAL or len(groups) <= 1:
                raise
            groups[:] = groups[:-1]


def setgroups(groups):
    max_groups = None
    try:
        max_groups = os.sysconf("SC_NGROUPS_MAX")
    except:
        pass
    try:
        return _setgroups_hack(groups[:max_groups])
    except OSError, exc:
        if exc.errno != errno.EPERM:
            raise
        if any(group not in groups for group in os.getgroups()):
            # we shouldn't be allowed to change to this group.
            raise


def initgroups(uid, gid):
    if grp and pwd:
        username = pwd.getpwuid(uid)[0]
        if hasattr(os, "initgroups"):  # Python 2.7+
            return os.initgroups(username, gid)
        groups = [gr.gr_gid for gr in grp.getgrall()
                                if username in gr.gr_mem]
        setgroups(groups)


def setegid(gid):
    """Set effective group id."""
    gid = parse_gid(gid)
    if gid != os.getegid():
        os.setegid(gid)


def seteuid(uid):
    """Set effective user id."""
    uid = parse_uid(uid)
    if uid != os.geteuid():
        os.seteuid(uid)


def setgid(gid):
    os.setgid(parse_gid(gid))


def setuid(uid):
    os.setuid(parse_uid(uid))


def maybe_drop_privileges(uid=None, gid=None):
    """Change process privileges to new user/group.

    If UID and GID is specified, the real user/group is changed.

    If only UID is specified, the real user is changed, and the group is
    changed to the users primary group.

    If only GID is specified, only the group is changed.

    """
    uid = uid and parse_uid(uid)
    gid = gid and parse_gid(gid)

    if uid:
        # If GID isn't defined, get the primary GID of the user.
        if not gid and pwd:
            gid = pwd.getpwuid(uid).pw_gid
        # Must set the GID before initgroups(), as setgid()
        # is known to zap the group list on some platforms.
        setgid(gid)
        initgroups(uid, gid)

        # at last:
        setuid(uid)
    else:
        gid and setgid(gid)


class Signals(object):
    """Convenience interface to :mod:`signals`.

    If the requested signal is not supported on the current platform,
    the operation will be ignored.

    **Examples**:

    .. code-block:: python

        >>> from celery.platforms import signals

        >>> signals["INT"] = my_handler

        >>> signals["INT"]
        my_handler

        >>> signals.supported("INT")
        True

        >>> signals.signum("INT")
        2

        >>> signals.ignore("USR1")
        >>> signals["USR1"] == signals.ignored
        True

        >>> signals.reset("USR1")
        >>> signals["USR1"] == signals.default
        True

        >>> signals.update(INT=exit_handler,
        ...                TERM=exit_handler,
        ...                HUP=hup_handler)

    """

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


if os.environ.get("NOSETPS"):

    def set_mp_process_title(*a, **k):
        pass
else:

    def set_mp_process_title(progname, info=None, hostname=None,  # noqa
            rate_limit=False):
        """Set the ps name using the multiprocessing process name.

        Only works if :mod:`setproctitle` is installed.

        """
        if not rate_limit or _setps_bucket.can_consume(1):
            if hostname:
                progname = "%s@%s" % (progname, hostname.split(".")[0])
            if current_process is not None:
                return set_process_title(
                    "%s:%s" % (progname, current_process().name), info=info)
            else:
                return set_process_title(progname, info=info)


def shellsplit(s, posix=True):
    # posix= option to shlex.split first available in Python 2.6+
    lexer = shlex.shlex(s, posix=not IS_WINDOWS)
    lexer.whitespace_split = True
    lexer.commenters = ''
    return list(lexer)
