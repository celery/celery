# -*- coding: utf-8 -*-
"""
    celery.platforms
    ~~~~~~~~~~~~~~~~

    Utilities dealing with platform specifics: signals, daemonization,
    users, groups, and so on.

"""
from __future__ import absolute_import
from __future__ import with_statement

import atexit
import errno
import os
import platform as _platform
import shlex
import signal as _signal
import sys

from billiard import current_process
from kombu.utils.encoding import safe_str
from contextlib import contextmanager

from .local import try_import

_setproctitle = try_import('setproctitle')
resource = try_import('resource')
pwd = try_import('pwd')
grp = try_import('grp')

# exitcodes
EX_OK = getattr(os, 'EX_OK', 0)
EX_FAILURE = 1
EX_UNAVAILABLE = getattr(os, 'EX_UNAVAILABLE', 69)
EX_USAGE = getattr(os, 'EX_USAGE', 64)

SYSTEM = _platform.system()
IS_OSX = SYSTEM == 'Darwin'
IS_WINDOWS = SYSTEM == 'Windows'

DAEMON_UMASK = 0
DAEMON_WORKDIR = '/'

PIDFILE_FLAGS = os.O_CREAT | os.O_EXCL | os.O_WRONLY
PIDFILE_MODE = ((os.R_OK | os.W_OK) << 6) | ((os.R_OK) << 3) | ((os.R_OK))

PIDLOCKED = """ERROR: Pidfile (%s) already exists.
Seems we're already running? (pid: %s)"""


def pyimplementation():
    """Returns string identifying the current Python implementation."""
    if hasattr(_platform, 'python_implementation'):
        return _platform.python_implementation()
    elif sys.platform.startswith('java'):
        return 'Jython ' + sys.platform
    elif hasattr(sys, 'pypy_version_info'):
        v = '.'.join(str(p) for p in sys.pypy_version_info[:3])
        if sys.pypy_version_info[3:]:
            v += '-' + ''.join(str(p) for p in sys.pypy_version_info[3:])
        return 'PyPy ' + v
    else:
        return 'CPython'


def _find_option_with_arg(argv, short_opts=None, long_opts=None):
    """Search argv for option specifying its short and longopt
    alternatives.

    Returns the value of the option if found.

    """
    for i, arg in enumerate(argv):
        if arg.startswith('-'):
            if long_opts and arg.startswith('--'):
                name, _, val = arg.partition('=')
                if name in long_opts:
                    return val
            if short_opts and arg in short_opts:
                return argv[i + 1]
    raise KeyError('|'.join(short_opts or [] + long_opts or []))


def maybe_patch_concurrency(argv, short_opts=None, long_opts=None):
    """With short and long opt alternatives that specify the command line
    option to set the pool, this makes sure that anything that needs
    to be patched is completed as early as possible.
    (e.g. eventlet/gevent monkey patches)."""
    try:
        pool = _find_option_with_arg(argv, short_opts, long_opts)
    except KeyError:
        pass
    else:
        # set up eventlet/gevent environments ASAP.
        from celery import concurrency
        concurrency.get_implementation(pool)


class LockFailed(Exception):
    """Raised if a pidlock can't be acquired."""


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


class Pidfile(object):
    """Pidfile

    This is the type returned by :func:`create_pidlock`.

    TIP: Use the :func:`create_pidlock` function instead,
    which is more convenient and also removes stale pidfiles (when
    the process holding the lock is no longer running).

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
        with ignore_errno('ENOENT'):
            with open(self.path, 'r') as fh:
                line = fh.readline()
                if line.strip() == line:  # must contain '\n'
                    raise ValueError(
                        'Partially written or invalid pidfile %r' % self.path)

                try:
                    return int(line.strip())
                except ValueError:
                    raise ValueError(
                        'pidfile %r contents invalid.' % self.path)

    def remove(self):
        """Removes the lock."""
        with ignore_errno(errno.ENOENT, errno.EACCES):
            os.unlink(self.path)

    def remove_if_stale(self):
        """Removes the lock if the process is not running.
        (does not respond to signals)."""
        try:
            pid = self.read_pid()
        except ValueError, exc:
            sys.stderr.write('Broken pidfile found. Removing it.\n')
            self.remove()
            return True
        if not pid:
            self.remove()
            return True

        try:
            os.kill(pid, 0)
        except os.error, exc:
            if exc.errno == errno.ESRCH:
                sys.stderr.write('Stale pidfile exists. Removing it.\n')
                self.remove()
                return True
        return False

    def write_pid(self):
        pid = os.getpid()
        content = '%d\n' % (pid, )

        pidfile_fd = os.open(self.path, PIDFILE_FLAGS, PIDFILE_MODE)
        pidfile = os.fdopen(pidfile_fd, 'w')
        try:
            pidfile.write(content)
            # flush and sync so that the re-read below works.
            pidfile.flush()
            try:
                os.fsync(pidfile_fd)
            except AttributeError:  # pragma: no cover
                pass
        finally:
            pidfile.close()

        rfh = open(self.path)
        try:
            if rfh.read() != content:
                raise LockFailed(
                    "Inconsistency: Pidfile content doesn't match at re-read")
        finally:
            rfh.close()
PIDFile = Pidfile  # compat alias


def create_pidlock(pidfile):
    """Create and verify pidfile.

    If the pidfile already exists the program exits with an error message,
    however if the process it refers to is not running anymore, the pidfile
    is deleted and the program continues.

    This function will automatically install an :mod:`atexit` handler
    to release the lock at exit, you can skip this by calling
    :func:`_create_pidlock` instead.

    :returns: :class:`Pidfile`.

    **Example**:

    .. code-block:: python

        pidlock = create_pidlock('/var/run/app.pid')

    """
    pidlock = _create_pidlock(pidfile)
    atexit.register(pidlock.release)
    return pidlock


def _create_pidlock(pidfile):
    pidlock = Pidfile(pidfile)
    if pidlock.is_locked() and not pidlock.remove_if_stale():
        raise SystemExit(PIDLOCKED % (pidfile, pidlock.read_pid()))
    pidlock.acquire()
    return pidlock


def fileno(f):
    """Get object fileno, or :const:`None` if not defined."""
    if isinstance(f, int):
        return f
    try:
        return f.fileno()
    except AttributeError:
        pass


def close_open_fds(keep=None):
    keep = [fileno(f) for f in keep if fileno(f)] if keep else []
    for fd in reversed(range(get_fdmax(default=2048))):
        if fd not in keep:
            with ignore_errno(errno.EBADF):
                os.close(fd)


class DaemonContext(object):
    _is_open = False

    def __init__(self, pidfile=None, workdir=None, umask=None,
                 fake=False, after_chdir=None, **kwargs):
        self.workdir = workdir or DAEMON_WORKDIR
        self.umask = DAEMON_UMASK if umask is None else umask
        self.fake = fake
        self.after_chdir = after_chdir
        self.stdfds = (sys.stdin, sys.stdout, sys.stderr)

    def redirect_to_null(self, fd):
        if fd:
            dest = os.open(os.devnull, os.O_RDWR)
            os.dup2(dest, fd)

    def open(self):
        if not self._is_open:
            if not self.fake:
                self._detach()

            os.chdir(self.workdir)
            os.umask(self.umask)

            if self.after_chdir:
                self.after_chdir()

            close_open_fds(self.stdfds)
            for fd in self.stdfds:
                self.redirect_to_null(fileno(fd))

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
    :keyword pidfile: Optional pidfile.  The pidfile will not be created,
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

        from celery.platforms import detached, create_pidlock

        with detached(logfile='/var/log/app.log', pidfile='/var/run/app.pid',
                      uid='nobody'):
            # Now in detached child process with effective user set to nobody,
            # and we know that our logfile can be written to, and that
            # the pidfile is not locked.
            pidlock = create_pidlock('/var/run/app.pid')

            # Run the program
            program.run(logfile='/var/log/app.log')

    """

    if not resource:
        raise RuntimeError('This platform does not support detach.')
    workdir = os.getcwd() if workdir is None else workdir

    signals.reset('SIGCLD')  # Make sure SIGCLD is using the default handler.
    if not os.geteuid():
        # no point trying to setuid unless we're root.
        maybe_drop_privileges(uid=uid, gid=gid)

    def after_chdir_do():
        # Since without stderr any errors will be silently suppressed,
        # we need to know that we have access to the logfile.
        logfile and open(logfile, 'a').close()
        # Doesn't actually create the pidfile, but makes sure it's not stale.
        if pidfile:
            _create_pidlock(pidfile).release()

    return DaemonContext(
        umask=umask, workdir=workdir, fake=fake, after_chdir=after_chdir_do,
    )


def parse_uid(uid):
    """Parse user id.

    uid can be an integer (uid) or a string (user name), if a user name
    the uid is taken from the password file.

    """
    try:
        return int(uid)
    except ValueError:
        try:
            return pwd.getpwnam(uid).pw_uid
        except (AttributeError, KeyError):
            raise KeyError('User does not exist: %r' % (uid, ))


def parse_gid(gid):
    """Parse group id.

    gid can be an integer (gid) or a string (group name), if a group name
    the gid is taken from the password file.

    """
    try:
        return int(gid)
    except ValueError:
        try:
            return grp.getgrnam(gid).gr_gid
        except (AttributeError, KeyError):
            raise KeyError('Group does not exist: %r' % (gid, ))


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
    """Set active groups from a list of group ids."""
    max_groups = None
    try:
        max_groups = os.sysconf('SC_NGROUPS_MAX')
    except Exception:
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
    """Compat version of :func:`os.initgroups` which was first
    added to Python 2.7."""
    if not pwd:  # pragma: no cover
        return
    username = pwd.getpwuid(uid)[0]
    if hasattr(os, 'initgroups'):  # Python 2.7+
        return os.initgroups(username, gid)
    groups = [gr.gr_gid for gr in grp.getgrall()
              if username in gr.gr_mem]
    setgroups(groups)


def setgid(gid):
    """Version of :func:`os.setgid` supporting group names."""
    os.setgid(parse_gid(gid))


def setuid(uid):
    """Version of :func:`os.setuid` supporting usernames."""
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

        >>> signals['INT'] = my_handler

        >>> signals['INT']
        my_handler

        >>> signals.supported('INT')
        True

        >>> signals.signum('INT')
        2

        >>> signals.ignore('USR1')
        >>> signals['USR1'] == signals.ignored
        True

        >>> signals.reset('USR1')
        >>> signals['USR1'] == signals.default
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
            raise TypeError('signal name must be uppercase string.')
        if not signal_name.startswith('SIG'):
            signal_name = 'SIG' + signal_name
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
    arg_start = 2 if 'manage' in argv[0] else 1
    if len(argv) > arg_start:
        return ' '.join(argv[arg_start:])
    return ''


def set_process_title(progname, info=None):
    """Set the ps name for the currently running process.

    Only works if :mod:`setproctitle` is installed.

    """
    proctitle = '[%s]' % progname
    proctitle = '%s %s' % (proctitle, info) if info else proctitle
    if _setproctitle:
        _setproctitle.setproctitle(safe_str(proctitle))
    return proctitle


if os.environ.get('NOSETPS'):  # pragma: no cover

    def set_mp_process_title(*a, **k):
        pass
else:

    def set_mp_process_title(progname, info=None, hostname=None):  # noqa
        """Set the ps name using the multiprocessing process name.

        Only works if :mod:`setproctitle` is installed.

        """
        if hostname:
            progname = '%s@%s' % (progname, hostname.split('.')[0])
        return set_process_title(
            '%s:%s' % (progname, current_process().name), info=info)


def shellsplit(s):
    """Compat. version of :func:`shlex.split` that supports
    the ``posix`` option which was first added in Python 2.6.

    Posix behavior will be disabled if running under Windows.

    """
    lexer = shlex.shlex(s, posix=not IS_WINDOWS)
    lexer.whitespace_split = True
    lexer.commenters = ''
    return list(lexer)


def get_errno(n):
    """Get errno for string, e.g. ``ENOENT``."""
    if isinstance(n, basestring):
        return getattr(errno, n)
    return n


@contextmanager
def ignore_errno(*errnos, **kwargs):
    """Context manager to ignore specific POSIX error codes.

    Takes a list of error codes to ignore, which can be either
    the name of the code, or the code integer itself::

        >>> with ignore_errno('ENOENT'):
        ...     with open('foo', 'r'):
        ...         return r.read()

        >>> with ignore_errno(errno.ENOENT, errno.EPERM):
        ...    pass

    :keyword types: A tuple of exceptions to ignore (when the errno matches),
                    defaults to :exc:`Exception`.
    """
    types = kwargs.get('types') or (Exception, )
    errnos = [get_errno(errno) for errno in errnos]
    try:
        yield
    except types, exc:
        if not hasattr(exc, 'errno'):
            raise
        if exc.errno not in errnos:
            raise
