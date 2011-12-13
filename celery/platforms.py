import os
import sys
import errno
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

try:
    import pwd
except ImportError:
    pwd = None

try:
    import grp
except ImportError:
    grp = None

DAEMON_UMASK = 0
DAEMON_WORKDIR = "/"
DAEMON_REDIRECT_TO = getattr(os, "devnull", "/dev/nulll")


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

    def is_locked(self):
        return os.path.exists(self.path)

    def release(self):
        self.remove()

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

    def __init__(self, pidfile=None,
            working_directory=DAEMON_WORKDIR, umask=DAEMON_UMASK, **kwargs):
        self.working_directory = working_directory
        self.umask = umask

    def detach(self):
        if os.fork() == 0:      # first child
            os.setsid()         # create new session
            if os.fork() > 0:   # second child
                os._exit(0)
        else:
            os._exit(0)

    def open(self):
        if self._is_open:
            return

        self.detach()

        os.chdir(self.working_directory)
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

    def close(self):
        if self._is_open:
            self._is_open = False


def create_daemon_context(logfile=None, pidfile=None, uid=None, gid=None,
        **options):
    if not CAN_DETACH:
        raise RuntimeError(
                "This platform does not support detach.")

    # Make sure SIGCLD is using the default handler.
    reset_signal("SIGCLD")

    set_effective_user(uid=uid, gid=gid)

    # Since without stderr any errors will be silently suppressed,
    # we need to know that we have access to the logfile.
    if logfile:
        open(logfile, "a").close()
    if pidfile:
        # Doesn't actually create the pidfile, but makes sure it's
        # not stale.
        create_pidlock(pidfile)

    defaults = {"umask": lambda: 0,
                "working_directory": lambda: os.getcwd()}

    for opt_name, opt_default_gen in defaults.items():
        if opt_name not in options or options[opt_name] is None:
            options[opt_name] = opt_default_gen()

    context = DaemonContext(**options)

    return context, context.close


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
        setgid(gid)
        setuid(uid)
    else:
        gid and setgid(gid)


def get_signal(signal_name):
    """Get signal number from signal name."""
    if not isinstance(signal_name, basestring) or not signal_name.isupper():
        raise TypeError("signal name must be uppercase string.")
    if not signal_name.startswith("SIG"):
        signal_name = "SIG" + signal_name
    return getattr(signal, signal_name)


def reset_signal(signal_name):
    """Reset signal to the default signal handler.

    Does nothing if the platform doesn't support signals,
    or the specified signal in particular.

    """
    try:
        signum = getattr(signal, signal_name)
        signal.signal(signum, signal.SIG_DFL)
    except (AttributeError, ValueError):
        pass


def ignore_signal(signal_name):
    """Ignore signal using :const:`SIG_IGN`.

    Does nothing if the platform doesn't support signals,
    or the specified signal in particular.

    """
    try:
        signum = getattr(signal, signal_name)
        signal.signal(signum, signal.SIG_IGN)
    except (AttributeError, ValueError):
        pass


def install_signal_handler(signal_name, handler):
    """Install a handler.

    Does nothing if the current platform doesn't support signals,
    or the specified signal in particular.

    """
    try:
        signum = getattr(signal, signal_name)
        signal.signal(signum, handler)
    except (AttributeError, ValueError):
        pass


def strargv(argv):
    arg_start = "manage" in argv[0] and 2 or 1
    if len(argv) > arg_start:
        return " ".join(argv[arg_start:])
    return ""


def set_process_title(progname, info=None):
    """Set the ps name for the currently running process.

    Only works if :mod:`setproctitle` is installed.

    """
    proctitle = "[%s]" % progname
    proctitle = info and "%s %s" % (proctitle, info) or proctitle
    if _setproctitle:
        _setproctitle(proctitle)
    return proctitle


def set_mp_process_title(progname, info=None, hostname=None):
    """Set the ps name using the multiprocessing process name.

    Only works if :mod:`setproctitle` is installed.

    """
    try:
        from multiprocessing.process import current_process
    except ImportError:
        return
    if hostname:
        progname = "%s@%s" % (progname, hostname.split(".")[0])
    return set_process_title("%s:%s" % (progname, current_process().name),
                             info=info)
