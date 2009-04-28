import os
import sys
import errno
import resource

__all__ = ["PIDFile", "remove_pidfile", "daemonize"]

# File mode creation mask of the daemon.
# No point in changing this, as we don't really create any files.
DAEMON_UMASK = 0

# Default working directory for the daemon.
DAEMON_WORKDIR = "/"

# Default maximum for the number of available file descriptors.
DAEMON_MAXFD = 1024

# The standard I/O file descriptors are redirected to /dev/null by default.
if (hasattr(os, "devnull")):
   REDIRECT_TO = os.devnull
else:
   REDIRECT_TO = "/dev/null"

class PIDFile(object):

    def __init__(self, pidfile):
        self.pidfile = pidfile

    def get_pid(self):
        pidfile_fh = file(self.pidfile, "r")
        pid = int(pidfile_fh.read().strip())
        pidfile_fh.close()
        return pid

    def check(self):
        if os.path.exists(self.pidfile) and os.path.isfile(self.pidfile):
            pid = self.get_pid()
            try:
                os.kill(pid, 0)
            except os.error, e:
                if e.errno == errno.ESRCH:
                   sys.stderr.write("Stale pidfile exists. removing it.\n")
                   self.remove()
            else:
                raise SystemExit("celeryd is already running.")

    def remove(self):
        os.unlink(self.pidfile)

    def write(self, pid=None):
        if not pid:
            pid = os.getpid()
        pidfile_fh = file(self.pidfile, "w")
        pidfile_fh.write("%d\n" % pid)
        pidfile_fh.close()


def remove_pidfile(pidfile):
    os.unlink(pidfile)


def daemonize(pidfile):
    """Detach a process from the controlling terminal and run it in the
    background as a daemon."""

    try:
        pid = os.fork()
    except OSError, e:
        raise Exception, "%s [%d]" % (e.strerror, e.errno)

    if pid == 0: # child
        os.setsid()

        try:
            pid = os.fork() # second child
        except OSError, e:
            raise Exception, "%s [%d]" % (e.strerror, e.errno)

        if pid == 0: # second child
            #os.chdir(DAEMON_WORKDIR)
            os.umask(DAEMON_UMASK)
        else: # parent (first child)
            pidfile.write(pid)
            os._exit(0)
    else: # root process
        os._exit(0)

    maxfd = resource.getrlimit(resource.RLIMIT_NOFILE)[1]
    if (maxfd == resource.RLIM_INFINITY):
        maxfd = DAEMON_MAXFD
  
    # Iterate through and close all file descriptors.
    for fd in range(0, maxfd):
        try:
            os.close(fd)
        except OSError:
            pass

    os.open(REDIRECT_TO, os.O_RDWR)
    # Duplicate standard input to standard output and standard error.
    os.dup2(0, 1) 
    os.dup2(0, 2)

    return 0
