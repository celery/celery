import signal
try:
    from setproctitle import setproctitle as _setproctitle
except ImportError:
    _setproctitle = None


from celery.utils import noop


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
