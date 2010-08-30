import signal
try:
    from setproctitle import setproctitle as _setproctitle
except ImportError:
    _setproctitle = None


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


def set_process_title(progname, info=None):
    """Set the ps name for the currently running process.

    Only works if :mod:`setproctitle` is installed.

    """
    proctitle = "[%s]" % progname
    proctitle = info and "%s %s" % (proctitle, info) or proctitle
    if _setproctitle:
        _setproctitle(proctitle)
    return proctitle


def set_mp_process_title(progname, info=None):
    """Set the ps name using the multiprocessing process name.

    Only works if :mod:`setproctitle` is installed.

    """
    from multiprocessing.process import current_process
    return set_process_title("%s.%s" % (progname, current_process().name),
                             info=info)
