"""Worker autoreloader.

Watches the source files of every module imported by the worker process
(not just the tasks modules) and asks the worker to restart when one of
them is modified, so changes to modules *imported by* the tasks are picked
up as well.

Modules that live in the standard library or in ``site-packages`` are
ignored: they are numerous, and they don't change while developing.
"""
import os
import site
import sys
import sysconfig
import threading

from celery.utils.log import get_logger

__all__ = ('Autoreloader',)

logger = get_logger(__name__)

DEFAULT_INTERVAL = 1.0


def _ignored_prefixes():
    paths = {sysconfig.get_path(name) for name in (
        'stdlib', 'platstdlib', 'purelib', 'platlib')}
    paths.update(site.getsitepackages())
    paths.add(site.getusersitepackages())
    return tuple(os.path.realpath(p) + os.sep for p in paths if p)


class Autoreloader(threading.Thread):
    """Poll modules' mtimes and call ``on_change(path)`` when one changes.

    Arguments:
        on_change (Callable): Called once, with the path of the first
            modified file found.
        interval (float): Seconds between two scans.
    """

    def __init__(self, on_change, interval=DEFAULT_INTERVAL):
        super().__init__(name='celery-autoreload', daemon=True)
        self.on_change = on_change
        self.interval = interval
        self._ignored = _ignored_prefixes()
        self._mtimes = {}
        self._resolved = {}
        self._stopped = threading.Event()

    def _resolve(self, filename):
        """Return the real path of a module source, or None if not watched.

        ``realpath`` stats every path component, which dominates the cost
        of a scan, so the result is cached per ``__file__``.
        """
        try:
            return self._resolved[filename]
        except KeyError:
            pass
        source = filename[:-1] if filename.endswith(('.pyc', '.pyo')) \
            else filename
        resolved = None
        if source.endswith('.py'):
            real = os.path.realpath(source)
            if not real.startswith(self._ignored):
                resolved = real
        self._resolved[filename] = resolved
        return resolved

    def module_files(self):
        """Return the watched source files of all imported modules."""
        files = set()
        for module in list(sys.modules.values()):
            filename = getattr(module, '__file__', None)
            if filename:
                resolved = self._resolve(filename)
                if resolved:
                    files.add(resolved)
        return files

    def find_changes(self):
        """Return the first changed file since the previous call, if any.

        Files seen for the first time are only recorded.
        """
        for filename in self.module_files():
            try:
                mtime = os.stat(filename).st_mtime
            except OSError:
                # deleted/renamed (e.g. by an editor's atomic save), the new
                # file will be picked up by the next scan.
                continue
            previous = self._mtimes.setdefault(filename, mtime)
            if mtime != previous:
                self._mtimes[filename] = mtime
                return filename

    def run(self):
        if not self._mtimes:
            self.find_changes()  # baseline
        while not self._stopped.wait(self.interval):
            try:
                changed = self.find_changes()
            except Exception as exc:  # pylint: disable=broad-except
                logger.exception('Autoreload scan failed: %r', exc)
                continue
            if changed:
                logger.info('Detected change in %s, restarting...', changed)
                self.on_change(changed)
                return

    def stop(self):
        self._stopped.set()
