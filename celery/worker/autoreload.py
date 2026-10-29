"""Worker autoreloader.

Watches the source files of every module imported by the worker process
(not just the tasks modules) and asks the worker to restart when one of
them is modified, so changes to modules *imported by* the tasks are picked
up as well.

On the prefork pool tasks run in child processes, so a module that is only
imported inside a task never shows up in the main process' ``sys.modules``.
To catch those too, every ``.py`` file under the directories of the watched
modules is watched as well.

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

# Directories that are never descended into when looking for source files.
IGNORED_DIRS = frozenset({'__pycache__', 'node_modules'})


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
        # walking these would scan far more than the project.
        self._no_walk = {os.path.abspath(os.sep),
                         os.path.realpath(os.path.expanduser('~'))}
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

    def _skip_dir(self, parent, name):
        path = os.path.join(parent, name)
        return (
            name.startswith('.') or name in IGNORED_DIRS
            or (path + os.sep).startswith(self._ignored)
            # virtualenv living inside the project.
            or os.path.isfile(os.path.join(path, 'pyvenv.cfg'))
        )

    def _roots(self, files):
        """Return the top-most directories containing ``files``."""
        roots = []
        for directory in sorted({os.path.dirname(f) for f in files}):
            if directory in self._no_walk:
                continue
            if not any(directory == r or directory.startswith(r + os.sep)
                       for r in roots):
                roots.append(directory)
        return roots

    def project_files(self):
        """Return the files to watch: imported modules and their siblings.

        Besides the imported modules this includes every ``.py`` file under
        the directories they live in, see the module docstring.
        """
        files = self.module_files()
        for root in self._roots(files):
            for dirpath, dirnames, filenames in os.walk(root):
                dirnames[:] = [d for d in dirnames
                               if not self._skip_dir(dirpath, d)]
                files.update(os.path.join(dirpath, name)
                             for name in filenames if name.endswith('.py'))
        return files

    def find_changes(self):
        """Return the first changed file since the previous call, if any.

        Files seen for the first time are only recorded.
        """
        for filename in self.project_files():
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
            if not changed:
                continue
            error = self._check_source(changed)
            if error:
                # restarting would only crash on import and leave nothing
                # watching, so wait for the next save instead.
                logger.warning(
                    'Detected change in %s but not restarting: %s',
                    changed, error)
                continue
            logger.info('Detected change in %s, restarting...', changed)
            self.on_change(changed)
            return

    @staticmethod
    def _check_source(filename):
        """Return why ``filename`` can't be imported right now, if so."""
        try:
            with open(filename, 'rb') as f:
                compile(f.read(), filename, 'exec', dont_inherit=True)
        except (SyntaxError, ValueError, OSError) as exc:
            return exc

    def stop(self):
        self._stopped.set()
