# -*- coding: utf-8 -*-
"""
    celery.worker.autoreload
    ~~~~~~~~~~~~~~~~~~~~~~~~

    This module implements automatic module reloading
"""
from __future__ import absolute_import
from __future__ import with_statement

import hashlib
import os
import select
import sys
import time

from collections import defaultdict

from kombu.utils import eventio

from celery.platforms import ignore_errno
from celery.utils.imports import module_file
from celery.utils.log import get_logger
from celery.utils.threads import bgThread, Event

from .bootsteps import StartStopComponent

try:                        # pragma: no cover
    import pyinotify
    _ProcessEvent = pyinotify.ProcessEvent
except ImportError:         # pragma: no cover
    pyinotify = None        # noqa
    _ProcessEvent = object  # noqa

logger = get_logger(__name__)


class WorkerComponent(StartStopComponent):
    name = 'worker.autoreloader'
    requires = ('pool', )

    def __init__(self, w, autoreload=None, **kwargs):
        self.enabled = w.autoreload = autoreload
        w.autoreloader = None

    def create_ev(self, w):
        ar = w.autoreloader = self.instantiate(w.autoreloader_cls, w)
        w.hub.on_init.append(ar.on_poll_init)
        w.hub.on_close.append(ar.on_poll_close)

    def create_threaded(self, w):
        w.autoreloader = self.instantiate(w.autoreloader_cls, w)
        return w.autoreloader

    def create(self, w):
        if hasattr(select, 'kqueue') and w.use_eventloop:
            return self.create_ev(w)
        return self.create_threaded(w)


def file_hash(filename, algorithm='md5'):
    hobj = hashlib.new(algorithm)
    with open(filename, 'rb') as f:
        for chunk in iter(lambda: f.read(1048576), ''):
            hobj.update(chunk)
    return hobj.digest()


class BaseMonitor(object):

    def __init__(self, files,
                 on_change=None, shutdown_event=None, interval=0.5):
        self.files = files
        self.interval = interval
        self._on_change = on_change
        self.modify_times = defaultdict(int)
        self.shutdown_event = shutdown_event or Event()

    def start(self):
        raise NotImplementedError('Subclass responsibility')

    def stop(self):
        pass

    def on_change(self, modified):
        if self._on_change:
            return self._on_change(modified)


class StatMonitor(BaseMonitor):
    """File change monitor based on the ``stat`` system call."""

    def _mtimes(self):
        return ((f, self._mtime(f)) for f in self.files)

    def _maybe_modified(self, f, mt):
        return mt is not None and self.modify_times[f] != mt

    def start(self):
        while not self.shutdown_event.is_set():
            modified = dict((f, mt) for f, mt in self._mtimes()
                            if self._maybe_modified(f, mt))
            if modified:
                self.on_change(modified)
                self.modify_times.update(modified)
            time.sleep(self.interval)

    @staticmethod
    def _mtime(path):
        try:
            return os.stat(path).st_mtime
        except Exception:
            pass


class KQueueMonitor(BaseMonitor):
    """File change monitor based on BSD kernel event notifications"""

    def __init__(self, *args, **kwargs):
        super(KQueueMonitor, self).__init__(*args, **kwargs)
        self.filemap = dict((f, None) for f in self.files)
        self.fdmap = {}

    def on_poll_init(self, hub):
        self.add_events(hub.poller)
        hub.poller.on_file_change = self.handle_event

    def on_poll_close(self, hub):
        self.close(hub.poller)

    def add_events(self, poller):
        for f in self.filemap:
            self.filemap[f] = fd = os.open(f, os.O_RDONLY)
            self.fdmap[fd] = f
            poller.watch_file(fd)

    def handle_event(self, events):
        self.on_change([self.fdmap[e.ident] for e in events])

    def start(self):
        self.poller = eventio.poll()
        self.add_events(self.poller)
        self.poller.on_file_change = self.handle_event
        while not self.shutdown_event.is_set():
            self.poller.poll(1)

    def close(self, poller):
        for f, fd in self.filemap.iteritems():
            if fd is not None:
                poller.unregister(fd)
                with ignore_errno('EBADF'):  # pragma: no cover
                    os.close(fd)
        self.filemap.clear()
        self.fdmap.clear()

    def stop(self):
        self.close(self.poller)
        self.poller.close()


class InotifyMonitor(_ProcessEvent):
    """File change monitor based on Linux kernel `inotify` subsystem"""

    def __init__(self, modules, on_change=None, **kwargs):
        assert pyinotify
        self._modules = modules
        self._on_change = on_change
        self._wm = None
        self._notifier = None

    def start(self):
        try:
            self._wm = pyinotify.WatchManager()
            self._notifier = pyinotify.Notifier(self._wm, self)
            add_watch = self._wm.add_watch
            flags = pyinotify.IN_MODIFY | pyinotify.IN_ATTRIB
            for m in self._modules:
                add_watch(m, flags)
            self._notifier.loop()
        finally:
            if self._wm:
                self._wm.close()
                # Notifier.close is called at the end of Notifier.loop
                self._wm = self._notifier = None

    def stop(self):
        pass

    def process_(self, event):
        self.on_change([event.path])

    process_IN_ATTRIB = process_IN_MODIFY = process_

    def on_change(self, modified):
        if self._on_change:
            return self._on_change(modified)


def default_implementation():
    # kqueue monitor not working properly at this time.
    if hasattr(select, 'kqueue'):
        return 'kqueue'
    if sys.platform.startswith('linux') and pyinotify:
        return 'inotify'
    else:
        return 'stat'

implementations = {'kqueue': KQueueMonitor,
                   'inotify': InotifyMonitor,
                   'stat': StatMonitor}
Monitor = implementations[
    os.environ.get('CELERYD_FSNOTIFY') or default_implementation()]


class Autoreloader(bgThread):
    """Tracks changes in modules and fires reload commands"""
    Monitor = Monitor

    def __init__(self, controller, modules=None, monitor_cls=None, **options):
        super(Autoreloader, self).__init__()
        self.controller = controller
        app = self.controller.app
        self.modules = app.loader.task_modules if modules is None else modules
        self.options = options
        self._monitor = None
        self._hashes = None
        self.file_to_module = {}

    def on_init(self):
        files = self.file_to_module
        files.update(dict(
            (module_file(sys.modules[m]), m) for m in self.modules))

        self._monitor = self.Monitor(
            files, self.on_change,
            shutdown_event=self._is_shutdown, **self.options)
        self._hashes = dict([(f, file_hash(f)) for f in files])

    def on_poll_init(self, hub):
        if self._monitor is None:
            self.on_init()
        self._monitor.on_poll_init(hub)

    def on_poll_close(self, hub):
        if self._monitor is not None:
            self._monitor.on_poll_close(hub)

    def body(self):
        self.on_init()
        with ignore_errno('EINTR', 'EAGAIN'):
            self._monitor.start()

    def _maybe_modified(self, f):
        if os.path.exists(f):
            digest = file_hash(f)
            if digest != self._hashes[f]:
                self._hashes[f] = digest
                return True
        return False

    def on_change(self, files):
        modified = [f for f in files if self._maybe_modified(f)]
        if modified:
            names = [self.file_to_module[module] for module in modified]
            logger.info('Detected modified modules: %r', names)
            self._reload(names)

    def _reload(self, modules):
        self.controller.reload(modules, reload=True)

    def stop(self):
        if self._monitor:
            self._monitor.stop()
