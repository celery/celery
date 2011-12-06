# -*- coding: utf-8 -*-
"""
    celery.worker.autoreload
    ~~~~~~~~~~~~~~~~~~~~~~~~

    This module implements the automatic module reloading
"""
from __future__ import with_statement

import os
import sys
import time
import select
import hashlib

from collections import defaultdict

from celery.task.control import broadcast


def file_hash(filename, algorithm='md5'):
    hobj = hashlib.new(algorithm)
    with open(filename, 'rb') as f:
        for chunk in iter(lambda: f.read(2 ** 20), ''):
            hobj.update(chunk)
    return hobj.digest()


class StatMonitor(object):
    """File change monitor based on `stat` system call"""
    def __init__(self, files, on_change=None, interval=0.5):
        self._files = files
        self._interval = interval
        self._on_change = on_change
        self._modify_times = defaultdict(int)

    def start(self):
        while True:
            modified = {}
            for m in self._files:
                mt = self._mtime(m)
                if mt is None:
                    break
                if self._modify_times[m] != mt:
                    modified[m] = mt
            else:
                if modified:
                    self.on_change(modified.keys())
                    self._modify_times.update(modified)

            time.sleep(self._interval)

    def on_change(self, modified):
        if self._on_change:
            return self._on_change(modified)

    @classmethod
    def _mtime(cls, path):
        try:
            return os.stat(path).st_mtime
        except:
            return


class KQueueMonitor(object):
    """File change monitor based on BSD kernel event notifications"""
    def __init__(self, files, on_change=None):
        assert hasattr(select, 'kqueue')
        self._files = dict([(f, None) for f in files])
        self._on_change = on_change

    def start(self):
        try:
            self._kq = select.kqueue()
            kevents = []
            for f in self._files:
                self._files[f] = fd = os.open(f, os.O_RDONLY)

                ev = select.kevent(fd,
                        filter=select.KQ_FILTER_VNODE,
                        flags=select.KQ_EV_ADD |
                              select.KQ_EV_ENABLE |
                              select.KQ_EV_CLEAR,
                        fflags=select.KQ_NOTE_WRITE |
                               select.KQ_NOTE_EXTEND)
                kevents.append(ev)

            events = self._kq.control(kevents, 0)
            while True:
                events = self._kq.control(kevents, 1)
                fds = [e.ident for e in events]
                modified = [k for k, v in self._files.iteritems()
                                            if v in fds]
                self.on_change(modified)
        finally:
            self.close()

    def close(self):
        self._kq.close()
        for f in self._files:
            if self._files[f] is not None:
                os.close(self._files[f])
                self._files[f] = None

    def on_change(self, modified):
        if self._on_change:
            return self._on_change(modified)


try:
    import pyinotify
except ImportError:
    pyinotify = None    # noqa


class InotifyMonitor(pyinotify and pyinotify.ProcessEvent or object):
    """File change monitor based on  Linux kernel `inotify` subsystem"""
    def __init__(self, modules, on_change=None):
        assert pyinotify
        self._modules = modules
        self._on_change = on_change

    def start(self):
        try:
            self._wm = pyinotify.WatchManager()
            self._notifier = pyinotify.Notifier(self._wm)
            for m in self._modules:
                self._wm.add_watch(m, pyinotify.IN_MODIFY)
            self._notifier.loop()
        finally:
            self.close()

    def close(self):
        self._notifier.stop()
        self._wm.close()

    def process_IN_MODIFY(self, event):
        self.on_change(event.pathname)

    def on_change(self, modified):
        if self._on_change:
            self._on_change(modified)


if hasattr(select, 'kqueue'):
    _monitor_cls = KQueueMonitor
elif sys.platform.startswith('linux') and pyinotify:
    _monitor_cls = InotifyMonitor
else:
    _monitor_cls = StatMonitor


class AutoReloader(object):
    """Tracks changes in modules and fires reload commands"""
    def __init__(self, modules, monitor_cls=_monitor_cls, *args, **kwargs):
        self._monitor = monitor_cls(modules, self.on_change, *args, **kwargs)
        self._hashes = dict([(f, file_hash(f)) for f in modules])

    def start(self):
        self._monitor.start()

    def on_change(self, files):
        modified = []
        for f in files:
            fhash = file_hash(f)
            if fhash != self._hashes[f]:
                modified.append(f)
                self._hashes[f] = fhash
        if modified:
            self._reload(map(self._module_name, modified))

    def _reload(self, modules):
        broadcast("pool_restart",
                arguments={"imports": modules, "reload_modules": True})

    @classmethod
    def _module_name(cls, path):
        return os.path.splitext(os.path.basename(path))[0]
