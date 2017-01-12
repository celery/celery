
# -*- coding: utf-8 -*-

"""A future executor supports getting tornado coroutine result synchronously.

With this, we can use coroutine in celery task like the following:


    from celery import app
    from tornado import gen

    @app.task
    @gen.coroutine
    def task_a():

        res = yield request_a()

    @gen.coroutine
    def request_a():

        return 1

"""

from __future__ import absolute_import, unicode_literals

from kombu.utils.compat import detect_environment
try:
    from tornado import concurrent, ioloop
except ImportError:
    concurrent = ioloop = None


__all__ = ['future_executor']


def get_future_executor():
    """Future executor factory function."""
    environment = detect_environment()
    if all((concurrent, ioloop)):
        if environment == 'eventlet':
            return eventletFutureExecutor
        elif environment == 'gevent':
            return geventFutureExecutor
        else:
            return defaultFutureExecutor
    else:
        return defaultFutureExecutor


class defaultFutureExecutor(object):
    """Default future executor, make no sense about future."""

    _local = None
    local = None
    spawn = None
    getcurrent = None

    @classmethod
    def apply_future(cls, func, *args, **kwargs):
        """Handle future result in task execution."""
        return func(*args, **kwargs)

    @classmethod
    def switch_to_ioloop(cls):

        pass


class greenFutureExecutor(defaultFutureExecutor):
    """Base class for future executor in greenlet."""

    @classmethod
    def get_hub(cls):
        """Return TornadoHub instance."""
        if cls._local is None:
            cls._local = cls.local()
            cls._local.hub = None
        if cls._local.hub is None:
            hub = cls._local.hub = TornadoHub()
            hub.start(cls.spawn)

        return cls._local.hub

    @classmethod
    def apply_future(cls, func, *args, **kwargs):
        """Handle future result in greenlet execution."""
        g = cls.getcurrent()
        g.need_switch = False

        def done(f):

            g.switch()
            cls.spawn(lambda: g.switch())  # finish task greenlet execution

        future = func(*args, **kwargs)
        if all((cls.spawn, cls.getcurrent)) and concurrent.is_future(future):
            hub = cls.get_hub()
            hub.io_loop().add_future(future, done)
            g.parent.switch()
            g.need_switch = True
            return future.result()
        else:
            return future

    @classmethod
    def switch_to_ioloop(cls):
        """Switch execution to ioloop greenlet."""
        g = cls.getcurrent()
        if g.need_switch:
            hub = cls.get_hub()
            hub.enter()


class eventletFutureExecutor(greenFutureExecutor):

    try:
        from eventlet.patcher import original
    except ImportError:
        pass
    else:
        threading = original('threading')
        local = threading.local

    try:
        from eventlet import getcurrent
    except ImportError:
        pass
    else:
        getcurrent = getcurrent

    try:
        from eventlet import spawn
    except ImportError:
        pass
    else:
        spawn_raw = spawn


class geventFutureExecutor(greenFutureExecutor):

    try:
        from gevent.monkey import get_original
    except ImportError:
        pass
    else:
        local = get_original('threading', 'local')

    try:
        from gevent import getcurrent
    except ImportError:
        pass
    else:
        getcurrent = getcurrent

    try:
        from gevent import spawn_raw
    except ImportError:
        pass
    else:
        spawn = spawn_raw


class TornadoHub(object):
    """Event hub implementation with tornado io loop."""

    # prevent http request timeout when using default poll timeout
    _wake_timeout = 0.2

    def __init__(self, *args, **kwargs):

        super(TornadoHub, self).__init__(*args, **kwargs)
        self._io_loop = ioloop.IOLoop.current()

    def start(self, spawn):

        self.g = spawn(self.run)

    def run(self):

        def on_wake():
            self._io_loop.call_later(self._wake_timeout, on_wake)

        self._io_loop.call_later(self._wake_timeout, on_wake)
        self._io_loop.start()

    def enter(self):
        """Switch to this hub execution."""
        self.g.switch()

    def stop(self, timeout=None):

        self._io_loop.stop()

    def io_loop(self):
        """Return io loop on hub instance."""
        return self._io_loop


future_executor = get_future_executor()
