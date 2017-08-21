
# -*- coding: utf-8 -*-

"""Future executor supports communication between tornado ioloop and greenlet.

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
import functools
import sys

from greenlet import greenlet
from kombu.utils.compat import detect_environment
try:
    from tornado import concurrent, gen, ioloop
except ImportError:
    raise RuntimeError(
        "you can't use this future module without tornado framework.")

from celery.utils.log import logger


__all__ = [
    'defaultFutureExecutor', 'geventFutureExecutor', 'eventletFutureExecutor',
    'get_future_executor']


_executor = None  # global future executor


def get_future_executor():
    """Future executor factory method."""
    global _executor
    if _executor:
        return _executor

    executor = defaultFutureExecutor

    environment = detect_environment()
    if environment == 'eventlet':
        from eventlet.patcher import is_monkey_patched
        if is_monkey_patched('os') or is_monkey_patched('select'):
            raise RuntimeError(
                "it currently doesn't support coroutine execution under "
                "eventlet's heavy monkey patch.")
        executor = eventletFutureExecutor
    elif environment == 'gevent':
        executor = geventFutureExecutor

    _executor = executor
    return _executor


class defaultFutureExecutor(object):
    """Default future executor."""

    import threading
    thread_cls = threading.Thread
    if sys.version_info.major < 3:
        event_cls = classmethod(threading.Event)
    else:
        event_cls = threading.Event

    _hub = None

    @classmethod
    def get_hub(cls):
        """Return TornadoHub instance."""
        if cls._hub is None:
            cls._hub = TornadoHub(cls.thread_cls, cls.event_cls)
            cls._hub.start()

        return cls._hub

    @classmethod
    def apply_future(cls, func, *args, **kwargs):
        """Execute coroutine in a standalone tornado ioloop.

        :param func: coroutine function or regular function.
        :param kwargs: if `callback` in kwargs dict,
            the value will be treated as a function and called when coroutine
            finishs.
        """
        hub = cls.get_hub()
        callback = kwargs.pop('callback', None)
        ret = func(*args, **kwargs)
        if concurrent.is_future(ret):
            if ret.done():
                return ret.result()

            return hub.execute_future(ret, callback)
        else:
            return ret


class greenFutureExecutor(defaultFutureExecutor):
    """Base class for future executor with greenlet."""

    @classmethod
    def apply_future(cls, func, *args, **kwargs):
        """Execute coroutine and send future result to the called greenlet."""
        g = cls.getcurrent()

        def future_done(g_hub, f):
            def resume_future():
                try:
                    ret = f.result()
                except Exception:
                    g.throw(f.exception())
                else:
                    g.switch(ret)

            # future is done and ready switching to the previous greenlet
            cls.spawn(resume_future, g_hub)

        hub = cls.get_hub()
        g_hub = cls.get_greentlet_hub()
        kwargs['callback'] = functools.partial(future_done, g_hub)
        hub.execute(func, *args, **kwargs)

        # yield to parent greenlet and run any other greenlets
        ret = g.parent.switch()
        return ret


class eventletFutureExecutor(greenFutureExecutor):
    """Future executor based on eventlet."""

    from eventlet import getcurrent
    from eventlet.patcher import is_monkey_patched, original

    thread_cls = original('threading').Thread
    if sys.version_info.major < 3:
        event_cls = classmethod(original('threading').Event)

        @classmethod
        def get_greentlet_hub(cls):
            from eventlet.hubs import get_hub
            return get_hub()
    else:
        event_cls = original('threading').Event
        from eventlet.hubs import get_hub as get_greentlet_hub

    @classmethod
    def spawn(cls, func, g_hub):
        """Spawn a new greenlet within the same hub of the caller.

        :param func: new greenlet's main function.
        :param g_hub: the hub of greenlet that called `apply_future` method.

        """
        g = greenlet(func, g_hub.greenlet)
        g_hub.schedule_call_global(0, g.switch)
        return g


class geventFutureExecutor(greenFutureExecutor):
    """Future executor based on gevent."""

    from gevent import getcurrent
    from gevent.monkey import get_original

    thread_cls = get_original('threading', 'Thread')
    if sys.version_info.major < 3:
        event_cls = classmethod(get_original('threading', 'Event'))

        @classmethod
        def get_greentlet_hub(cls):
            from gevent import get_hub
            return get_hub()
    else:
        event_cls = get_original('threading', 'Event')
        from gevent import get_hub as get_greentlet_hub

    @classmethod
    def spawn(cls, func, g_hub):
        """Spawn a new greenlet within the same hub of the caller.

        :param func: new greenlet's main function.
        :param g_hub: the hub of greenlet that called `apply_future` method.

        """
        g = greenlet(func, g_hub)
        g_hub.loop.run_callback(g.switch)
        return g


class TornadoHub(object):
    """Tornado coroutine execution hub."""

    def __init__(self, thread_cls, event_cls, **kwargs):
        """Initialize tornado hub.

        :param thread_cls: the native `threading.Thread` class.

        """
        self._io_loop = None
        self._ioloop_event = event_cls()
        self._io_thread = thread_cls(target=self._run)
        self._io_thread.setDaemon(True)

    def _run(self):
        self._io_loop = ioloop.IOLoop.instance()
        self._ioloop_event.set()
        self._io_loop.start()

    def start(self):
        """Start tornado io thread."""
        self._io_thread.start()

        # ensure the tornado ioloop has been initialized
        if not self._io_loop:
            self._ioloop_event.wait()

    def execute(self, func, *args, **kwargs):
        """Execute coroutine within tornado ioloop.

        :param func: coroutine function.
        :param kwargs: if `callback` in kwargs, the value will be treated as
            a function and called.
        """
        callback = kwargs.pop('callback', None)
        self._io_loop.add_callback(functools.partial(
            self.on_run, callback, func, *args, **kwargs))

    def execute_future(self, future, callback=None):
        """Execute future within tornado ioloop.

        :param func: coroutine function.
        :param kwargs: if `callback` in kwargs, the value will be treated as
            a function and called.
        """
        callback = callback or (lambda f: f.result())
        self._io_loop.add_callback(functools.partial(
            self._io_loop.add_future, future, callback))

    def on_run(self, callback, func, *args, **kwargs):
        """Coroutine run callback."""
        ret = func(*args, **kwargs)
        if concurrent.is_future(ret):
            self._io_loop.add_future(
                ret, functools.partial(self.on_finish, callback))
        else:
            f = gen.TracebackFuture()
            f.set_result(ret)
            if callback:
                callback(f)

    def on_finish(self, callback, future):
        """Coroutine finish callback."""
        if not callback:
            return

        try:
            callback(future)
        except Exception:
            logger.error(
                'uncaught exception when corotuine finished:', exc_info=1)

    def stop(self):
        """Stop the tornado coroutine thread.

        Normally, we shoud not use this method directly, and just for test.
        """
        self._io_loop.add_callback(lambda: self._io_loop.stop())

    @property
    def io_loop(self):
        """Return io loop of hub instance."""
        return self._io_loop

    def __del__(self):
        self.stop()
